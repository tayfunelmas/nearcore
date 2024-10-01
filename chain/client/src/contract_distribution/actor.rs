use std::sync::Arc;

use itertools::Itertools;
use near_async::messaging::CanSend;
use near_async::messaging::{Actor, Handler, Sender};
use near_async::MultiSend;
use near_async::MultiSenderFrom;
use near_chain::Error;
use near_chain_configs::MutableValidatorSigner;
use near_epoch_manager::EpochManagerAdapter;
use near_network::contract_distribution::SignedEncodedContractChangesMessage;
use near_network::types::{NetworkRequests, PeerManagerAdapter, PeerManagerMessageRequest};
use near_performance_metrics_macros::perf;
use near_primitives::contract_distribution::{
    ChunkContractChanges, ChunkContractChangesProof, ContractChanges, SignedEncodedContractChanges,
};
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::sharding::ChunkHash;
use near_primitives::stateless_validation::ChunkProductionKey;
use near_store::adapter::contract_store::ContractStoreAdapter;
use near_store::Store;
use near_vm_runner::ContractCode;

use crate::client_actor::ClientSenderForContractDistribution;
use crate::stateless_validation::validate::validate_chunk_production_key;

pub struct ContractDistributionActor {
    /// Adapter to send messages to the network.
    network_adapter: PeerManagerAdapter,
    client_sender: ClientSenderForContractDistribution,
    /// Validator signer to sign the state witness. This field is mutable and optional. Use with caution!
    /// Lock the value of mutable validator signer for the duration of a request to ensure consistency.
    /// Please note that the locked value should not be stored anywhere or passed through the thread boundary.
    my_signer: MutableValidatorSigner,
    /// Epoch manager to get the set of validators
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    store: ContractStoreAdapter,
}

impl Actor for ContractDistributionActor {}

#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub struct DistributeContractChangesRequest {
    pub contract_changes: ChunkContractChanges,
}

#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub struct CommitContractChangesRequest {
    pub chunks: Vec<ChunkContractChangesProof>,
}

#[derive(Clone, MultiSend, MultiSenderFrom)]
pub struct ContractDistributionSenderForClient {
    pub distribute_contract_changes: Sender<DistributeContractChangesRequest>,
}

impl Handler<DistributeContractChangesRequest> for ContractDistributionActor {
    #[perf]
    fn handle(&mut self, msg: DistributeContractChangesRequest) {
        if let Err(err) = self.handle_distribute_contract_changes(msg.contract_changes) {
            tracing::error!(target: "client", ?err, "Failed to handle DistributeContractChangesRequest");
        }
    }
}

impl Handler<SignedEncodedContractChangesMessage> for ContractDistributionActor {
    fn handle(&mut self, msg: SignedEncodedContractChangesMessage) {
        if let Err(err) = self.handle_contract_changes_received(msg.0) {
            tracing::error!(target: "client", ?err, "Failed to handle ContractChangesMessage");
        }
    }
}

impl ContractDistributionActor {
    pub fn new(
        network_adapter: PeerManagerAdapter,
        client_sender: ClientSenderForContractDistribution,
        my_signer: MutableValidatorSigner,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        store: ContractStoreAdapter,
    ) -> Self {
        Self { network_adapter, client_sender, my_signer, epoch_manager, store }
    }

    pub fn handle_distribute_contract_changes(
        &mut self,
        raw_changes: ChunkContractChanges,
    ) -> Result<(), Error> {
        let metadata = &raw_changes.metadata;

        tracing::debug!(target: "client", epoch_id=?metadata.epoch_id, shard_id=?metadata.shard_id, height=?metadata.height_created, "distribute_contract_changes");

        let signer = match self.my_signer.get() {
            Some(signer) => signer,
            None => {
                return Err(Error::NotAValidator(format!("distribute contract changes")));
            }
        };

        let validators = self
            .epoch_manager
            .get_epoch_all_validators(&metadata.epoch_id)?
            .into_iter()
            .map(|vs| vs.account_id().clone())
            .collect_vec();

        let encoded_changes = SignedEncodedContractChanges::new(raw_changes, &signer)?;

        self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::ContractChanges(validators, encoded_changes),
        ));

        Ok(())
    }

    pub fn handle_contract_changes_received(
        &mut self,
        signed_encoded_changes: SignedEncodedContractChanges,
    ) -> Result<(), Error> {
        let chunk_key = signed_encoded_changes.chunk_production_key();
        let chunk_producer = self.epoch_manager.get_chunk_producer_info(
            &chunk_key.epoch_id,
            chunk_key.height_created,
            chunk_key.shard_id,
        )?;

        if !signed_encoded_changes.verify(chunk_producer.public_key()) {
            return Err(Error::InvalidSignature);
        }

        if !validate_chunk_production_key(
            self.epoch_manager.as_ref(),
            chunk_key,
            // This node may not be a validator for the given chunk, so do not check it.
            None,
            &self.store,
        )? {
            return Err(Error::InvalidContractChanges("Invalid chunk production key".to_string()));
        }

        let (changes, _size) = signed_encoded_changes.decode()?;

        validate_contract_changes(&changes.inner())?;

        tracing::info!(target: "code-dist", changes=?changes.inner(), "Received contract changes");
        self.client_sender.send(ContractChangesMessage(changes));

        Ok(())
    }
}

fn validate_contract_changes(changes: &ContractChanges) -> Result<(), Error> {
    for change in changes.0.iter() {
        if change.refcount_delta == 0 {
            return Err(Error::InvalidContractChanges("Refcount delta is zero".to_string()));
        }
        if change.code_hash == CryptoHash::default() {
            return Err(Error::InvalidContractChanges("Code hash is set to default".to_string()));
        }
        if let Some(code) = change.code_hash.as_ref() {
            if code.len() == 0 {
                return Err(Error::InvalidContractChanges("Code is empty".to_string()));
            }
            if hash(code) != change.code_hash {
                return Err(Error::InvalidContractChanges("Invalid code hash".to_string()));
            }
        }
    }
    Ok(())
}
