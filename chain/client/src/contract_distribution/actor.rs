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
use near_primitives::contract_distribution::{ContractChanges, SignedEncodedContractChanges};
use near_primitives::hash::CryptoHash;
use near_primitives::types::ShardId;
use near_store::adapter::contract_store::ContractStoreAdapter;
use near_store::adapter::StoreAdapter;

use crate::client_actor::ClientSenderForContractDistribution;
use crate::contract_distribution::client::ContractChangesMessage;
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
    pub block_hash: CryptoHash,
    pub shard_id: ShardId,
}

#[derive(Clone, MultiSend, MultiSenderFrom)]
pub struct ContractDistributionSenderForClient {
    pub distribute_contract_changes: Sender<DistributeContractChangesRequest>,
}

impl Handler<DistributeContractChangesRequest> for ContractDistributionActor {
    #[perf]
    fn handle(&mut self, msg: DistributeContractChangesRequest) {
        tracing::trace!(target: "code-dist", "Actor handling DistributeContractChangesRequest");
        if let Err(err) = self.handle_distribute_contract_changes(&msg.block_hash, msg.shard_id) {
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
        block_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<(), Error> {
        let Some(contract_changes) =
            self.store.get_chunk_contract_changes(&block_hash, shard_id)?
        else {
            tracing::error!(target: "code-dist", ?block_hash, shard_id, "Failed to find chunk contract changes");
            return Err(Error::Other(format!(
                "ChunkContractChanges not found for block {:?} and shard {}",
                block_hash, shard_id
            )));
        };

        tracing::trace!(target: "code-dist", ?block_hash, shard_id, "Actor signing and encoding chunk contract changes");

        let metadata = &contract_changes.metadata;
        let validators = self
            .epoch_manager
            .get_epoch_all_validators(&metadata.epoch_id)?
            .into_iter()
            .map(|vs| vs.account_id().clone())
            .collect_vec();

        let signer = match self.my_signer.get() {
            Some(signer) => signer,
            None => {
                return Err(Error::NotAValidator(format!("distribute contract changes")));
            }
        };
        let encoded_changes = SignedEncodedContractChanges::new(contract_changes, &signer)?;

        tracing::trace!(target: "code-dist", ?block_hash, shard_id, ?validators, "Actor distributing chunk contract changes to validators");
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
            &self.store.store(),
        )? {
            return Err(Error::InvalidContractChanges("Invalid chunk production key".to_string()));
        }

        let (changes, _size) = signed_encoded_changes.decode()?;

        validate_contract_changes(changes.inner())?;

        tracing::trace!(target: "code-dist", changes=?changes.inner(), "Received contract changes");
        self.client_sender.send(ContractChangesMessage(changes));

        Ok(())
    }
}

fn validate_contract_changes(changes: &ContractChanges) -> Result<(), Error> {
    for change in changes.0.iter() {
        change.validate().map_or(Ok(()), |err| Err(Error::InvalidContractChanges(err)))?;
    }
    Ok(())
}
