use std::sync::Arc;

use near_async::messaging::{Actor, Handler, Sender};
use near_async::MultiSend;
use near_async::MultiSenderFrom;
use near_chain::Error;
use near_chain_configs::{MutableConfigValue, MutableValidatorSigner};
use near_epoch_manager::EpochManagerAdapter;
use near_network::types::PeerManagerAdapter;
use near_performance_metrics_macros::perf;
use near_primitives::stateless_validation::contract_distribution::ContractChanges;
use near_primitives::validator_signer::ValidatorSigner;

use crate::Client;

pub struct ContractDistributionActor {
    /// Adapter to send messages to the network.
    network_adapter: PeerManagerAdapter,
    /// Validator signer to sign the state witness. This field is mutable and optional. Use with caution!
    /// Lock the value of mutable validator signer for the duration of a request to ensure consistency.
    /// Please note that the locked value should not be stored anywhere or passed through the thread boundary.
    my_signer: MutableValidatorSigner,
    /// Epoch manager to get the set of validators
    epoch_manager: Arc<dyn EpochManagerAdapter>,
}

impl Actor for ContractDistributionActor {}

#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub struct DistributeContractChangesRequest {
    pub contract_changes: ContractChanges,
}

#[derive(Clone, MultiSend, MultiSenderFrom)]
pub struct ContractDistributionSenderForClient {
    pub distribute_contract_changes: Sender<DistributeContractChangesRequest>,
}

impl Handler<DistributeContractChangesRequest> for ContractDistributionActor {
    #[perf]
    fn handle(&mut self, msg: DistributeContractChangesRequest) {
        if let Err(err) = self.handle_distribute_contract_changes(msg) {
            tracing::error!(target: "client", ?err, "Failed to handle distribute contract changes request");
        }
    }
}

impl ContractDistributionActor {
    pub fn new(
        network_adapter: PeerManagerAdapter,
        my_signer: MutableValidatorSigner,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
    ) -> Self {
        Self { network_adapter, my_signer, epoch_manager }
    }

    pub fn handle_distribute_contract_changes(
        &mut self,
        msg: DistributeContractChangesRequest,
    ) -> Result<(), Error> {
        // let DistributeStateWitnessRequest { epoch_id, chunk_header, state_witness } = msg;

        // tracing::debug!(
        //     target: "client",
        //     chunk_hash=?chunk_header.chunk_hash(),
        //     "distribute_chunk_state_witness",
        // );

        // let signer = match self.my_signer.get() {
        //     Some(signer) => signer,
        //     None => {
        //         return Err(Error::NotAValidator(format!("distribute state witness")));
        //     }
        // };

        // let witness_bytes = compress_witness(&state_witness)?;

        // self.send_state_witness_parts(epoch_id, chunk_header, witness_bytes, &signer)?;

        Ok(())
    }
}

impl Client {
    pub fn process_contract_changes(
        &mut self,
        contract_changes: ContractChanges,
    ) -> Result<(), Error> {
        // TODO
        Ok(())
    }
}
