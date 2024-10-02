use std::sync::{Arc, Mutex};

use near_async::messaging::CanSend;
use near_network::contract_distribution::SignedEncodedContractChangesMessage;

use crate::contract_distribution::actor::{
    ContractDistributionActor, DistributeContractChangesRequest,
};

#[derive(Clone)]
pub struct SynchronousContractDistributionAdapter(Arc<Mutex<ContractDistributionActor>>);

impl SynchronousContractDistributionAdapter {
    pub fn new(actor: ContractDistributionActor) -> Self {
        Self(Arc::new(Mutex::new(actor)))
    }
}

impl CanSend<DistributeContractChangesRequest> for SynchronousContractDistributionAdapter {
    fn send(&self, msg: DistributeContractChangesRequest) {
        let mut actor = self.0.lock().unwrap();
        let _ = actor.handle_distribute_contract_changes(&msg.block_hash, msg.shard_id);
    }
}

impl CanSend<SignedEncodedContractChangesMessage> for SynchronousContractDistributionAdapter {
    fn send(&self, msg: SignedEncodedContractChangesMessage) {
        let mut actor = self.0.lock().unwrap();
        let _ = actor.handle_contract_changes_received(msg.0);
    }
}
