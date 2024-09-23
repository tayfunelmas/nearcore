use std::sync::{Arc, Mutex};

use near_async::messaging::CanSend;

use crate::stateless_validation::contract_distribution::DistributeContractChangesRequest;
use crate::ContractDistributionActor;

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
        let _ = actor.handle_distribute_contract_changes(msg);
    }
}
