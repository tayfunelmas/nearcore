use near_primitives::contract_distribution::ChunkContractChanges;

use crate::Client;
use near_chain::Error;

#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub struct ContractChangesMessage(pub ChunkContractChanges);

impl Client {
    pub(crate) fn process_contract_changes(
        &self,
        _contract_changes: ChunkContractChanges,
    ) -> Result<(), Error> {
        Ok(())
    }
}
