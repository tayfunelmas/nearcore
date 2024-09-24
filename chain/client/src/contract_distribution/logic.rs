use near_primitives::contract_distribution::ContractChanges;

use crate::Client;
use near_chain::Error;

#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub struct ContractChangesMessage(pub ContractChanges);

impl Client {
    pub(crate) fn process_contract_changes(
        &self,
        _contract_changes: ContractChanges,
    ) -> Result<(), Error> {
        Ok(())
    }
}
