use near_async::messaging::Sender;
use near_async::{MultiSend, MultiSendMessage, MultiSenderFrom};
use near_primitives::contract_distribution::SignedEncodedContractChanges;

#[derive(actix::Message, Clone, Debug, PartialEq, Eq)]
#[rtype(result = "()")]
pub struct SignedEncodedContractChangesMessage(pub SignedEncodedContractChanges);

#[derive(Clone, MultiSend, MultiSenderFrom, MultiSendMessage)]
#[multi_send_message_derive(Debug)]
#[multi_send_input_derive(Debug, Clone, PartialEq, Eq)]
pub struct ContractDistributionSenderForNetwork {
    pub contract_changes: Sender<SignedEncodedContractChangesMessage>,
}
