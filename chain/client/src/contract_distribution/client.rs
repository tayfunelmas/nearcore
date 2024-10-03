use crate::Client;
use near_async::messaging::CanSend;
use near_chain::ChainStoreAccess;
use near_chain::Error;
use near_primitives::{
    contract_distribution::ChunkContractChanges, hash::CryptoHash, types::ShardId,
};
use near_store::adapter::StoreAdapter;

use super::actor::DistributeContractChangesRequest;

#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub struct ContractChangesMessage(pub ChunkContractChanges);

impl Client {
    pub(crate) fn distribute_contract_changes(&self, block_hash: CryptoHash, shard_id: ShardId) {
        tracing::trace!(target: "code-dist", "Client sending DistributeContractChangesRequest");
        self.contract_distribution_adapter
            .send(DistributeContractChangesRequest { block_hash, shard_id });
    }

    pub(crate) fn on_contract_changes_received(
        &mut self,
        changes: ChunkContractChanges,
    ) -> Result<(), Error> {
        // Get the (block_hash, shard_id) pair to save the changes in DB.
        let block_hash = changes.metadata.block_hash;
        let shard_id = changes.metadata.shard_id;

        let mut store_update = self.chain.chain_store.store().contract_store().store_update();
        store_update.save_chunk_contract_changes(&block_hash, shard_id, &changes)?;
        store_update.commit().map_err(|error| Error::Other(error.to_string()))
    }
}
