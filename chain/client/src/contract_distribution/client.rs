use crate::Client;
use near_async::messaging::CanSend;
use near_chain::ChainStoreAccess;
use near_chain::Error;
use near_primitives::{
    block::ChunksCollection, contract_distribution::ChunkContractChanges, hash::CryptoHash,
    types::ShardId,
};
use near_store::adapter::StoreAdapter;
use near_store::{adapter::contract_store::ContractStoreAdapter, StoreUpdate};

use super::actor::DistributeContractChangesRequest;

#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub struct ContractChangesMessage(pub ChunkContractChanges);

pub struct ContractChangesTracker {
    store: ContractStoreAdapter,
}

impl ContractChangesTracker {
    pub fn new(store: ContractStoreAdapter) -> Self {
        Self { store }
    }

    fn save_contract_changes(
        &mut self,
        next_chunks: ChunksCollection,
    ) -> Result<StoreUpdate, Error> {
        // let mut block_contract_changes = ContractChanges::default();
        // for chunk_header in chunks.iter() {
        //     let chunk_key = ChunkProductionKey {
        //         epoch_id: *epoch_id,
        //         height_created: chunk_header.height_created(),
        //         shard_id: chunk_header.shard_id(),
        //     };
        //     let Some(cache_entry) = self.uncommitted_changes.pop(&chunk_key) else {
        //         panic!("Failed to find contract changes for chunk production key: {:?}", chunk_key)
        //     };
        //     // Validate chunk hash and merkelized root before merging with others.
        //     if let Err(error) = Self::validate_contract_changes(&cache_entry, chunk_header) {
        //         tracing::error!("Failed to validate contract changes for chunk: {:#}", error);
        //         continue;
        //     }

        //     block_contract_changes.merge_from(cache_entry.changes);
        // }
        let store_update = self.store.store_update();
        // store_update.save_block_contract_changes(block_contract_changes)?;
        Ok(store_update.into())
    }

    // fn validate_contract_changes(
    //     cache_entry: &ContractChangesCacheEntry,
    //     chunk_header: &ShardChunkHeader,
    // ) -> Result<(), Error> {
    //     if cache_entry.chunk_hash != chunk_header.chunk_hash() {
    //         return Err(Error::InvalidContractChanges("Invalid chunk hash".to_string()));
    //     }
    //     if cache_entry.changes.merklize() != chunk_header.contract_changes_root().unwrap() {
    //         return Err(Error::InvalidContractChanges(
    //             "Invalid merkle root for contract changes".to_string(),
    //         ));
    //     }
    //     Ok(())
    // }
}

impl Client {
    pub(crate) fn distribute_contract_changes(&self, block_hash: CryptoHash, shard_id: ShardId) {
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
        store_update.save_chunk_contract_changes(&block_hash, shard_id, &changes);
        store_update.commit().map_err(|error| Error::Other(error.to_string()))
    }
}
