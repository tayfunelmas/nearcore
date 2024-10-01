use std::num::NonZeroUsize;

use lru::LruCache;
use near_primitives::{
    block::ChunksCollection,
    contract_distribution::{ChunkContractChanges, ContractChanges},
    sharding::{ChunkHash, ShardChunkHeader},
    stateless_validation::ChunkProductionKey,
    types::EpochId,
};
use near_store::{adapter::contract_store::ContractStoreAdapter, StoreUpdate};

use crate::Client;
use near_chain::Error;

#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub struct ContractChangesMessage(pub ChunkContractChanges);

#[derive(Debug)]
struct ContractChangesCacheEntry {
    changes: ContractChanges,
    chunk_hash: ChunkHash,
}

pub struct ContractChangesTracker {
    store: ContractStoreAdapter,
    // We currently store the received ContractChanges messages until they are validated by the chunk headers in the new block.
    // Consider moving them into a new column in DB.
    uncommitted_changes: LruCache<ChunkProductionKey, ContractChangesCacheEntry>,
}

impl ContractChangesTracker {
    pub fn new(store: ContractStoreAdapter) -> Self {
        let uncommitted_changes = LruCache::new(NonZeroUsize::new(10).unwrap());
        Self { store, uncommitted_changes }
    }

    pub fn update(&mut self, contract_changes: ChunkContractChanges) -> Result<(), Error> {
        let cache_key = contract_changes.chunk_production_key();
        tracing::info!(target: "code-dist", "Updated tracker for chunk info: {:?}", cache_key);
        let cache_entry = ContractChangesCacheEntry {
            chunk_hash: contract_changes.metadata.chunk_hash().clone(),
            changes: contract_changes.into(),
        };
        let existing = self.uncommitted_changes.put(cache_key, cache_entry);
        assert!(
            existing.is_none(),
            "Has cache entry with same chunk info: {:?}",
            existing.unwrap()
        );
        Ok(())
    }

    fn save_contract_changes(
        &mut self,
        epoch_id: &EpochId,
        chunks: ChunksCollection,
    ) -> Result<StoreUpdate, Error> {
        let mut block_contract_changes = ContractChanges::default();
        for chunk_header in chunks.iter() {
            let chunk_key = ChunkProductionKey {
                epoch_id: *epoch_id,
                height_created: chunk_header.height_created(),
                shard_id: chunk_header.shard_id(),
            };
            let Some(cache_entry) = self.uncommitted_changes.pop(&chunk_key) else {
                panic!("Failed to find contract changes for chunk production key: {:?}", chunk_key)
            };
            // Validate chunk hash and merkelized root before merging with others.
            if let Err(error) = Self::validate_contract_changes(&cache_entry, chunk_header) {
                tracing::error!("Failed to validate contract changes for chunk: {:#}", error);
                continue;
            }

            block_contract_changes.merge_from(cache_entry.changes);
        }
        let store_update = self.store.store_update();
        store_update.save_contract_changes(block_contract_changes)?;
        Ok(store_update.into())
    }

    fn validate_contract_changes(
        cache_entry: &ContractChangesCacheEntry,
        chunk_header: &ShardChunkHeader,
    ) -> Result<(), Error> {
        if cache_entry.chunk_hash != chunk_header.chunk_hash() {
            return Err(Error::InvalidContractChanges("Invalid chunk hash".to_string()));
        }
        if cache_entry.changes.merklize() != chunk_header.contract_changes_root().unwrap() {
            return Err(Error::InvalidContractChanges(
                "Invalid merkle root for contract changes".to_string(),
            ));
        }
        Ok(())
    }
}

impl Client {
    pub(crate) fn process_contract_changes(
        &mut self,
        contract_changes: ChunkContractChanges,
    ) -> Result<(), Error> {
        self.contract_changes_tracker.update(contract_changes)?;
        Ok(())
    }
}
