use near_primitives::contract_distribution::{ContractChange, ContractChanges};
use near_primitives::errors::{MissingTrieValueContext, StorageError};
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::types::CodeHash;
use near_vm_runner::ContractCode;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use crate::adapter::contract_store::ContractStoreAdapter;
use crate::adapter::StoreAdapter;
use crate::{TrieDBStorage, TrieStorage};

#[derive(Default)]
struct ContractCodeWithRefcount {
    code: Option<ContractCode>,
    refcount_delta: i32,
}

impl From<ContractCode> for ContractCodeWithRefcount {
    fn from(code: ContractCode) -> Self {
        Self { code: Some(code), refcount_delta: 0 }
    }
}

#[derive(Clone)]
pub struct UncommittedContractChanges(
    // TODO(#11099): This is single-threaded (per shard), so remove RwLock.
    Arc<RwLock<Option<BTreeMap<CodeHash, ContractCodeWithRefcount>>>>,
);

impl UncommittedContractChanges {
    fn new() -> Self {
        Self(Arc::new(RwLock::new(Some(BTreeMap::new()))))
    }

    fn record_deploy(&self, code: ContractCode) {
        let mut guard = self.0.write().expect("no panics");
        let changes = match guard.as_mut().unwrap().entry(*code.hash()) {
            Entry::Occupied(o) => {
                let changes = o.into_mut();
                if changes.code.is_none() {
                    changes.code = Some(code);
                }
                changes
            }
            Entry::Vacant(v) => v.insert(code.into()),
        };
        changes.refcount_delta += 1;
    }

    fn record_delete(&self, code_hash: &CodeHash) {
        let mut guard = self.0.write().expect("no panics");
        let changes = match guard.as_mut().unwrap().entry(*code_hash) {
            Entry::Occupied(o) => o.into_mut(),
            Entry::Vacant(v) => v.insert(ContractCodeWithRefcount::default()),
        };
        changes.refcount_delta -= 1;
    }

    fn get(&self, code_hash: &CodeHash) -> Option<ContractCode> {
        // Note: We do not check the refcount but always return the code.
        let guard = self.0.read().expect("no panics");
        if let Some(changes) = guard.as_ref().unwrap().get(code_hash) {
            if let Some(code) = changes.code.as_ref() {
                debug_assert_eq!(code.hash(), code_hash);
                return Some(ContractCode::new(code.code().to_vec(), Some(*code_hash)));
            }
        }
        None
    }

    fn take_changes(&self) -> ContractChanges {
        let mut changes = ContractChanges::default();
        let mut guard = self.0.write().expect("no panics");
        let existing_changes = guard.replace(BTreeMap::new()).expect("should always be present");
        for (code_hash, code_with_refcount) in existing_changes.into_iter() {
            if code_with_refcount.refcount_delta != 0 {
                changes.0.push(ContractChange::new(
                    code_hash,
                    code_with_refcount.code.as_ref().map(|c| c.code().to_vec()),
                    code_with_refcount.refcount_delta,
                ));
            }
        }
        changes
    }

    fn is_empty(&self) -> bool {
        // Note: We do not check the refcount but always return the code.
        let guard = self.0.read().expect("no panics");
        guard.as_ref().unwrap().is_empty()
    }
}

/// Reads contract code from the trie by its hash.
///
/// Cloning is cheap.
#[derive(Clone)]
pub struct ContractStorageUpdate {
    storage: Arc<dyn TrieStorage>,

    /// During an apply of a single chunk contracts may be deployed through the
    /// `Action::DeployContract`.
    ///
    /// Unfortunately `TrieStorage` does not have a way to write to the underlying storage, and the
    /// `TrieUpdate` will only write the contract once the whole transaction is committed at the
    /// end of the chunk's apply.
    ///
    /// As a temporary work-around while we're still involving `Trie` for `ContractCode` storage,
    /// we'll keep a list of such deployed contracts here. Once the contracts are no longer part of
    /// The State this field should be removed, and the `Storage::store` function should be
    /// adjusted to write out the contract into the relevant part of the database immediately
    /// (without going through transactional storage operations and such).
    uncommitted_changes: UncommittedContractChanges,

    committed_changes: Option<ContractChanges>,
}

impl ContractStorageUpdate {
    pub(crate) fn from(storage: Arc<dyn TrieStorage>) -> Self {
        Self {
            storage,
            uncommitted_changes: UncommittedContractChanges::new(),
            committed_changes: None,
        }
    }

    pub fn get(&self, code_hash: CodeHash) -> Result<Option<ContractCode>, StorageError> {
        if let Some(v) = self.uncommitted_changes.get(&code_hash) {
            return Ok(Some(ContractCode::new(v.code().to_vec(), Some(code_hash))));
        }
        match self.storage.retrieve_raw_bytes(&code_hash) {
            Ok(code) => Ok(Some(ContractCode::new(code.to_vec(), Some(code_hash)))),
            Err(StorageError::MissingTrieValue(_, _)) => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub(crate) fn store(&self, code: ContractCode) {
        // TODO(#11099): assert!(self.committed_changes.is_none(), "Cannot store after commit");
        self.uncommitted_changes.record_deploy(code);
    }

    pub(crate) fn delete(&self, code_hash: &CodeHash) {
        // TODO(#11099): assert!(self.committed_changes.is_none(), "Cannot delete after commit");
        self.uncommitted_changes.record_delete(code_hash);
    }

    pub(crate) fn rollback(&mut self) {
        // TODO(#11099): assert!(self.committed_changes.is_none(), "Cannot rollback after commit");
        let _ = self.uncommitted_changes.take_changes();
    }

    pub(crate) fn commit(&mut self) {
        // TODO(#11099): assert!(self.committed_changes.is_none(), "Already committed");
        self.committed_changes = Some(self.uncommitted_changes.take_changes());
    }

    pub(crate) fn finalize(self) -> ContractChanges {
        // TODO(#11099): assert!(self.uncommitted_changes.is_empty(), "Has uncommited changes before finalizing");
        self.committed_changes.unwrap_or_else(|| panic!("Finalizing before committing"))
    }
}

/// Reads contract code from the trie by its hash.
///
/// Cloning is cheap.
#[derive(Clone)]
pub struct ContractStorage {
    store: ContractStoreAdapter,
    state_fallback: Option<Arc<TrieDBStorage>>,
}

impl ContractStorage {
    pub fn new(store: ContractStoreAdapter, shard_uid: Option<ShardUId>) -> Self {
        let state_fallback =
            shard_uid.map(|shard_uid| Arc::new(TrieDBStorage::new(store.trie_store(), shard_uid)));
        Self { store, state_fallback }
    }
}

impl TrieStorage for ContractStorage {
    fn retrieve_raw_bytes(&self, hash: &CryptoHash) -> Result<Arc<[u8]>, StorageError> {
        match self.store.get_contract_code(hash) {
            Ok(code) => Ok(Arc::from(code.as_slice())),
            Err(StorageError::MissingTrieValue(context, hash_from_error)) => {
                if let Some(state_fallback) = self.state_fallback.as_ref() {
                    match state_fallback.retrieve_raw_bytes(hash) {
                        Ok(code) => Ok(code),
                        Err(StorageError::MissingTrieValue(_, hash)) => {
                            Err(StorageError::MissingTrieValue(
                                MissingTrieValueContext::ContractStorage,
                                hash,
                            ))
                        }
                        Err(error) => Err(error),
                    }
                } else {
                    Err(StorageError::MissingTrieValue(context, hash_from_error))
                }
            }
            Err(error) => Err(error),
        }
    }
}
