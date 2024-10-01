use near_primitives::contract_distribution::{ContractChange, ContractChanges};
use near_primitives::errors::StorageError;
use near_primitives::hash::CryptoHash;
use near_primitives::types::CodeHash;
use near_vm_runner::ContractCode;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use crate::adapter::contract_store::ContractStoreAdapter;
use crate::TrieStorage;

#[derive(Default)]
struct ContractCodeWithRefcount {
    code: Option<ContractCode>,
    refcount_delta: u64,
}

impl ContractCodeWithRefcount {
    fn new(code: Option<ContractCode>) -> Self {
        Self { code, refcount_delta: 0 }
    }
}

#[derive(Clone, Default)]
pub struct UncommittedContractChanges(Arc<RwLock<BTreeMap<CodeHash, ContractCodeWithRefcount>>>);

impl UncommittedContractChanges {
    fn new() -> Self {
        Self::default()
    }

    fn record_deploy(&self, code: ContractCode) {
        let mut guard = self.0.write().expect("no panics");
        let changes = match guard.entry(*code.hash()) {
            Entry::Occupied(o) => {
                let changes = o.into_mut();
                if changes.code.is_none() {
                    changes.code = Some(code);
                }
                changes
            }
            Entry::Vacant(v) => v.insert(ContractCodeWithRefcount::new(Some(ContractCode::new(
                code.code().to_vec(),
                Some(*code.hash()),
            )))),
        };
        changes.refcount_delta += 1;
    }

    fn record_delete(&self, code_hash: &CodeHash) {
        let mut guard = self.0.write().expect("no panics");
        let changes = match guard.entry(*code_hash) {
            Entry::Occupied(o) => o.into_mut(),
            Entry::Vacant(v) => v.insert(ContractCodeWithRefcount::new(None)),
        };
        changes.refcount_delta -= 1;
    }

    fn get(&self, code_hash: &CodeHash) -> Option<ContractCode> {
        // Note: We do not check the refcount but always return the code.
        let guard = self.0.read().expect("no panics");
        if let Some(changes) = guard.get(code_hash) {
            if let Some(code) = changes.code.as_ref() {
                debug_assert_eq!(code.hash(), code_hash);
                return Some(ContractCode::new(code.code().to_vec(), Some(*code_hash)));
            }
        }
        None
    }

    // TODO(#11099): Implement into() operation.
    fn changes(&self) -> ContractChanges {
        let mut changes = ContractChanges::default();
        let guard = self.0.read().expect("no panics");
        for (code_hash, code_with_refcount) in guard.iter() {
            if code_with_refcount.refcount_delta != 0 {
                changes.0.push(ContractChange {
                    code_hash: *code_hash,
                    code: code_with_refcount.code.as_ref().map(|c| c.code().to_vec()),
                    refcount_delta: code_with_refcount.refcount_delta,
                });
            }
        }
        changes
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
    /// TODO(#11099): Move Arc<> to here.
    uncommitted_changes: UncommittedContractChanges,
}

impl ContractStorageUpdate {
    pub(crate) fn from(storage: Arc<dyn TrieStorage>) -> Self {
        Self { storage, uncommitted_changes: UncommittedContractChanges::new() }
    }

    pub fn get(&self, code_hash: CodeHash) -> Result<Option<ContractCode>, StorageError> {
        if let Some(v) = self.uncommitted_changes.get(&code_hash) {
            return Ok(Some(ContractCode::new(v.code().to_vec(), Some(code_hash))));
        }
        self.storage
            .retrieve_raw_bytes(&code_hash)
            .map(|raw_code| Some(ContractCode::new(raw_code.to_vec(), Some(code_hash))))
    }

    pub(crate) fn store(&self, code: ContractCode) {
        self.uncommitted_changes.record_deploy(code);
    }

    pub(crate) fn delete(&self, code_hash: &CodeHash) {
        self.uncommitted_changes.record_delete(code_hash);
    }

    pub(crate) fn rollback(&mut self) {
        self.uncommitted_changes = UncommittedContractChanges::new();
    }

    pub(crate) fn get_contract_changes(&self) -> ContractChanges {
        self.uncommitted_changes.changes()
    }
}

/// Reads contract code from the trie by its hash.
///
/// Cloning is cheap.
#[derive(Clone)]
pub struct ContractStorage {
    store: ContractStoreAdapter,
}

impl ContractStorage {
    pub fn new(store: ContractStoreAdapter) -> Self {
        Self { store }
    }
}

impl TrieStorage for ContractStorage {
    fn retrieve_raw_bytes(&self, hash: &CryptoHash) -> Result<Arc<[u8]>, StorageError> {
        self.store.get(hash).map(|code| Arc::from(code.as_slice()))
    }
}
