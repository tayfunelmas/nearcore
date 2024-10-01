use crate::{DBCol, Store, StoreAdapter, StoreUpdate, StoreUpdateAdapter};
use near_primitives::contract_distribution::ContractChanges;
use near_primitives::errors::{MissingTrieValueContext, StorageError};
use near_primitives::types::{CodeBytes, CodeHash};
use std::io;
use std::num::NonZero;
use std::sync::Arc;

use super::StoreUpdateHolder;

#[derive(Clone)]
pub struct ContractStoreAdapter {
    store: Store,
}

impl StoreAdapter for ContractStoreAdapter {
    fn store(&self) -> Store {
        self.store.clone()
    }
}

impl ContractStoreAdapter {
    pub fn new(store: Store) -> Self {
        Self { store }
    }

    pub fn store_update(&self) -> ContractStoreUpdateAdapter<'static> {
        ContractStoreUpdateAdapter {
            store_update: StoreUpdateHolder::Owned(self.store.store_update()),
        }
    }

    pub fn get(&self, code_hash: &CodeHash) -> Result<Arc<CodeBytes>, StorageError> {
        let val = self
            .store
            .get(DBCol::ContractCode, code_hash.as_ref())
            .map_err(|_| StorageError::StorageInternalError)?
            .ok_or(StorageError::MissingTrieValue(
                MissingTrieValueContext::ContractStorage,
                *code_hash,
            ))?;
        Ok(Arc::new(val.into()))
    }
}

pub struct ContractStoreUpdateAdapter<'a> {
    store_update: StoreUpdateHolder<'a>,
}

impl Into<StoreUpdate> for ContractStoreUpdateAdapter<'static> {
    fn into(self) -> StoreUpdate {
        self.store_update.into()
    }
}

impl ContractStoreUpdateAdapter<'static> {
    pub fn commit(self) -> io::Result<()> {
        let store_update: StoreUpdate = self.into();
        store_update.commit()
    }

    pub fn save_contract_changes(&self, _changes: ContractChanges) -> io::Result<()> {
        unimplemented!("TODO(#11099): Implement this.")
    }
}

impl<'a> StoreUpdateAdapter for ContractStoreUpdateAdapter<'a> {
    fn store_update(&mut self) -> &mut StoreUpdate {
        &mut self.store_update
    }
}

impl<'a> ContractStoreUpdateAdapter<'a> {
    pub fn new(store_update: &'a mut StoreUpdate) -> Self {
        Self { store_update: StoreUpdateHolder::Reference(store_update) }
    }

    pub fn decrement_refcount_by(&mut self, code_hash: &CodeHash, decrement: NonZero<u32>) {
        self.store_update.decrement_refcount_by(DBCol::ContractCode, code_hash.as_ref(), decrement);
    }

    pub fn decrement_refcount(&mut self, code_hash: &CodeHash) {
        self.store_update.decrement_refcount(DBCol::ContractCode, code_hash.as_ref());
    }

    pub fn increment_refcount_by(
        &mut self,
        code_hash: &CodeHash,
        code: &CodeBytes,
        decrement: NonZero<u32>,
    ) {
        self.store_update.increment_refcount_by(
            DBCol::ContractCode,
            code_hash.as_ref(),
            code,
            decrement,
        );
    }
}
