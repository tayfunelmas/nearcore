use borsh::{BorshDeserialize, BorshSerialize};
use bytesize::ByteSize;
use near_crypto::{PublicKey, Signature};
use near_primitives_core::types::{BlockHeight, CodeBytes, CodeHash, ShardId};
use near_schema_checker_lib::ProtocolSchema;
use std::io::Error;

use crate::{
    sharding::{ChunkHash, ShardChunkHeader},
    stateless_validation::{ChunkProductionKey, SignatureDifferentiator},
    types::EpochId,
    utils::compression::CompressedData,
    validator_signer::ValidatorSigner,
};

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ContractChangesMetadata {
    /// Epoch ID in which the chunk is created.
    pub epoch_id: EpochId,
    /// Block height at which this chunk is created.
    pub height_created: BlockHeight,
    /// Shard ID of the chunk.
    pub shard_id: ShardId,
    // Hash of the chunk.
    pub chunk_hash: ChunkHash,
}

impl ContractChangesMetadata {
    pub fn chunk_production_key(&self) -> ChunkProductionKey {
        ChunkProductionKey {
            shard_id: self.shard_id,
            epoch_id: self.epoch_id,
            height_created: self.height_created,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ChunkContractChanges {
    pub metadata: ContractChangesMetadata,
    pub changes: ContractChanges,
}

#[derive(
    Debug, Default, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema,
)]
pub struct ContractChanges(pub Vec<ContractChange>);

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ContractChange {
    pub code_hash: CodeHash,
    pub code: Option<CodeBytes>,
    pub refcount_delta: u64,
}

impl ChunkContractChanges {
    pub fn new(
        epoch_id: EpochId,
        chunk_header: &ShardChunkHeader,
        changes: ContractChanges,
    ) -> Self {
        let metadata = ContractChangesMetadata {
            epoch_id,
            height_created: chunk_header.height_created(),
            shard_id: chunk_header.shard_id(),
            chunk_hash: chunk_header.chunk_hash(),
        };
        Self { metadata, changes }
    }
}

pub const MAX_UNCOMPRESSED_CONTRACT_CHANGES_SIZE: u64 =
    ByteSize::mib(if cfg!(feature = "test_features") { 512 } else { 64 }).0;
pub const CONTRACT_CHANGES_COMPRESSION_LEVEL: i32 = 3;

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    BorshSerialize,
    BorshDeserialize,
    ProtocolSchema,
    derive_more::From,
    derive_more::AsRef,
)]
pub struct EncodedContractChanges(Box<[u8]>);

impl
    CompressedData<
        ChunkContractChanges,
        MAX_UNCOMPRESSED_CONTRACT_CHANGES_SIZE,
        CONTRACT_CHANGES_COMPRESSION_LEVEL,
    > for EncodedContractChanges
{
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct SignedEncodedContractChanges {
    inner: EncodedContractChangesInner,
    signature: Signature,
}

impl SignedEncodedContractChanges {
    pub fn new(
        contract_changes: ChunkContractChanges,
        signer: &ValidatorSigner,
    ) -> Result<Self, Error> {
        let inner = EncodedContractChangesInner::new(contract_changes)?;
        let signature = signer.sign_contract_changes(&inner);
        Ok(Self { inner, signature })
    }

    pub fn chunk_production_key(&self) -> ChunkProductionKey {
        self.inner.metadata.chunk_production_key()
    }

    pub fn verify(&self, public_key: &PublicKey) -> bool {
        let data = borsh::to_vec(&self.inner).unwrap();
        self.signature.verify(&data, public_key)
    }

    pub fn decode(&self) -> Result<(ChunkContractChanges, usize), std::io::Error> {
        self.inner.encoded_changes.decode()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct EncodedContractChangesInner {
    metadata: ContractChangesMetadata,
    encoded_changes: EncodedContractChanges,
    signature_differentiator: SignatureDifferentiator,
}

impl EncodedContractChangesInner {
    fn new(contract_changes: ChunkContractChanges) -> Result<Self, Error> {
        let (encoded_changes, _raw_size) = EncodedContractChanges::encode(&contract_changes)?;
        Ok(Self {
            metadata: contract_changes.metadata,
            encoded_changes,
            signature_differentiator: "ContractChanges".to_owned(),
        })
    }
}
