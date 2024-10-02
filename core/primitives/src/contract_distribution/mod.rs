use borsh::{BorshDeserialize, BorshSerialize};
use bytesize::ByteSize;
use itertools::Itertools;
use near_crypto::{PublicKey, Signature};
use near_primitives_core::{
    hash::{hash, CryptoHash},
    types::{BlockHeight, CodeBytes, CodeHash, MerkleHash, ShardId},
};
use near_schema_checker_lib::ProtocolSchema;

use crate::{
    merkle::merklize,
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

    pub fn chunk_hash(&self) -> &ChunkHash {
        &self.chunk_hash
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ChunkContractChanges {
    pub metadata: ContractChangesMetadata,
    pub changes: ContractChanges,
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

    pub fn chunk_production_key(&self) -> ChunkProductionKey {
        self.metadata.chunk_production_key()
    }

    pub fn inner(&self) -> &ContractChanges {
        &self.changes
    }
}

impl Into<ContractChanges> for ChunkContractChanges {
    fn into(self) -> ContractChanges {
        self.changes
    }
}

#[derive(
    Debug, Default, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema,
)]
pub struct ContractChanges(pub Vec<ContractChange>);

impl ContractChanges {
    pub fn merge_from(&mut self, other: ContractChanges) {
        // TODO(#11099): Optimize this, for now leaving with a naive implementation.
        'outer: for other_change in other.0.into_iter() {
            for my_change in self.0.iter_mut() {
                if my_change.code_hash == other_change.code_hash {
                    my_change.refcount_delta += other_change.refcount_delta;
                    if my_change.code.is_none() && other_change.code.is_some() {
                        my_change.code = other_change.code;
                    }
                    continue 'outer;
                }
            }
            self.0.push(other_change);
        }
    }

    // TODO(#11099): Use CryptoHash::default() for the default root hash.
    // For this, search for .then_some(ContractChanges::default().merklize());
    pub fn merklize(&self) -> MerkleHash {
        let changes: Vec<CodeHashWithRefCount> = self
            .0
            .iter()
            .map(|c| CodeHashWithRefCount {
                code_hash: c.code_hash,
                refcount_delta: c.refcount_delta,
            })
            .collect_vec();
        let (root, _paths) = merklize(changes.as_slice());
        root
    }
}

// Used to calculate the merkle hash of the contract changes.
#[derive(BorshSerialize, ProtocolSchema)]
struct CodeHashWithRefCount {
    code_hash: CodeHash,
    refcount_delta: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ContractChange {
    pub code_hash: CodeHash,
    pub code: Option<CodeBytes>,
    pub refcount_delta: u64,
}

impl ContractChange {
    pub fn validate(&self) -> Option<String> {
        if self.refcount_delta == 0 {
            return Some("Refcount delta is zero".to_string());
        }
        if self.code_hash == CryptoHash::default() {
            return Some("Code hash is set to default".to_string());
        }
        if let Some(code) = self.code.as_ref() {
            if code.is_empty() {
                return Some("Code is empty".to_string());
            }
            if hash(code) != self.code_hash {
                return Some("Invalid code hash".to_string());
            }
        }
        None
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
    ) -> Result<Self, std::io::Error> {
        let inner = EncodedContractChangesInner::new(contract_changes)?;
        let signature = signer.sign_contract_changes(&inner);
        Ok(Self { inner, signature })
    }

    pub fn metadata(&self) -> &ContractChangesMetadata {
        &self.inner.metadata
    }

    pub fn chunk_production_key(&self) -> ChunkProductionKey {
        self.metadata().chunk_production_key()
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
    fn new(contract_changes: ChunkContractChanges) -> Result<Self, std::io::Error> {
        let (encoded_changes, _raw_size) = EncodedContractChanges::encode(&contract_changes)?;
        Ok(Self {
            metadata: contract_changes.metadata,
            encoded_changes,
            signature_differentiator: "ContractChanges".to_owned(),
        })
    }
}
