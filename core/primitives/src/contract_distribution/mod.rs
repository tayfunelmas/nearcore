use borsh::{BorshDeserialize, BorshSerialize};
use bytesize::ByteSize;
use itertools::Itertools;
use near_crypto::{PublicKey, Signature};
use near_primitives_core::{
    hash::{hash, CryptoHash},
    types::{BlockHeight, CodeBytes, CodeHash, MerkleHash, ProtocolVersion, ShardId},
    version::ProtocolFeature,
};
use near_schema_checker_lib::ProtocolSchema;

use crate::{
    merkle::merklize,
    stateless_validation::{ChunkProductionKey, SignatureDifferentiator},
    types::EpochId,
    utils::compression::CompressedData,
    validator_signer::ValidatorSigner,
};

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ContractChangesMetadata {
    /// Epoch ID in which the chunk is created.
    pub epoch_id: EpochId,
    /// Block hash at which this chunk is included (applied).
    pub block_hash: CryptoHash,
    /// Block height at which this chunk is created.
    pub height_created: BlockHeight,
    /// Shard ID of the chunk.
    pub shard_id: ShardId,
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

impl ChunkContractChanges {
    pub fn new(
        epoch_id: EpochId,
        block_hash: CryptoHash,
        height_created: BlockHeight,
        shard_id: ShardId,
        changes: ContractChanges,
    ) -> Self {
        let metadata = ContractChangesMetadata { epoch_id, block_hash, height_created, shard_id };
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

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn merkle_root(&self) -> MerkleHash {
        // If no change, then return the default hash.
        if self.is_empty() {
            return MerkleHash::default();
        }
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

    /// Returns the merkle root for the default (empty) changes or None if not enabled in the given protocol version.
    pub fn default_merkle_root(protocol_version: ProtocolVersion) -> Option<MerkleHash> {
        ProtocolFeature::ExcludeContractCodeFromStateWitness
            .enabled(protocol_version)
            .then(|| MerkleHash::default())
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
        if self.code_hash == CodeHash::default() {
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

#[cfg(test)]
mod tests {
    use near_primitives_core::{types::MerkleHash, version::ProtocolFeature};

    use crate::contract_distribution::ContractChanges;

    #[test]
    fn test_default_merkle_hash() {
        assert_eq!(
            ContractChanges::default_merkle_root(
                ProtocolFeature::StatelessValidation.protocol_version()
            ),
            None
        );
        assert_eq!(
            ContractChanges::default_merkle_root(
                ProtocolFeature::ExcludeContractCodeFromStateWitness.protocol_version()
            ),
            Some(MerkleHash::default())
        );
    }
}
