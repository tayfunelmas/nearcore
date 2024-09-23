use borsh::{BorshDeserialize, BorshSerialize};
use bytesize::ByteSize;
use near_crypto::{PublicKey, Signature};
use near_primitives_core::types::{BlockHeight, ShardId};
use near_schema_checker_lib::ProtocolSchema;
use std::io::Error;

use crate::{
    action::Action, sharding::ShardChunkHeader, types::EpochId, utils::compression::CompressedData,
    validator_signer::ValidatorSigner,
};

use super::{state_witness::ChunkStateTransition, SignatureDifferentiator};

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct SignedEncodedContractChanges {
    inner: EncodedContractChangesInner,
    pub signature: Signature,
}

impl SignedEncodedContractChanges {
    pub fn new(contract_changes: ContractChanges, signer: &ValidatorSigner) -> Result<Self, Error> {
        let inner = EncodedContractChangesInner::new(contract_changes)?;
        let signature = signer.sign_contract_changes(&inner);
        Ok(Self { inner, signature })
    }

    pub fn verify(&self, public_key: &PublicKey) -> bool {
        let data = borsh::to_vec(&self.inner).unwrap();
        self.signature.verify(&data, public_key)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct EncodedContractChangesInner {
    metadata: ContractChangesMetadata,
    encoded_changes: EncodedContractChanges,
    signature_differentiator: SignatureDifferentiator,
}

impl EncodedContractChangesInner {
    fn new(contract_changes: ContractChanges) -> Result<Self, Error> {
        let (encoded_changes, _raw_size) = EncodedContractChanges::encode(&contract_changes)?;
        Ok(Self {
            metadata: contract_changes.metadata,
            encoded_changes,
            signature_differentiator: "ContractChanges".to_owned(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ContractChangesMetadata {
    /// Chunk production metadata (shard_id, epoch_id, height_created):
    /// Epoch ID in which the chunk is created.
    pub epoch_id: EpochId,
    /// Block height at which this chunk is created.
    pub height_created: BlockHeight,
    /// Shard ID of the chunk.
    pub shard_id: ShardId,
}

impl ContractChangesMetadata {
    pub fn new(epoch_id: EpochId, height_created: BlockHeight, shard_id: ShardId) -> Self {
        Self { epoch_id, height_created, shard_id }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ContractChanges {
    pub metadata: ContractChangesMetadata,
    pub state_transition: ChunkStateTransition,
    pub actions: Vec<Action>,
}

impl ContractChanges {
    pub fn new(
        epoch_id: EpochId,
        chunk_header: &ShardChunkHeader,
        state_transition: ChunkStateTransition,
        actions: Vec<Action>,
    ) -> Self {
        let metadata = ContractChangesMetadata::new(
            epoch_id,
            chunk_header.height_created(),
            chunk_header.shard_id(),
        );
        Self { metadata, state_transition, actions }
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
        ContractChanges,
        MAX_UNCOMPRESSED_CONTRACT_CHANGES_SIZE,
        CONTRACT_CHANGES_COMPRESSION_LEVEL,
    > for EncodedContractChanges
{
}
