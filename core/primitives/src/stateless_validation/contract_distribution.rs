use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::{PublicKey, Signature};
use near_primitives_core::types::{BlockHeight, ShardId};
use near_schema_checker_lib::ProtocolSchema;

use crate::{sharding::ShardChunkHeader, types::EpochId, validator_signer::ValidatorSigner};

use super::{ChunkProductionKey, SignatureDifferentiator};

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ContractChanges {
    inner: ContractChangesInner,
    pub signature: Signature,
}

impl ContractChanges {
    pub fn new(
        epoch_id: EpochId,
        chunk_header: ShardChunkHeader,
        signer: &ValidatorSigner,
    ) -> Self {
        let inner = ContractChangesInner::new(epoch_id, chunk_header);
        let signature = signer.sign_contract_changes(&inner);
        Self { inner, signature }
    }

    pub fn chunk_production_key(&self) -> ChunkProductionKey {
        ChunkProductionKey {
            shard_id: self.inner.shard_id,
            epoch_id: self.inner.epoch_id,
            height_created: self.inner.height_created,
        }
    }

    pub fn verify(&self, public_key: &PublicKey) -> bool {
        let data = borsh::to_vec(&self.inner).unwrap();
        self.signature.verify(&data, public_key)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ContractChangesInner {
    /// Chunk production metadata (shard_id, epoch_id, height_created):
    /// Shard ID of the chunk.
    shard_id: ShardId,
    /// Epoch ID in which the chunk is created.
    epoch_id: EpochId,
    /// Block height at which this chunk is created.
    height_created: BlockHeight,

    signature_differentiator: SignatureDifferentiator,
}

impl ContractChangesInner {
    fn new(epoch_id: EpochId, chunk_header: ShardChunkHeader) -> Self {
        Self {
            epoch_id,
            shard_id: chunk_header.shard_id(),
            height_created: chunk_header.height_created(),
            signature_differentiator: "ContractChanges".to_owned(),
        }
    }
}
