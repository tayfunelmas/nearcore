use bytesize::ByteSize;
use near_chain::ChainStore;
use near_chain::ChainStoreAccess;
use near_primitives::{
    action::{Action, DeployContractAction},
    types::BlockHeight,
};
use near_store::Store;
use nearcore::NearConfig;

#[derive(clap::Parser)]
pub struct ContractChangesCmd {
    #[clap(long)]
    start_index: Option<BlockHeight>,
    #[clap(long)]
    end_index: Option<BlockHeight>,
}

impl ContractChangesCmd {
    pub fn run(self, near_config: NearConfig, store: Store) {
        print_contract_changes(self.start_index, self.end_index, near_config, store);
    }
}

struct ReportLine {
    height: BlockHeight,
    shard_id: usize,
    num_deploys: usize,
    num_deletes: usize,
    code_size: u64,
}

pub(crate) fn print_contract_changes(
    start_height: Option<BlockHeight>,
    end_height: Option<BlockHeight>,
    near_config: NearConfig,
    store: Store,
) {
    let chain_store = ChainStore::new(
        store,
        near_config.genesis.config.genesis_height,
        near_config.client_config.save_trie_changes,
    );
    let start_height: BlockHeight = start_height.unwrap_or_else(|| chain_store.tail().unwrap());
    let end_height: BlockHeight = end_height.unwrap_or_else(|| chain_store.head().unwrap().height);

    println!("Height, ShardId, NumDeployContractAction, NumDeleteAccount, TotalDeployedCodeSize");
    for height in start_height..=end_height {
        if let Ok(block_hash) = chain_store.get_block_hash_by_height(height) {
            let block = chain_store.get_block(&block_hash).unwrap();
            let chunks = block.chunks();
            for shard_id in 0..chunks.len() {
                let mut line =
                    ReportLine { height, shard_id, num_deploys: 0, num_deletes: 0, code_size: 0 };
                let chunk_header = &chunks[shard_id];
                let is_new_chunk: bool = chunk_header.is_new_chunk(height);
                if !is_new_chunk {
                    continue;
                }
                let chunk_hash = chunk_header.chunk_hash();
                let chunk = chain_store.get_chunk(&chunk_hash).unwrap();
                for tx in chunk.transactions() {
                    for action in tx.transaction.actions() {
                        match action {
                            Action::DeployContract(DeployContractAction { code }) => {
                                line.num_deploys += 1;
                                line.code_size += code.len() as u64;
                            }
                            Action::DeleteAccount(_) => {
                                line.num_deletes += 1;
                            }
                            _ => {} // ignore other actions
                        }
                    }
                }
                println!(
                    "{},{},{},{},{}",
                    line.height,
                    line.shard_id,
                    line.num_deploys,
                    line.num_deletes,
                    ByteSize::b(line.code_size)
                );
            }
        }
    }
}
