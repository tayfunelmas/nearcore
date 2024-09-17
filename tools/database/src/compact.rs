use crate::utils::{open_rocksdb, open_rocksdb_cold, resolve_column};
use clap::Parser;
use near_store::{db::Database, Mode, NodeStorage, Temperature};
use std::path::PathBuf;

#[derive(Parser)]
pub(crate) struct RunCompactionCommand {
    /// If specified only this column will compacted
    #[arg(short, long)]
    column: Option<String>,
    /// What store temperature should compaction run on. Allowed values are hot and cold but
    /// cold is only available when cold_store is configured.
    #[clap(long, short = 't', default_value = "hot")]
    store_temperature: Temperature,
}

impl RunCompactionCommand {
    pub(crate) fn run(&self, home: &PathBuf) -> anyhow::Result<()> {
        let config = nearcore::config::Config::from_file_skip_validation(
            &home.join(nearcore::config::CONFIG_FILENAME),
        )?;

        let storage = NodeStorage::opener(home, config.archive, &config.store, config.cold_store.as_ref()).open_in_mode(Mode::ReadWrite)?;
        let store = match self.store_temperature {
            Temperature::Hot => storage.get_hot_store(),
            Temperature::Cold => storage.get_cold_store().ok_or_else(|| anyhow::anyhow!("No cold store"))?,
        };
        // if let Some(col_name) = &self.column {
        //     db.compact_column(resolve_column(col_name)?)?;
        // } else {
        //     db.compact()?;
        // }
        store.compact()?;
        eprintln!("Compaction is finished!");
        Ok(())
    }
}
