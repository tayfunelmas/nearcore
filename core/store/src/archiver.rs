use std::collections::HashMap;
use std::{io, sync::Arc};

use near_primitives::block::Tip;
use std::io::Read;
use std::io::Write;
use strum::IntoEnumIterator;

use crate::db::assert_no_overwrite;
use crate::Store;
use crate::COLD_HEAD_KEY;
use crate::HEAD_KEY;
use crate::{
    config::{ArchivalStorageConfig, ArchivalStorageLocation},
    db::{refcount, ColdDB, DBOp, DBTransaction, Database},
    DBCol,
};

pub struct ArchivalStorageOpener {
    home_dir: std::path::PathBuf,
    config: ArchivalStorageConfig,
}

impl ArchivalStorageOpener {
    pub fn new(home_dir: std::path::PathBuf, config: ArchivalStorageConfig) -> Self {
        Self { home_dir, config }
    }

    pub fn open(&self, cold_db: Arc<ColdDB>) -> io::Result<Arc<Archiver>> {
        let storage: Arc<dyn ArchivalStorage> = match &self.config.storage {
            ArchivalStorageLocation::ColdDB => {
                tracing::info!(target: "archiver", "Using ColdDB as the archival storage location");
                Arc::new(ColdDBArchiver::new(cold_db.clone()))
            }
            ArchivalStorageLocation::Filesystem { path } => {
                let base_path = self.home_dir.join(path);
                tracing::info!(target: "archiver", path=%base_path.display(), "Using filesystem as the archival storage location");
                Arc::new(FilesystemArchiver::open(base_path.as_path())?)
            }
        };
        let cold_store = Store::new(cold_db.clone());
        Ok(Arc::new(Archiver { cold_store, cold_db, storage }))
    }
}

#[derive(Clone)]
pub struct Archiver {
    cold_store: Store,
    cold_db: Arc<ColdDB>,
    storage: Arc<dyn ArchivalStorage>,
}

impl Archiver {
    pub(crate) fn from(cold_db: Arc<ColdDB>) -> Arc<Archiver> {
        let storage: Arc<dyn ArchivalStorage> = Arc::new(ColdDBArchiver::new(cold_db.clone()));
        let cold_store = Store::new(cold_db.clone());
        Arc::new(Archiver { cold_store, cold_db, storage })
    }

    pub fn get_head(&self) -> io::Result<Option<Tip>> {
        let cold_head = self.cold_store.get_ser::<Tip>(DBCol::BlockMisc, HEAD_KEY).unwrap();
        Ok(cold_head)
    }

    pub fn set_head(&self, tip: &Tip) -> io::Result<()> {
        // Write HEAD to the cold db.
        {
            let mut transaction = DBTransaction::new();
            transaction.set(DBCol::BlockMisc, HEAD_KEY.to_vec(), borsh::to_vec(&tip)?);
            self.cold_db.write(transaction).unwrap();
        }

        // Write COLD_HEAD_KEY to the cold db.
        {
            let mut transaction = DBTransaction::new();
            transaction.set(DBCol::BlockMisc, COLD_HEAD_KEY.to_vec(), borsh::to_vec(&tip)?);
            self.cold_db.write(transaction).unwrap();
        }
        Ok(())
    }

    pub fn write(&self, batch: DBTransaction) -> io::Result<()> {
        self.storage.write(batch)
    }

    pub fn cold_db(&self) -> Arc<ColdDB> {
        self.cold_db.clone()
    }
}

pub trait ArchivalStorage: Sync + Send {
    fn write(&self, batch: DBTransaction) -> io::Result<()>;
}

struct ColdDBArchiver {
    cold_db: Arc<ColdDB>,
}

impl ColdDBArchiver {
    fn new(cold_db: Arc<ColdDB>) -> Self {
        Self { cold_db }
    }
}

impl ArchivalStorage for ColdDBArchiver {
    fn write(&self, transaction: DBTransaction) -> io::Result<()> {
        self.cold_db.write(transaction)
    }
}

struct FilesystemArchiver {
    base_dir: rustix::fd::OwnedFd,
    col_to_dir: HashMap<DBCol, std::path::PathBuf>,
}

impl FilesystemArchiver {
    fn open(path: &std::path::Path) -> io::Result<Self> {
        let col_to_dir = Self::setup_dirs(path).unwrap();
        let dir = rustix::fs::open(path, rustix::fs::OFlags::DIRECTORY, rustix::fs::Mode::empty())
            .unwrap();
        tracing::debug!(
            target: "archiver",
            path = %path.display(),
            message = "opened archive directory"
        );
        Ok(Self { base_dir: dir, col_to_dir })
    }

    fn setup_dirs(base_path: &std::path::Path) -> io::Result<HashMap<DBCol, std::path::PathBuf>> {
        std::fs::create_dir_all(base_path).unwrap();
        let mut col_to_dir = HashMap::new();
        for col in DBCol::iter() {
            if col.is_cold() {
                let path: std::path::PathBuf =
                    [base_path, Self::to_dirname(col)].into_iter().collect();
                std::fs::create_dir(&path).unwrap();
                col_to_dir.insert(col, path);
            }
        }
        Ok(col_to_dir)
    }

    #[inline]
    fn get_path(&self, col: DBCol, key: &[u8]) -> std::path::PathBuf {
        let dirname = self.col_to_dir.get(&col);
        let filename = bs58::encode(key).with_alphabet(bs58::Alphabet::BITCOIN).into_string();
        [dirname.as_ref().unwrap(), std::path::Path::new(&filename)].into_iter().collect()
    }

    fn to_dirname(col: DBCol) -> &'static std::path::Path {
        <&str>::from(col).as_ref()
    }

    fn write_file(&self, col: DBCol, key: &[u8], value: &[u8]) -> io::Result<()> {
        use rustix::fs::{Mode, OFlags};
        let mut temp_file = tempfile::Builder::new()
            .make_in("", |filename| {
                let mode = Mode::RUSR | Mode::WUSR | Mode::RGRP | Mode::WGRP;
                let flags = OFlags::CREATE | OFlags::TRUNC | OFlags::WRONLY;
                Ok(std::fs::File::from(rustix::fs::openat(&self.base_dir, filename, flags, mode)?))
            })
            .unwrap();

        temp_file.write_all(value).unwrap();

        let temp_path = temp_file.into_temp_path();
        let file_path = self.get_path(col, key);
        rustix::fs::renameat(&self.base_dir, &*temp_path, &self.base_dir, file_path).unwrap();
        std::mem::forget(temp_path);
        Ok(())
    }

    fn read_file(&self, col: DBCol, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        use rustix::fs::{Mode, OFlags};
        let mode = Mode::empty();
        let flags = OFlags::RDONLY;
        let file_path = self.get_path(col, key);
        let file = rustix::fs::openat(&self.base_dir, &file_path, flags, mode);
        let file = match file {
            Err(rustix::io::Errno::NOENT) => return Ok(None),
            Err(e) => return Err(e.into()),
            Ok(file) => file,
        };
        let stat = rustix::fs::fstat(&file).unwrap();
        let mut buffer: Vec<u8> = Vec::with_capacity(stat.st_size.try_into().unwrap());
        let mut file = std::fs::File::from(file);
        file.read_to_end(&mut buffer).unwrap();
        Ok(Some(buffer))
    }

    fn delete_file(&self, col: DBCol, key: &[u8]) -> io::Result<()> {
        let file_path = self.get_path(col, key);
        Ok(rustix::fs::unlinkat(&self.base_dir, &file_path, rustix::fs::AtFlags::empty())?)
    }
}

impl ArchivalStorage for FilesystemArchiver {
    fn write(&self, transaction: DBTransaction) -> io::Result<()> {
        for op in transaction.ops {
            match op {
                DBOp::Set { col, key, value } => self.write_file(col, &key, &value),
                DBOp::Insert { col, key, value } => {
                    if cfg!(debug_assertions) {
                        if let Some(old_value) = self.read_file(col, &key)? {
                            assert_no_overwrite(col, &key, &value, &*old_value)
                        }
                    }
                    self.write_file(col, &key, &value)
                }
                DBOp::UpdateRefcount { col, key, value } => {
                    let existing = self.read_file(col, &key).unwrap();
                    let operands = [value.as_slice()];
                    let merged =
                        refcount::refcount_merge(existing.as_ref().map(Vec::as_slice), operands);
                    if merged.is_empty() {
                        self.delete_file(col, &key)
                    } else {
                        debug_assert!(
                            refcount::decode_value_with_rc(&merged).1 > 0,
                            "Inserting value with non-positive refcount"
                        );
                        self.write_file(col, &key, &merged)
                    }
                }
                DBOp::Delete { .. } | DBOp::DeleteAll { .. } | DBOp::DeleteRange { .. } => {
                    unreachable!("Delete operations unsupported")
                }
            }
            .unwrap();
        }
        Ok(())
    }
}
