use async_std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub enum EntryPath {
    Dir(PathBuf),
    File(PathBuf),
}

impl EntryPath {
    pub fn get_path(&self) -> &Path {
        match self {
            EntryPath::Dir(path) => path,
            EntryPath::File(path) => path,
        }
    }

    #[inline]
    pub fn is_file(&self) -> bool {
        matches!(*self, EntryPath::File(_))
    }

    #[inline]
    pub fn is_dir(&self) -> bool {
        !self.is_file()
    }
}
