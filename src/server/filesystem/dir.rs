use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::io::ErrorKind;
use std::ops::Deref;
use std::os::unix::fs::DirBuilderExt;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::Arc;

use async_std::fs;
use async_std::fs::{DirBuilder, OpenOptions};
use async_std::sync::Mutex;
use fuse::{FileAttr, FileType};
use futures::stream::FuturesOrdered;
use futures::StreamExt;
use log::{debug, warn};

use crate::errno::Errno;
use crate::helper::compare_collection;
use crate::{Apply, Result};

use super::attr::metadata_to_file_attr;
use super::entry::Entry;
use super::inode::Inode;
use super::inode::InodeMap;
use super::SetAttr;

#[derive(Debug)]
struct InnerDir {
    parent: Option<Dir>,
    inode: Inode,
    name: OsString,
    children: BTreeMap<OsString, Entry>,
}

#[derive(Debug, Clone)]
pub struct Dir(Arc<Mutex<InnerDir>>);

impl Dir {
    pub fn new_root() -> Self {
        Self(Arc::new(Mutex::new(InnerDir {
            parent: None,
            inode: 1,
            name: OsString::from("/"),
            children: BTreeMap::new(),
        })))
    }

    pub fn from_exist(parent: &Dir, name: &OsStr, inode: Inode) -> Self {
        Self(Arc::new(Mutex::new(InnerDir {
            parent: Some(parent.clone()),
            inode,
            name: name.to_os_string(),
            children: BTreeMap::new(),
        })))
    }

    pub async fn lookup(
        &mut self,
        name: impl AsRef<OsStr>,
        inode_map: &mut InodeMap,
        inode_gen: &mut Inode,
    ) -> Result<Entry> {
        let name = name.as_ref();

        let path = self.get_absolute_path().apply(|path| path.push(name));

        let mut inner = self.0.try_lock().unwrap();

        let metadata = match fs::metadata(&path).await {
            Err(err) => {
                if err.kind() == ErrorKind::NotFound {
                    if let Some(entry) = inner.children.remove(name) {
                        let inode = entry.get_inode();

                        if let Entry::Dir(mut dir) = entry {
                            dir.delete_children(inode_map);
                        }

                        inode_map.remove(&inode);
                    }
                }

                return Err(err.into());
            }

            Ok(metadata) => metadata,
        };

        match inner.children.get(name) {
            None => {
                *inode_gen += 1;
                let new_inode = *inode_gen.deref();

                let entry = Entry::new(new_inode, name, self, &metadata);

                inner.children.insert(name.to_os_string(), entry.clone());
                inode_map.insert(new_inode, entry.clone());

                Ok(entry)
            }

            Some(entry) => {
                // child entry type isn't right, fix it
                if (metadata.is_dir() && entry.is_file()) || (metadata.is_file() && entry.is_dir())
                {
                    let inode = entry.get_inode();

                    if let Entry::Dir(mut dir) = inner.children.remove(name).expect("checked") {
                        dir.delete_children(inode_map);
                    }

                    inode_map.remove(&inode);

                    *inode_gen += 1;

                    let new_inode = *inode_gen.deref();

                    let entry = Entry::new(new_inode, name, self, &metadata);

                    inner.children.insert(name.to_os_string(), entry.clone());
                    inode_map.insert(new_inode, entry);
                }

                Ok(inner.children.get(name).expect("checked").clone())
            }
        }
    }

    pub async fn readdir(
        &mut self,
        inode_map: &mut InodeMap,
        inode_gen: &mut Inode,
    ) -> Result<Vec<(Inode, FileType, OsString, FileAttr)>> {
        let path = self.get_absolute_path();

        let mut entries = fs::read_dir(&path).await?;

        let mut entry_futures = FuturesOrdered::new();

        while let Some(entry) = entries.next().await {
            let entry = entry?;

            entry_futures.push(async move {
                let name = entry.file_name();

                let metadata = match entry.metadata().await {
                    Err(err) => return Err((name, Errno::from(err))),

                    Ok(metadata) => metadata,
                };

                Ok((name, metadata))
            });
        }

        let entries = entry_futures
            .filter_map(|result| async {
                match result {
                    Err((name, err)) => {
                        warn!(
                            "when readdir in {:?}, child entry {:?} has error {}",
                            path, name, err
                        );

                        None
                    }

                    Ok(success) => Some(success),
                }
            })
            .collect::<Vec<_>>()
            .await;

        let mut inner = self.0.try_lock().unwrap();

        // fix dir children map
        let new_names = entries.iter().map(|(name, _)| name.to_os_string());
        let old_names = inner.children.keys().map(|name| name.to_os_string());

        let need_delete_name = compare_collection(new_names, old_names);

        // remove not exist child entry
        for name in need_delete_name {
            debug!("clean not exist child entry {:?} in {}", name, inner.inode);

            match inner.children.remove(&name).expect("checked") {
                Entry::File(file) => {
                    inode_map.remove(&file.get_inode());
                }

                Entry::Dir(mut dir) => dir.delete_children(inode_map),
            }
        }

        let mut child_info = Vec::with_capacity(entries.len());

        let current_dir_attr = metadata_to_file_attr(inner.inode, fs::metadata(&path).await?)?;
        let parent_dir_attr = if let Some(dir) = &inner.parent {
            metadata_to_file_attr(
                dir.get_inode(),
                fs::metadata(&dir.get_absolute_path()).await?,
            )?
        } else {
            current_dir_attr
        };

        child_info.extend_from_slice(&[
            (
                inner.inode,
                FileType::Directory,
                OsString::from("."),
                current_dir_attr,
            ),
            (
                parent_dir_attr.ino,
                FileType::Directory,
                OsString::from(".."),
                parent_dir_attr,
            ),
        ]);

        // insert new child entry, update same name but type not same child entry and collect child
        // entry info.
        for (name, metadata) in entries {
            match inner.children.get(&name) {
                None => {
                    *inode_gen += 1;

                    let new_inode = *inode_gen.deref();

                    let entry = Entry::new(new_inode, &name, self, &metadata);
                    let kind = entry.get_kind();

                    inode_map.insert(new_inode, entry.clone());

                    inner.children.insert(name.to_os_string(), entry);

                    // collect
                    child_info.push((
                        new_inode,
                        kind,
                        name,
                        metadata_to_file_attr(new_inode, metadata)?,
                    ));
                }

                Some(entry) => {
                    let mut need_update = false;

                    if metadata.is_dir() {
                        if let Entry::File(file) = entry {
                            need_update = true;

                            let inode = file.get_inode();

                            inode_map.remove(&inode);
                        } else {
                            let inode = if let Entry::Dir(dir) = entry {
                                dir.0.try_lock().unwrap().inode
                            } else {
                                unreachable!()
                            };

                            // collect
                            child_info.push((
                                inode,
                                FileType::Directory,
                                name.to_os_string(),
                                metadata_to_file_attr(inode, metadata.clone())?,
                            ));
                        }
                    } else if let Entry::Dir(dir) = entry {
                        need_update = true;

                        let dir = dir.0.try_lock().unwrap();
                        let inode = dir.inode;
                        let name = dir.name.to_os_string();

                        drop(dir);

                        let dir = inner.children.remove(&name).expect("checked");

                        if let Entry::Dir(mut dir) = dir {
                            dir.delete_children(inode_map);
                        }

                        inode_map.remove(&inode);
                    } else {
                        let inode = if let Entry::File(file) = entry {
                            file.get_inode()
                        } else {
                            unreachable!()
                        };

                        // collect
                        child_info.push((
                            inode,
                            FileType::RegularFile,
                            name.to_os_string(),
                            metadata_to_file_attr(inode, metadata.clone())?,
                        ));
                    }

                    if !need_update {
                        continue;
                    }

                    *inode_gen += 1;

                    let new_inode = *inode_gen.deref();

                    let entry = Entry::new(new_inode, &name, self, &metadata);
                    let kind = entry.get_kind();

                    inode_map.insert(new_inode, entry.clone());

                    inner.children.insert(name.to_os_string(), entry);

                    // collect
                    child_info.push((
                        new_inode,
                        kind,
                        name,
                        metadata_to_file_attr(new_inode, metadata)?,
                    ));
                }
            }
        }

        Ok(child_info)
    }

    pub async fn remove_child(
        &mut self,
        name: impl AsRef<OsStr>,
        kind: FileType,
        inode_map: &mut InodeMap,
        inode_gen: &mut Inode,
    ) -> Result<()> {
        let name = name.as_ref();

        let child_path = self.get_absolute_path().apply(|path| path.push(name));

        let mut inner = self.0.try_lock().unwrap();

        let metadata = match fs::metadata(&child_path).await {
            Err(err) => {
                if err.kind() == ErrorKind::NotFound {
                    if let Some(entry) = inner.children.remove(name) {
                        let inode = entry.get_inode();

                        if let Entry::Dir(mut dir) = entry {
                            dir.delete_children(inode_map);
                        }

                        inode_map.remove(&inode);
                    }
                }

                return Err(err.into());
            }

            Ok(metadata) => metadata,
        };

        match inner.children.get(name) {
            None => {
                if metadata.is_dir() && kind == FileType::RegularFile {
                    return Err(libc::EISDIR.into());
                } else if kind == FileType::Directory {
                    return Err(libc::ENOTDIR.into());
                }

                if metadata.is_dir() {
                    fs::remove_dir_all(&child_path).await?;
                } else {
                    fs::remove_file(&child_path).await?;
                }

                Ok(())
            }

            Some(entry) => {
                // child entry type isn't right, fix it
                if (metadata.is_dir() && entry.is_file()) || (metadata.is_file() && entry.is_dir())
                {
                    let inode = entry.get_inode();

                    if let Entry::Dir(mut dir) = inner.children.remove(name).expect("checked") {
                        dir.delete_children(inode_map);
                    }

                    inode_map.remove(&inode);

                    *inode_gen += 1;

                    let new_inode = *inode_gen.deref();

                    let entry = Entry::new(new_inode, name, self, &metadata);

                    inner.children.insert(name.to_os_string(), entry.clone());
                    inode_map.insert(new_inode, entry);
                }

                if metadata.is_dir() && kind == FileType::RegularFile {
                    return Err(libc::EISDIR.into());
                } else if kind == FileType::Directory {
                    return Err(libc::ENOTDIR.into());
                }

                if metadata.is_dir() {
                    fs::remove_dir_all(&child_path).await?;
                } else {
                    fs::remove_file(&child_path).await?;
                }

                let entry = inner.children.remove(name).expect("checked");
                let inode = entry.get_inode();

                if let Entry::Dir(mut dir) = entry {
                    dir.delete_children(inode_map);
                }

                inode_map.remove(&inode);

                Ok(())
            }
        }
    }

    pub async fn create_dir(
        &mut self,
        name: impl AsRef<OsStr>,
        mode: u32,
        inode_map: &mut InodeMap,
        inode_gen: &mut Inode,
    ) -> Result<Entry> {
        self.create_child(name, mode, 0, FileType::Directory, inode_map, inode_gen)
            .await
    }

    pub async fn create_file(
        &mut self,
        name: impl AsRef<OsStr>,
        mode: u32,
        flags: i32,
        inode_map: &mut InodeMap,
        inode_gen: &mut Inode,
    ) -> Result<Entry> {
        self.create_child(
            name,
            mode,
            flags,
            FileType::RegularFile,
            inode_map,
            inode_gen,
        )
        .await
    }

    async fn create_child(
        &mut self,
        name: impl AsRef<OsStr>,
        mode: u32,
        flags: i32,
        kind: FileType,
        inode_map: &mut InodeMap,
        inode_gen: &mut Inode,
    ) -> Result<Entry> {
        let name = name.as_ref();

        let child_path = self.get_absolute_path().apply(|path| path.push(name));

        let mut inner = self.0.try_lock().unwrap();

        match fs::metadata(&child_path).await {
            Ok(metadata) => {
                if let Some(entry) = inner.children.get(name) {
                    // child entry type isn't right, fix it
                    if (metadata.is_dir() && entry.is_file())
                        || (metadata.is_file() && entry.is_dir())
                    {
                        let inode = entry.get_inode();

                        if let Entry::Dir(mut dir) = inner.children.remove(name).expect("checked") {
                            dir.delete_children(inode_map);
                        }

                        inode_map.remove(&inode);

                        *inode_gen += 1;

                        let new_inode = *inode_gen.deref();

                        let entry = Entry::new(new_inode, name, self, &metadata);

                        inner.children.insert(name.to_os_string(), entry.clone());
                        inode_map.insert(new_inode, entry);
                    }
                } else {
                    *inode_gen += 1;

                    let new_inode = *inode_gen.deref();

                    let entry = Entry::new(new_inode, name, self, &metadata);

                    inner.children.insert(name.to_os_string(), entry.clone());
                    inode_map.insert(new_inode, entry);
                }

                return Err(libc::EEXIST.into());
            }

            Err(err) => {
                if err.kind() != ErrorKind::NotFound {
                    return Err(err.into());
                }
            }
        }

        // name is not exist, can create dir now

        // try to clean old exist entry
        if let Some(entry) = inner.children.remove(name) {
            let inode = entry.get_inode();

            if let Entry::Dir(mut dir) = entry {
                dir.delete_children(inode_map);
            }

            inode_map.remove(&inode);
        }

        let metadata = if kind == FileType::Directory {
            DirBuilder::new().mode(mode).create(&child_path).await?;

            fs::metadata(&child_path).await?
        } else {
            let sys_file: async_std::fs::File = OpenOptions::new()
                .create_new(true)
                .mode(mode)
                .custom_flags(flags)
                .open(&child_path)
                .await?;

            sys_file.metadata().await?
        };

        *inode_gen += 1;

        let new_inode = *inode_gen.deref();

        let entry = Entry::new(new_inode, name, self, &metadata);

        inner.children.insert(name.to_os_string(), entry.clone());
        inode_map.insert(new_inode, entry.clone());

        Ok(entry)
    }

    pub async fn move_child_to_new_parent(
        &mut self,
        name: impl AsRef<OsStr>,
        new_parent: &mut Dir,
        new_name: impl AsRef<OsStr>,
        // mode: u32,
        inode_map: &mut InodeMap,
        inode_gen: &mut Inode,
    ) -> Result<()> {
        let name = name.as_ref();
        let new_name = new_name.as_ref();

        let old_path = self.get_absolute_path().apply(|path| path.push(name));
        let new_path = new_parent
            .get_absolute_path()
            .apply(|path| path.push(new_name));

        let mut inner = self.0.try_lock().unwrap();

        let metadata = match fs::metadata(&old_path).await {
            Err(err) => {
                if err.kind() == ErrorKind::NotFound {
                    if let Some(entry) = inner.children.remove(name) {
                        let inode = entry.get_inode();

                        if let Entry::Dir(mut dir) = entry {
                            dir.delete_children(inode_map);
                        }

                        inode_map.remove(&inode);
                    }
                }

                return Err(err.into());
            }

            Ok(metadata) => metadata,
        };

        match inner.children.get(name) {
            None => {
                // old parent doesn't have this child entry, we can rename directly and only add
                // child entry to new parent

                fs::rename(&old_path, &new_path).await?;

                /*let metadata = fs::metadata(&new_path).await?;
                let permissions = metadata.permissions().apply(|perm| perm.set_mode(mode));

                fs::set_permissions(&new_path, permissions).await?;*/

                let mut new_inner = new_parent.0.try_lock().unwrap();

                // try to clean old exist child entry
                if let Some(entry) = new_inner.children.remove(new_name) {
                    let inode = entry.get_inode();

                    if let Entry::Dir(mut dir) = entry {
                        dir.delete_children(inode_map);
                    }

                    inode_map.remove(&inode);
                }

                *inode_gen += 1;

                let new_inode = *inode_gen.deref();

                let entry = Entry::new(new_inode, name, new_parent, &metadata);

                new_inner
                    .children
                    .insert(new_name.to_os_string(), entry.clone());
                inode_map.insert(new_inode, entry);

                Ok(())
            }

            Some(entry) => {
                if (metadata.is_dir() && entry.is_file()) || (metadata.is_file() && entry.is_dir())
                {
                    let inode = entry.get_inode();

                    if let Entry::Dir(mut dir) = inner.children.remove(name).expect("checked") {
                        dir.delete_children(inode_map);
                    }

                    inode_map.remove(&inode);

                    *inode_gen += 1;

                    let new_inode = *inode_gen.deref();

                    let entry = Entry::new(new_inode, name, self, &metadata);

                    inner.children.insert(name.to_os_string(), entry.clone());
                    inode_map.insert(new_inode, entry);
                }

                fs::rename(&old_path, &new_path).await?;

                let mut entry = inner.children.remove(name).expect("checked");

                let mut new_inner = new_parent.0.try_lock().unwrap();

                // try to clean old exist child entry
                if let Some(entry) = new_inner.children.remove(new_name) {
                    let inode = entry.get_inode();

                    if let Entry::Dir(mut dir) = entry {
                        dir.delete_children(inode_map);
                    }

                    inode_map.remove(&inode);
                }

                entry.set_new_parent(new_parent);
                entry.set_new_name(new_name);

                new_inner.children.insert(new_name.to_os_string(), entry);

                Ok(())
            }
        }
    }

    pub async fn get_attr(&self) -> Result<FileAttr> {
        let path = self.get_absolute_path();

        let inode = self.0.try_lock().unwrap().inode;

        metadata_to_file_attr(inode, fs::metadata(path).await?)
    }

    pub async fn set_attr(&self, set_attr: SetAttr) -> Result<FileAttr> {
        let path = self.get_absolute_path();

        let metadata = fs::metadata(&path).await?;

        if let Some(mode) = set_attr.mode {
            let mut permissions = metadata.permissions();

            permissions.set_mode(mode);

            fs::set_permissions(&path, permissions).await?;
        }

        if set_attr.size.is_some() {
            return Err(libc::EISDIR.into());
        }

        self.get_attr().await
    }

    pub fn get_inode(&self) -> Inode {
        self.0.try_lock().unwrap().inode
    }

    pub fn get_absolute_path(&self) -> PathBuf {
        let inner = self.0.try_lock().unwrap();

        if let Some(parent) = &inner.parent {
            parent
                .get_absolute_path()
                .apply(|path| path.push(&inner.name))
        } else {
            PathBuf::from("/")
        }
    }

    pub fn set_new_parent(&mut self, new_parent: &Dir) {
        self.0.try_lock().unwrap().parent = Some(new_parent.clone());
    }

    pub fn set_new_name(&mut self, new_name: &OsStr) {
        self.0.try_lock().unwrap().name = new_name.to_os_string();
    }

    fn delete_children(&mut self, inode_map: &mut InodeMap) {
        let mut inner = self.0.try_lock().unwrap();

        for (_, entry) in inner.children.iter_mut() {
            match entry {
                Entry::Dir(dir) => {
                    dir.delete_children(inode_map);

                    inode_map.remove(&dir.0.try_lock().unwrap().inode);
                }

                Entry::File(file) => {
                    inode_map.remove(&file.get_inode());
                }
            }
        }
    }
}
