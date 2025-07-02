use std::{
    path::PathBuf,
    sync::{Arc, LazyLock},
};

use gix::{
    attrs::{
        search::{MetadataCollection, Outcome},
        Search,
    },
    glob::pattern::Case,
};
use tokio::io::AsyncReadExt as _;

use crate::{
    backend::TreeValue,
    merged_tree::MergedTree,
    repo_path::{RepoPath, RepoPathBuf, RepoPathComponent},
    store::Store,
};

static GITATTRIBUTES_COMPONENT: LazyLock<&RepoPathComponent> = LazyLock::new(|| {
    RepoPathComponent::new(".gitattributes").expect("Always a valid repo path component.")
});

/// TODO
pub struct GitAttributesResolver<'a> {
    tree: &'a MergedTree,
    store: Arc<Store>,
    collection: MetadataCollection,
    search: Search,
    gitattributes_dirs: Vec<RepoPathBuf>,
    current_dir: Option<RepoPathBuf>,
}

impl<'a> GitAttributesResolver<'a> {
    /// TODO
    pub fn new(tree: &'a MergedTree, store: Arc<Store>) -> GitAttributesResolver<'a> {
        let mut collection = MetadataCollection::default();
        let search = Search::new_globals(
            Vec::<PathBuf>::new(),
            &mut Vec::<u8>::new(),
            &mut collection,
        )
        .expect("No files to read");
        Self {
            tree,
            store,
            collection,
            search,
            gitattributes_dirs: Vec::new(),
            current_dir: None,
        }
    }

    /// TODO
    pub async fn resolve(&mut self, file_path: &RepoPath) -> ResolvedAttributes {
        self.change_dir(file_path.parent().unwrap()).await;
        let mut outcome = Outcome::default();
        outcome.initialize(&self.collection);
        self.search.pattern_matching_relative_path(
            file_path.as_internal_file_string().into(),
            Case::default(),
            Some(false),
            &mut outcome,
        );
        let mut text: TextAttribute = TextAttribute::Unspecified;
        let mut eol: EolAttribute = EolAttribute::Unspecified;
        for m in outcome.iter() {
            match m.assignment.name.as_str() {
                "text" => {
                    if m.assignment.state.is_set() {
                        if let Some(value) = m.assignment.state.as_bstr() {
                            if value == "auto" {
                                text = TextAttribute::Auto;
                            } else {
                                eprintln!("unknown value for attribute 'text': {value}");
                            }
                        } else {
                            text = TextAttribute::Set;
                        }
                    } else if m.assignment.state.is_unset() {
                        text = TextAttribute::Unset;
                    }
                }
                "eol" => {
                    if let Some(value) = m.assignment.state.as_bstr() {
                        if value == "lf" {
                            eol = EolAttribute::Lf;
                        } else if value == "crlf" {
                            eol = EolAttribute::Crlf;
                        } else {
                            eprintln!("unknown value for attribute 'eol': {value}");
                        }
                    }
                }
                _ => {}
            }
        }
        if text == TextAttribute::Unspecified && eol != EolAttribute::Unspecified {
            text = TextAttribute::Set;
        }
        ResolvedAttributes { text, eol }
    }

    async fn change_dir(&mut self, target_dir: &RepoPath) {
        let mut current = self
            .current_dir
            .as_ref()
            .map(|d| d.to_owned())
            .unwrap_or_else(RepoPathBuf::root);
        if self.current_dir.is_none() {
            // First time usage -> potentially add root gitattributes
            self.add_gitattributes_for_dir(&current).await;
        } else {
            // Non-first-time usage
            while !target_dir.starts_with(&current) {
                if &current == self.gitattributes_dirs.last().unwrap() {
                    self.gitattributes_dirs.pop();
                    let _ = &self.search.pop_pattern_list();
                }
                current = current.parent().unwrap().to_owned();
            }
        }
        for component in target_dir.strip_prefix(&current).unwrap().components() {
            current = current.join(component);
            self.add_gitattributes_for_dir(&current).await;
        }
        self.current_dir = Some(target_dir.to_owned());
    }

    async fn add_gitattributes_for_dir(&mut self, dir: &RepoPath) {
        let gitattributes_path = dir.join(&GITATTRIBUTES_COMPONENT);
        if let Some(TreeValue::File {
            id: attributes_file_id,
            executable: _,
            copy_id: _,
        }) = self
            .tree
            .path_value(&gitattributes_path)
            .unwrap()
            .as_normal()
        {
            let mut buf = Vec::<u8>::new();
            self.store
                .read_file(&gitattributes_path, attributes_file_id)
                .await
                .unwrap()
                .read_to_end(&mut buf)
                .await
                .unwrap();
            self.gitattributes_dirs.push(dir.to_owned());
            self.search.add_patterns_buffer(
                &buf,
                gitattributes_path.as_internal_file_string().into(),
                None,
                &mut self.collection,
                false,
            );
        }
    }
}

/// TODO
#[derive(Debug)]
pub struct ResolvedAttributes {
    /// TODO
    pub text: TextAttribute,
    /// TODO
    pub eol: EolAttribute,
}

/// TODO
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TextAttribute {
    /// TODO
    Set,
    /// TODO
    Unset,
    /// TODO
    Auto,
    /// TODO
    Unspecified,
}

/// TODO
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EolAttribute {
    /// TODO
    Lf,
    /// TODO
    Crlf,
    /// TODO
    Unspecified,
}
