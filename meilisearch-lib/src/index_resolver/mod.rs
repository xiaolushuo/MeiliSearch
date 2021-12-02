pub mod error;
pub mod index_store;
pub mod meta_store;

use std::convert::TryInto;
use std::path::Path;

use chrono::Utc;
use error::{IndexResolverError, Result};
use heed::Env;
use index_store::{IndexStore, MapIndexStore};
use meilisearch_error::ResponseError;
use meta_store::{HeedMetaStore, IndexMetaStore};
use milli::update::DocumentDeletionResult;
use serde::{Deserialize, Serialize};
use tokio::task::spawn_blocking;
use uuid::Uuid;

use crate::index::update_handler::UpdateHandler;
use crate::index::{error::Result as IndexResult, Index};
use crate::options::IndexerOpts;
use crate::tasks::batch::Batch;
use crate::tasks::task::{DocumentDeletion, Job, Task, TaskContent, TaskEvent, TaskId, TaskResult};
use crate::tasks::Pending;
use crate::tasks::TaskPerformer;
use crate::update_file_store::UpdateFileStore;

use self::meta_store::IndexMeta;

pub type HardStateIndexResolver = IndexResolver<HeedMetaStore, MapIndexStore>;

/// An index uid is composed of only ascii alphanumeric characters, - and _, between 1 and 400
/// bytes long
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct IndexUid(#[cfg_attr(test, proptest(regex("[a-zA-Z0-9_-]{1,400}")))] String);

pub fn create_index_resolver(
    path: impl AsRef<Path>,
    index_size: usize,
    indexer_opts: &IndexerOpts,
    meta_env: heed::Env,
    file_store: UpdateFileStore,
) -> anyhow::Result<HardStateIndexResolver> {
    let uuid_store = HeedMetaStore::new(meta_env)?;
    let index_store = MapIndexStore::new(&path, index_size, indexer_opts)?;
    Ok(IndexResolver::new(uuid_store, index_store, file_store))
}

impl IndexUid {
    pub fn new(uid: String) -> Result<Self> {
        if !uid
            .chars()
            .all(|x| x.is_ascii_alphanumeric() || x == '-' || x == '_')
            || !(1..=400).contains(&uid.len())
        {
            Err(IndexResolverError::BadlyFormatted(uid))
        } else {
            Ok(Self(uid))
        }
    }

    #[cfg(test)]
    pub fn new_unchecked(s: impl AsRef<str>) -> Self {
        Self(s.as_ref().to_string())
    }

    pub fn into_inner(self) -> String {
        self.0
    }

    /// Return a reference over the inner str.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::ops::Deref for IndexUid {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryInto<IndexUid> for String {
    type Error = IndexResolverError;

    fn try_into(self) -> Result<IndexUid> {
        IndexUid::new(self)
    }
}

#[async_trait::async_trait]
impl<U, I> TaskPerformer for IndexResolver<U, I>
where
    U: IndexMetaStore + Send + Sync + 'static,
    I: IndexStore + Send + Sync + 'static,
{
    type Error = ResponseError;

    async fn process(&self, mut batch: Batch) -> Batch {
        // Until batching is implemented, all batch should contain only one update.
        debug_assert_eq!(batch.len(), 1);

        match batch.tasks.first_mut() {
            Some(Pending::Task(task)) => {
                task.events.push(TaskEvent::Processing(Utc::now()));

                match self.process_task(task).await {
                    Ok(success) => {
                        task.events.push(TaskEvent::Succeded {
                            result: success,
                            timestamp: Utc::now(),
                        });
                    }
                    Err(err) => task.events.push(TaskEvent::Failed {
                        error: err.into(),
                        timestamp: Utc::now(),
                    }),
                }
            }
            Some(Pending::Job(job)) => {
                let job = std::mem::take(job);
                self.process_job(job).await;
            }

            None => (),
        }

        batch
    }

    async fn finish(&self, batch: &Batch) {
        for task in &batch.tasks {
            if let Some(content_uuid) = task.get_content_uuid() {
                if let Err(e) = self.file_store.delete(content_uuid).await {
                    log::error!("error deleting update file: {}", e);
                }
            }
        }
    }
}

pub struct IndexResolver<U, I> {
    index_uuid_store: U,
    index_store: I,
    file_store: UpdateFileStore,
}

impl IndexResolver<HeedMetaStore, MapIndexStore> {
    pub fn load_dump(
        src: impl AsRef<Path>,
        dst: impl AsRef<Path>,
        index_db_size: usize,
        env: Env,
        indexer_opts: &IndexerOpts,
    ) -> anyhow::Result<()> {
        HeedMetaStore::load_dump(&src, env)?;
        let indexes_path = src.as_ref().join("indexes");
        let indexes = indexes_path.read_dir()?;
        let update_handler = UpdateHandler::new(indexer_opts)?;
        for index in indexes {
            Index::load_dump(&index?.path(), &dst, index_db_size, &update_handler)?;
        }

        Ok(())
    }
}

impl<U, I> IndexResolver<U, I>
where
    U: IndexMetaStore,
    I: IndexStore,
{
    pub fn new(index_uuid_store: U, index_store: I, file_store: UpdateFileStore) -> Self {
        Self {
            index_uuid_store,
            index_store,
            file_store,
        }
    }

    async fn process_task(&self, task: &Task) -> Result<TaskResult> {
        let index_uid = task.index_uid.clone();
        match &task.content {
            TaskContent::DocumentAddition {
                content_uuid,
                merge_strategy,
                primary_key,
                ..
            } => {
                let primary_key = primary_key.clone();
                let content_uuid = *content_uuid;
                let method = *merge_strategy;

                let index = self.get_or_create_index(index_uid, task.id).await?;
                let file_store = self.file_store.clone();
                let result = spawn_blocking(move || {
                    index.update_documents(method, content_uuid, primary_key, file_store)
                })
                .await??;

                Ok(result.into())
            }
            TaskContent::DocumentDeletion(DocumentDeletion::Ids(ids)) => {
                let ids = ids.clone();
                let index = self.get_index(index_uid.into_inner()).await?;

                let DocumentDeletionResult {
                    deleted_documents, ..
                } = spawn_blocking(move || index.delete_documents(&ids)).await??;

                Ok(TaskResult::DocumentDeletion { deleted_documents })
            }
            TaskContent::DocumentDeletion(DocumentDeletion::Clear) => {
                let index = self.get_index(index_uid.into_inner()).await?;
                let deleted_documents = spawn_blocking(move || -> IndexResult<u64> {
                    let number_documents = index.stats()?.number_of_documents;
                    index.clear_documents()?;
                    Ok(number_documents)
                })
                .await??;

                Ok(TaskResult::ClearAll { deleted_documents })
            }
            TaskContent::SettingsUpdate {
                settings,
                is_deletion,
            } => {
                let index = if *is_deletion {
                    self.get_index(index_uid.into_inner()).await?
                } else {
                    self.get_or_create_index(index_uid, task.id).await?
                };

                let settings = settings.clone();
                spawn_blocking(move || index.update_settings(&settings.check())).await??;

                Ok(TaskResult::Other)
            }
            TaskContent::IndexDeletion => {
                let index = self.delete_index(index_uid.into_inner()).await?;

                let deleted_documents = spawn_blocking(move || -> IndexResult<u64> {
                    Ok(index.stats()?.number_of_documents)
                })
                .await??;

                Ok(TaskResult::ClearAll { deleted_documents })
            }
            TaskContent::IndexCreation { primary_key } => {
                let index = self.create_index(index_uid, task.id).await?;

                if let Some(primary_key) = primary_key {
                    let primary_key = primary_key.clone();
                    spawn_blocking(move || index.update_primary_key(primary_key)).await??;
                }

                Ok(TaskResult::Other)
            }
            TaskContent::IndexUpdate { primary_key } => {
                let index = self.get_index(index_uid.into_inner()).await?;

                if let Some(primary_key) = primary_key {
                    let primary_key = primary_key.clone();
                    spawn_blocking(move || index.update_primary_key(primary_key)).await??;
                }

                Ok(TaskResult::Other)
            }
        }
    }

    async fn process_job(&self, job: Job) {
        match job {
            Job::Dump { ret, path } => {
                log::trace!("The Dump task is getting executed");

                if ret.send(self.dump(path).await).is_err() {
                    log::error!("The dump actor died.");
                }
            }
            Job::Empty => log::error!("Tried to process an empty task."),
            Job::Snapshot(job) => {
                if let Err(e) = job.run().await {
                    log::error!("Error performing snapshot: {}", e);
                }
            }
        }
    }

    pub async fn dump(&self, path: impl AsRef<Path>) -> Result<()> {
        for (_, index) in self.list().await? {
            index.dump(&path)?;
        }
        self.index_uuid_store.dump(path.as_ref().to_owned()).await?;
        Ok(())
    }

    async fn create_index(&self, uid: IndexUid, creation_task_id: TaskId) -> Result<Index> {
        match self.index_uuid_store.get(uid.into_inner()).await? {
            (uid, Some(_)) => Err(IndexResolverError::IndexAlreadyExists(uid)),
            (uid, None) => {
                let uuid = Uuid::new_v4();
                let index = self.index_store.create(uuid).await?;
                match self
                    .index_uuid_store
                    .insert(
                        uid,
                        IndexMeta {
                            uuid,
                            creation_task_id,
                        },
                    )
                    .await
                {
                    Err(e) => {
                        match self.index_store.delete(uuid).await {
                            Ok(Some(index)) => {
                                index.close();
                            }
                            Ok(None) => (),
                            Err(e) => log::error!("Error while deleting index: {:?}", e),
                        }
                        Err(e)
                    }
                    Ok(()) => Ok(index),
                }
            }
        }
    }

    /// Get or create an index with name `uid`.
    pub async fn get_or_create_index(&self, uid: IndexUid, task_id: TaskId) -> Result<Index> {
        match self.create_index(uid, task_id).await {
            Ok(index) => Ok(index),
            Err(IndexResolverError::IndexAlreadyExists(uid)) => self.get_index(uid).await,
            Err(e) => Err(e),
        }
    }

    pub async fn list(&self) -> Result<Vec<(String, Index)>> {
        let uuids = self.index_uuid_store.list().await?;
        let mut indexes = Vec::new();
        for (name, IndexMeta { uuid, .. }) in uuids {
            match self.index_store.get(uuid).await? {
                Some(index) => indexes.push((name, index)),
                None => {
                    // we found an unexisting index, we remove it from the uuid store
                    let _ = self.index_uuid_store.delete(name).await;
                }
            }
        }

        Ok(indexes)
    }

    pub async fn delete_index(&self, uid: String) -> Result<Index> {
        match self.index_uuid_store.delete(uid.clone()).await? {
            Some(IndexMeta { uuid, .. }) => match self.index_store.delete(uuid).await? {
                Some(index) => {
                    index.clone().close();
                    Ok(index)
                }
                None => Err(IndexResolverError::UnexistingIndex(uid)),
            },
            None => Err(IndexResolverError::UnexistingIndex(uid)),
        }
    }

    pub async fn get_index(&self, uid: String) -> Result<Index> {
        match self.index_uuid_store.get(uid).await? {
            (name, Some(IndexMeta { uuid, .. })) => {
                match self.index_store.get(uuid).await? {
                    Some(index) => Ok(index),
                    None => {
                        // For some reason we got a uuid to an unexisting index, we return an error,
                        // and remove the uuid from the uuid store.
                        let _ = self.index_uuid_store.delete(name.clone()).await;
                        Err(IndexResolverError::UnexistingIndex(name))
                    }
                }
            }
            (name, _) => Err(IndexResolverError::UnexistingIndex(name)),
        }
    }

    pub async fn get_index_creation_task_id(&self, index_uid: String) -> Result<TaskId> {
        let (uid, meta) = self.index_uuid_store.get(index_uid).await?;
        meta.map(
            |IndexMeta {
                 creation_task_id, ..
             }| creation_task_id,
        )
        .ok_or(IndexResolverError::UnexistingIndex(uid))
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use super::*;

    use futures::future::ok;
    use milli::update::{DocumentAdditionResult, IndexDocumentsMethod};
    use nelson::Mocker;
    use proptest::prelude::*;

    use crate::index::{
        error::{IndexError, Result as IndexResult},
        Checked, IndexMeta, IndexStats, Settings,
    };
    use index_store::MockIndexStore;
    use meta_store::MockIndexMetaStore;

    proptest! {
        #[test]
        fn test_process_task(
            task in any::<Task>(),
            index_exists in any::<bool>(),
            index_op_fails in any::<bool>(),
            any_int in any::<u64>(),
            ) {
            actix_rt::System::new().block_on(async move {
                let uuid = Uuid::new_v4();
                let mut index_store = MockIndexStore::new();

                let mocker = Mocker::default();

                // Return arbitrary data from index call.
                match &task.content {
                    TaskContent::DocumentAddition{primary_key, ..} => {
                        let result = move || if !index_op_fails {
                            Ok(DocumentAdditionResult { indexed_documents: any_int, number_of_documents: any_int })
                        } else {
                            // return this error because it's easy to generate...
                            Err(IndexError::DocumentNotFound("a doc".into()))
                        };
                        if primary_key.is_some() {
                            mocker.when::<String, IndexResult<IndexMeta>>("update_primary_key")
                                .then(move |_| Ok(IndexMeta{ created_at: Utc::now(), updated_at: Utc::now(), primary_key: None }));
                        }
                        mocker.when::<(IndexDocumentsMethod, Uuid, Option<String>, UpdateFileStore), IndexResult<DocumentAdditionResult>>("update_documents")
                                .then(move |(_, _, _, _)| result());
                    }
                    TaskContent::SettingsUpdate{..} => {
                        let result = move || if !index_op_fails {
                            Ok(())
                        } else {
                            // return this error because it's easy to generate...
                            Err(IndexError::DocumentNotFound("a doc".into()))
                        };
                        mocker.when::<&Settings<Checked>, IndexResult<()>>("update_settings")
                                .then(move |_| result());
                    }
                    TaskContent::DocumentDeletion(DocumentDeletion::Ids(_ids)) => {
                        let result = move || if !index_op_fails {
                            Ok(any_int as u64)
                        } else {
                            // return this error because it's easy to generate...
                            Err(IndexError::DocumentNotFound("a doc".into()))
                        };

                        mocker.when::<&[String], IndexResult<u64>>("delete_documents")
                                .then(move |_| result());
                    },
                    TaskContent::DocumentDeletion(DocumentDeletion::Clear) => {
                        let result = move || if !index_op_fails {
                            Ok(())
                        } else {
                            // return this error because it's easy to generate...
                            Err(IndexError::DocumentNotFound("a doc".into()))
                        };
                        mocker.when::<(), IndexResult<()>>("clear_documents")
                            .then(move |_| result());
                    },
                    TaskContent::IndexDeletion => {
                        mocker.when::<(), ()>("close")
                            .times(index_exists as usize)
                            .then(move |_| ());
                    }
                    TaskContent::IndexUpdate { primary_key }
                    | TaskContent::IndexCreation { primary_key } => {
                        if primary_key.is_some() {
                            let result = move || if !index_op_fails {
                                Ok(IndexMeta{ created_at: Utc::now(), updated_at: Utc::now(), primary_key: None })
                            } else {
                                // return this error because it's easy to generate...
                                Err(IndexError::DocumentNotFound("a doc".into()))
                            };
                            mocker.when::<String, IndexResult<IndexMeta>>("update_primary_key")
                                .then(move |_| result());
                            }
                    }
                }

                mocker.when::<(), IndexResult<IndexStats>>("stats")
            .then(|()| Ok(IndexStats { size: 0, number_of_documents: 0, is_indexing: Some(false), field_distribution: BTreeMap::new() }));

                let index = Index::mock(mocker);

                match &task.content {
                    // an unexisting index should trigger an index creation in the folllowing cases:
                    TaskContent::DocumentAddition { .. }
                    | TaskContent::SettingsUpdate { is_deletion: false, .. }
                    | TaskContent::IndexCreation { .. } if !index_exists => {
                        index_store
                            .expect_create()
                            .once()
                            .withf(move |&found| !index_exists || found == uuid)
                            .returning(move |_| Box::pin(ok(index.clone())));
                    },
                    TaskContent::IndexDeletion => {
                        index_store
                            .expect_delete()
                            // this is called only if the index.exists
                            .times(index_exists as usize)
                            .withf(move |&found| !index_exists || found == uuid)
                            .returning(move |_| Box::pin(ok(Some(index.clone()))));
                    }
                    // if index already exists, create index will return an error
                    TaskContent::IndexCreation { .. } if index_exists => (),
                    // The index exists and get should be called
                    _ if index_exists => {
                        index_store
                            .expect_get()
                            .once()
                            .withf(move |&found| found == uuid)
                            .returning(move |_| Box::pin(ok(Some(index.clone()))));
                    },
                    // the index doesn't exist and shouldn't be created, the uuidstore will return an error, and get_index will never be called.
                    _ => (),
                }

                let mut uuid_store = MockIndexMetaStore::new();
                uuid_store
                    .expect_get()
                    .returning(move |uid| {
                        Box::pin(ok((uid, index_exists.then(|| crate::index_resolver::meta_store::IndexMeta {uuid, creation_task_id: 0 }))))
                    });

                // we sould only be creating an index if the index doesn't alredy exist
                uuid_store
                    .expect_insert()
                    .withf(move |_, _| !index_exists)
                    .returning(|_, _| Box::pin(ok(())));

                uuid_store
                    .expect_delete()
                    .times(matches!(task.content, TaskContent::IndexDeletion) as usize)
                    .returning(move |_| Box::pin(ok(index_exists.then(|| crate::index_resolver::meta_store::IndexMeta { uuid, creation_task_id: 0}))));

                let mocker = Mocker::default();
                let update_file_store = UpdateFileStore::mock(mocker);
                let index_resolver = IndexResolver::new(uuid_store, index_store, update_file_store);

                let result = index_resolver.process_task(&task).await;

                // Test for some expected output scenarios:
                // Index creation and deletion cannot fail because of a failed index op, since they
                // don't perform index ops.
                if index_op_fails && !matches!(task.content, TaskContent::IndexDeletion | TaskContent::IndexCreation { primary_key: None } | TaskContent::IndexUpdate { primary_key: None })
                    || (index_exists && matches!(task.content, TaskContent::IndexCreation { .. }))
                    || (!index_exists && matches!(task.content, TaskContent::IndexDeletion
                                                                | TaskContent::DocumentDeletion(_)
                                                                | TaskContent::SettingsUpdate { is_deletion: true, ..}
                                                                | TaskContent::IndexUpdate { .. } ))
                {
                    assert!(result.is_err(), "{:?}", result);
                } else {
                    assert!(result.is_ok(), "{:?}", result);
                }
            });
        }
    }
}
