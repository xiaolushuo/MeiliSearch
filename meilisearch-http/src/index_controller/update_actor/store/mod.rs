mod codec;
pub mod dump;

use std::collections::VecDeque;
use std::fs::{copy, create_dir_all, remove_file, File};
use std::path::Path;
use std::sync::Arc;
use std::{
    collections::{BTreeMap, HashSet},
    path::PathBuf,
};

use futures::StreamExt;
use heed::types::{ByteSlice, OwnedType, SerdeJson, Unit};
use heed::zerocopy::U64;
use heed::{CompactionOption, Database, Env, EnvOpenOptions};
use tokio::select;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot};
use tokio::sync::mpsc::error::TrySendError;
use uuid::Uuid;

use codec::*;

use super::error::Result;
use super::UpdateMeta;
use crate::helpers::EnvSizer;
use crate::index_controller::{index_actor::CONCURRENT_INDEX_MSG, updates::*, IndexActorHandle};

#[allow(clippy::upper_case_acronyms)]
type BEU64 = U64<heed::byteorder::BE>;
type UpdateId = u64;
type GlobalUpdateId = u64;

const UPDATE_DIR: &str = "update_files";

pub struct UpdateStoreInfo {
    /// Size of the update store in bytes.
    pub size: u64,
    /// Uuid of the currently processing update if it exists
    pub processing: Option<Uuid>,
}

/// A data structure that allows concurrent reads AND exactly one writer.
#[allow(clippy::large_enum_variant)]
pub enum Status {
    Idle,
    Processing(Uuid, Processing),
    Snapshoting,
    Dumping,
}

#[derive(Debug)]
enum Task {
    DeleteIndex {
        ret: oneshot::Sender<()>,
        uuid: Uuid,
    },
    Update {
        processing: Processing,
        index_uuid: Uuid,
    },
    Snapshot,
    Dump,
}

enum SnapshotRequest {
    Dump,
    Snapshot,
}

pub type UpdateReceiver = mpsc::Receiver<(Uuid, UpdateMeta, Option<Uuid>, oneshot::Sender<Result<Enqueued>>)>;

/// The write queue is used to serialize write to the update store, and ensure the consistency of
/// the database.
pub struct WriteQueue {
    task_receiver: mpsc::Receiver<Task>,
    /// index uuid, update metadata, content
    update_receiver: UpdateReceiver,
    worker_receiver: mpsc::Receiver<WorkerMsg>,
    store: Arc<UpdateStore<RW>>,
    update_queue: VecDeque<UpdateId>,
    pending_update: usize,
    status: Status,
}

impl WriteQueue {
    pub fn new<I: IndexActorHandle + Sync + Send + 'static>(
        store: Arc<UpdateStore<RW>>,
        task_receiver: mpsc::Receiver<Task>,
        update_receiver: UpdateReceiver,
        indexes: I,
        ) -> Self {
        // TODO: Load the updates in the update store
        let (worker_sender, worker_receiver) = mpsc::channel(100);
        let worker = UpdateWorker::new(&store.path, worker_sender, indexes);
        tokio::spawn(worker.run());

        Self {
            task_receiver,
            update_receiver,
            worker_receiver,
            store,
            update_queue: VecDeque::new(),
            pending_update: 0,
            status: Status::Idle,
        }
    }

    /// Run the WriteQueue loop.
    ///
    /// The write queue acts as a scheduler for operation that need to be performed on the update
    /// store. It schedule write operation one at a time.
    ///
    /// The highest priority task is to get a job slot from the worker. Whenever the worker is
    /// ready for more work, it poll the WriteQueue and hands it channed where the WriteQueue can
    /// send a task.
    ///
    /// We then look for Snapshot, Dump, or index deletion tasks, since we want to run then ASAP,
    /// before any other update.
    ///
    /// We then look at if we have received any update registration request, if it is the case, we
    /// register it, and push it on the update queue,
    ///
    /// If we have nothing else to do, then we pop our update queue, and process the update.
    async fn run(mut self) {
        let mut task_sender = None;
        loop {
            select! {
                biased;

                Some(msg) = self.worker_receiver.recv() => {
                    match msg {
                        WorkerMsg::Ready(snd) => { task_sender.replace(snd); },
                        WorkerMsg::Processed(result, ret) => {
                            self.store.process(result).await.expect("Could not write update result disk.");
                            // Signal to the worker that the update was written correctly to the store.
                            ret.send(()).expect("Worker process exited unexpectedly");
                        },
                    }
                },
                // If a task is waiting, and we need to perform a snapshot, we do it with the
                // highest prority.
                Some(task) = self.task_receiver.recv(), if task_sender.is_some() => {
                    // OK, we just checked that it is Some.
                    let wait_notifier = task_sender.take().unwrap();
                    wait_notifier.send(task);
                },
                Some((uuid, update, content, ret)) = self.update_receiver.recv(), if task_sender.is_some() => {
                    match self.store.register_update(update, content, uuid) {
                        Ok((global_id, enqueued)) => {
                            self.update_queue.push_front(global_id);
                            let _ = ret.send(Ok(enqueued));
                            self.pending_update += 1;
                        }
                        Err(e) => {
                            let _ = ret.send(Err(e.into()));
                        }
                    };
                },
                // There is a pending update to process and the worker is available, we can process
                // it.
                _ = futures::future::ready(()), if self.pending_update > 0 && task_sender.is_some() => {
                    let wait_notifier = task_sender.take().unwrap();
                    if let Some((index_uuid, processing)) = self.store.process_first().unwrap() {
                        let task = Task::Update { processing, index_uuid };
                        wait_notifier.send(task).expect("Fatal error: Update worker exited unexpectedly.");
                        self.pending_update -= 1;
                    }
                },
                else => break,
            }
        }
    }
}

#[derive(Debug)]
enum WorkerMsg {
    Ready(oneshot::Sender<Task>),
    Processed(UpdateStatus, oneshot::Sender<()>),
}

struct UpdateWorker<I> {
    pub write_queue: mpsc::Sender<WorkerMsg>,
    /// A list of deleted indexes to filter out from the update to process
    pub path: PathBuf,
    pub indexes: I,
}

impl<I: IndexActorHandle> UpdateWorker<I> {
    pub fn new(path: impl AsRef<Path>, write_queue: mpsc::Sender<WorkerMsg>, indexes: I) -> Self {
        let path = path.as_ref().to_owned();
        Self {
            write_queue,
            path,
            indexes,
        }
    }

    /// Runs the UpdateWorker loop.
    ///
    /// The UpdateWorker job is to poll the WriteQueue for a task to accomplish. Whenever it is
    /// available for work, it sends a message to the WriteQueue, and wait for it to respond with a
    /// task.
    async fn run(self) {
        loop {
            let (sender, receiver) = oneshot::channel();
            self.write_queue.send(WorkerMsg::Ready(sender)).await.expect("Write queue exited.");
            match receiver.await {
                Ok(task) => {
                    match task {
                        Task::DeleteIndex { ret, uuid } => todo!(),
                        Task::Update { processing, index_uuid } => self.perform_update(index_uuid, processing).await,
                        Task::Snapshot => todo!(),
                        Task::Dump => todo!(),
                    };
                },
                // The receiver has been dropped, we try to poll the queue again.
                Err(_) => (),
            }
        }
    }

    async fn perform_update(&self, uuid: Uuid, update: Processing) {
        let content_path = update.from.content.map(|uuid| update_uuid_to_file_path(&self.path, uuid));
        let update_id = update.id();

        let file = match content_path {
            Some(ref path) => {
                let file = File::open(path).unwrap();
                Some(file)
            }
            None => None,
        };

        // Process the pending update using the provided user function.
        let result = match self.indexes.update(uuid, update.clone(), file).await {
                Ok(result) => result,
                Err(e) => Err(update.fail(e.into())),
        };

        // Once the pending update have been successfully processed
        // we must remove the content from the pending and processing stores and
        // write the *new* meta to the processed-meta store and commit.
        let result = match result {
            Ok(res) => res.into(),
            Err(res) => res.into(),
        };

        // We send the result to the write loop to the result is recorded, and wait for
        // acknowledgement before the worker can go into a ready state again.
        let (sender, receiver) = oneshot::channel();
        let msg = WorkerMsg::Processed(uuid, result, sender);
        self.write_queue.send(msg).await.expect("write loop exited");

        receiver.await.expect("write loop exited");
    }
}

#[derive(Clone)]
pub struct UpdateStore<Mode> {
    pub env: Env,
    /// A queue containing the updates to process, ordered by arrival.
    /// The key are built as follow:
    /// | global_update_id | index_uuid | update_id |
    /// |     8-bytes      |  16-bytes  |  8-bytes  |
    pending_queue: Database<PendingKeyCodec, SerdeJson<Enqueued>>,
    /// Map indexes to the next available update id. If NextIdKey::Global is queried, then the next
    /// global update id is returned
    next_update_id: Database<NextIdCodec, OwnedType<BEU64>>,
    /// Contains all the performed updates meta, be they failed, aborted, or processed.
    /// The keys are built as follow:
    /// |    Uuid  |   id    |
    /// | 16-bytes | 8-bytes |
    processed: Database<UpdateKeyCodec, SerdeJson<UpdateStatus>>,
    /// Holds the currenlty processing update metadata.
    processing: Database<Unit, SerdeJson<(Uuid, Processing)>>,
    /// Path of the database.
    path: PathBuf,
    mode: std::marker::PhantomData<Mode>,
}

pub struct RO;
pub struct RW;

trait Readable {}
trait Writable {}

impl Readable for RO {}
impl Readable for RW {}

impl Writable for RW {}

impl<T> UpdateStore<T> {
    fn new(
        mut options: EnvOpenOptions,
        path: impl AsRef<Path>,
    ) -> anyhow::Result<(UpdateStore<RW>, UpdateStore<RO>)> {
        options.max_dbs(7);

        let env = options.open(&path)?;
        let pending_queue = env.create_database(Some("pending-queue"))?;
        let next_update_id = env.create_database(Some("next-update-id"))?;
        let updates = env.create_database(Some("processed"))?;
        let processing = env.create_database(Some("processing"))?;

        let (notification_sender, notification_receiver) = mpsc::channel(1);

        let writer = Self {
                env,
                pending_queue,
                next_update_id,
                processed: updates,
                processing,
                path: path.as_ref().to_owned(),
                mode: std::marker::PhantomData,
            };

        let reader = writer.get_reader();

        Ok((writer, reader))
    }
}

impl<Mode: Readable> UpdateStore<Mode> {
    async fn update_status(&self, status: UpdateStatus, global_id: UpdateId) -> Result<()> {
        todo!()
        //let store = self.store.clone();
        //let path = self.path.clone();
        //tokio::task::spawn_blocking(move || {
            //// TODO: ignore when the enqueued update doesn't exist. (i.e: deleted index)
            //let mut txn = store.env.write_txn()?;
            //store.delete_pending(&mut txn, global_id)?;
            //store.put_status(&mut txn, &status)?;
            //txn.commit()?;
            //// remove update file if it exists
            //if let Some(content) = status.content() {
                //// ignore error
                //let path = update_uuid_to_file_path(&path, content);
                //let _ = remove_file(path);
            //}
            //Ok(())
        //}).await?
    }

    /// List the updates for `index_uuid`.
    pub fn list(&self, index_uuid: Uuid) -> Result<Vec<UpdateStatus>> {
        let mut update_list = BTreeMap::<u64, UpdateStatus>::new();

        let txn = self.env.read_txn()?;

        let pendings = self.pending_queue.iter(&txn)?.lazily_decode_data();
        for entry in pendings {
            let ((_, uuid, id), pending) = entry?;
            if uuid == index_uuid {
                update_list.insert(id, pending.decode()?.into());
            }
        }

        let updates = self
            .processed
            .remap_key_type::<ByteSlice>()
            .prefix_iter(&txn, index_uuid.as_bytes())?;

        for entry in updates {
            let (_, update) = entry?;
            update_list.insert(update.id(), update);
        }

        // If the currently processing update is from this index, replace the corresponding pending update with this one.
        match *self.state.read() {
            Status::Processing(uuid, ref processing) if uuid == index_uuid => {
                update_list.insert(processing.id(), processing.clone().into());
            }
            _ => (),
        }

        Ok(update_list.into_iter().map(|(_, v)| v).collect())
    }

    /// Returns the update associated meta or `None` if the update doesn't exist.
    pub fn meta(&self, index_uuid: Uuid, update_id: u64) -> heed::Result<Option<UpdateStatus>> {
        // Check if the update is the one currently processing
        match *self.state.read() {
            Status::Processing(uuid, ref processing)
                if uuid == index_uuid && processing.id() == update_id =>
            {
                return Ok(Some(processing.clone().into()));
            }
            _ => (),
        }

        let txn = self.env.read_txn()?;
        // Else, check if it is in the updates database:
        let update = self.processed.get(&txn, &(index_uuid, update_id))?;

        if let Some(update) = update {
            return Ok(Some(update));
        }

        // If nothing was found yet, we resolve to iterate over the pending queue.
        let pendings = self.pending_queue.iter(&txn)?.lazily_decode_data();

        for entry in pendings {
            let ((_, uuid, id), pending) = entry?;
            if uuid == index_uuid && id == update_id {
                return Ok(Some(pending.decode()?.into()));
            }
        }

        // No update was found.
        Ok(None)
    }

    pub fn get_info(&self) -> Result<UpdateStoreInfo> {
        let mut size = self.env.size();
        let txn = self.env.read_txn()?;
        for entry in self.pending_queue.iter(&txn)? {
            let (_, pending) = entry?;
            if let Enqueued {
                content: Some(uuid),
                ..
            } = pending
            {
                let path = update_uuid_to_file_path(&self.path, uuid);
                size += File::open(path)?.metadata()?.len();
            }
        }
        let processing = match *self.state.read() {
            Status::Processing(uuid, _) => Some(uuid),
            _ => None,
        };

        Ok(UpdateStoreInfo { size, processing })
    }

}

impl<Mode: Writable + Readable> UpdateStore<Mode> {
    /// Gets a readable UpdateStore out of a writable UpdateStore.
    fn get_reader(&self) -> UpdateStore<RO> {
        UpdateStore {
            env: self.env.clone(),
            pending_queue: self.pending_queue.clone(),
            next_update_id: self.next_update_id.clone(),
            processed: self.processed.clone(),
            processing: self.processing.clone(),
            path: self.path.clone(),
            mode: std::marker::PhantomData,
        }
    }

    /// Moves the update from processing  to the processed store.
    ///
    /// Errors if there are no currently processing updates.
    async fn process(&self, update: UpdateStatus) -> Result<()> {
        let mut txn = self.env.write_txn()?;
        let id = update.id();
        // TODO: check that the processing update is the same as the one we are asked to process.
        match self.processing.get(&txn, &())? {
            Some((index_uuid, processing)) => {
                debug_assert_eq!(processing.id(), update.id());
                self.processing.delete(&mut txn, &())?;
                self.processed.put(&mut txn, &(index_uuid, id), &update)?;
                // Remove associated content if it exists.
                if let Some(uuid) = update.content() {
                    let path = update_uuid_to_file_path(&self.path, uuid);
                    remove_file(path)?;
                }

                txn.commit()?;

                Ok(())
            }
            None => todo!("No processing updates error"),
        }
    }

    /// Turn the given global id into the current processing update. Fails if there is already a
    /// processing update
    fn process_first(&self) -> Result<Option<(Uuid, Processing)>> {
        let mut txn = self.env.write_txn()?;

        if self.processing.get(&txn, &())?.is_some() {
            todo!("tryng to process an update while an update is still processing");
        }

        match self.pending_queue.first(&txn)? {
            Some((key @ (_, uuid, _), update)) => {
                let update = update.processing();
                self.processing.put(&mut txn, &(), &(uuid, update))?;
                self.pending_queue.delete(&mut txn, &key)?;
                txn.commit()?;
                Ok(Some((uuid, update)))
            }
            None => Ok(None)
        }
    }

    fn delete_pending(&self, txn: &mut heed::RwTxn, id: UpdateId) -> Result<()> {
        todo!()
    }

    fn put_status(&self, txn: &mut heed::RwTxn, status: &UpdateStatus) -> Result<()> {
        todo!()
    }

    fn register_pending(&self, index_uuid: Uuid, update: Enqueued) -> Result<UpdateId> {
        todo!()
    }

    /// Returns the next global update id and the next update id for a given `index_uuid`.
    fn next_update_id(&self, txn: &mut heed::RwTxn, index_uuid: Uuid) -> heed::Result<(GlobalUpdateId, UpdateId)> {
        let global_id = self
            .next_update_id
            .get(txn, &NextIdKey::Global)?
            .map(U64::get)
            .unwrap_or_default();

        self.next_update_id
            .put(txn, &NextIdKey::Global, &BEU64::new(global_id + 1))?;

        let update_id = self.next_update_id_raw(txn, index_uuid)?;

        Ok((global_id, update_id))
    }

    /// Returns the next next update id for a given `index_uuid` without
    /// incrementing the global update id. This is useful for the dumps.
    fn next_update_id_raw(&self, txn: &mut heed::RwTxn, index_uuid: Uuid) -> heed::Result<u64> {
        let update_id = self
            .next_update_id
            .get(txn, &NextIdKey::Index(index_uuid))?
            .map(U64::get)
            .unwrap_or_default();

        self.next_update_id.put(
            txn,
            &NextIdKey::Index(index_uuid),
            &BEU64::new(update_id + 1),
        )?;

        Ok(update_id)
    }

    /// Registers the update content in the pending store and the meta
    /// into the pending-meta store. Returns the new unique update id.
    pub fn register_update(
        &self,
        meta: UpdateMeta,
        content: Option<Uuid>,
        index_uuid: Uuid,
    ) -> heed::Result<(GlobalUpdateId, Enqueued)> {
        let mut txn = self.env.write_txn()?;

        let (global_id, update_id) = self.next_update_id(&mut txn, index_uuid)?;
        let meta = Enqueued::new(meta, update_id, content);

        self.pending_queue
            .put(&mut txn, &(global_id, index_uuid, update_id), &meta)?;

        txn.commit()?;

        if let Err(TrySendError::Closed(())) = self.notification_sender.try_send(()) {
            panic!("Update store loop exited");
        }

        Ok((global_id, meta))
    }

    /// Push already processed update in the UpdateStore without triggering the notification
    /// process. This is useful for the dumps.
    pub fn register_raw_updates(
        &self,
        wtxn: &mut heed::RwTxn,
        update: &UpdateStatus,
        index_uuid: Uuid,
    ) -> heed::Result<()> {
        match update {
            UpdateStatus::Enqueued(enqueued) => {
                let (global_id, _update_id) = self.next_update_id(wtxn, index_uuid)?;
                self.pending_queue.remap_key_type::<PendingKeyCodec>().put(
                    wtxn,
                    &(global_id, index_uuid, enqueued.id()),
                    enqueued,
                )?;
            }
            _ => {
                let _update_id = self.next_update_id_raw(wtxn, index_uuid)?;
                self.processed.put(wtxn, &(index_uuid, update.id()), update)?;
            }
        }
        Ok(())
    }

    /// Delete all updates for an index from the update store. If the currently processing update
    /// is for `index_uuid`, the call will block until the update is terminated.
    pub fn delete_all(&self, index_uuid: Uuid) -> Result<()> {
        let mut txn = self.env.write_txn()?;
        let _lock = self.state.write();
        // Contains all the content file paths that we need to be removed if the deletion was successful.
        let mut uuids_to_remove = Vec::new();

        let mut pendings = self.pending_queue.iter_mut(&mut txn)?.lazily_decode_data();

        while let Some(Ok(((_, uuid, _), pending))) = pendings.next() {
            if uuid == index_uuid {
                let mut pending = pending.decode()?;
                if let Some(update_uuid) = pending.content.take() {
                    uuids_to_remove.push(update_uuid);
                }

                // Invariant check: we can only delete the current entry when we don't hold
                // references to it anymore. This must be done after we have retrieved its content.
                unsafe {
                    pendings.del_current()?;
                }
            }
        }

        drop(pendings);

        let mut updates = self
            .processed
            .remap_key_type::<ByteSlice>()
            .prefix_iter_mut(&mut txn, index_uuid.as_bytes())?
            .lazily_decode_data();

        while let Some(_) = updates.next() {
            unsafe {
                updates.del_current()?;
            }
        }

        drop(updates);

        txn.commit()?;

        // If the currently processing update is from our index, we wait until it is
        // finished before returning. This ensure that no write to the index occurs after we delete it.
        if let Status::Processing(uuid, _) = *self.state.read() {
            if uuid == index_uuid {
                // wait for a write lock, do nothing with it.
                self.state.write();
            }
        }

        // Finally, remove any outstanding update files. This must be done after waiting for the
        // last update to ensure that the update files are not deleted before the update needs
        // them.
        uuids_to_remove
            .iter()
            .map(|uuid| update_uuid_to_file_path(&self.path, *uuid))
            .for_each(|path| {
                let _ = remove_file(path);
            });

        Ok(())
    }

    pub fn snapshot(
        &self,
        uuids: &HashSet<Uuid>,
        path: impl AsRef<Path>,
        handle: impl IndexActorHandle + Clone,
    ) -> Result<()> {
        let state_lock = self.state.write();
        state_lock.swap(Status::Snapshoting);

        let txn = self.env.write_txn()?;

        let update_path = path.as_ref().join("updates");
        create_dir_all(&update_path)?;

        // acquire write lock to prevent further writes during snapshot
        create_dir_all(&update_path)?;
        let db_path = update_path.join("data.mdb");

        // create db snapshot
        self.env.copy_to_path(&db_path, CompactionOption::Enabled)?;

        let update_files_path = update_path.join(UPDATE_DIR);
        create_dir_all(&update_files_path)?;

        let pendings = self.pending_queue.iter(&txn)?.lazily_decode_data();

        for entry in pendings {
            let ((_, uuid, _), pending) = entry?;
            if uuids.contains(&uuid) {
                if let Enqueued {
                    content: Some(uuid),
                    ..
                } = pending.decode()?
                {
                    let path = update_uuid_to_file_path(&self.path, uuid);
                    copy(path, &update_files_path)?;
                }
            }
        }

        let path = &path.as_ref().to_path_buf();
        let handle = &handle;
        // Perform the snapshot of each index concurently. Only a third of the capabilities of
        // the index actor at a time not to put too much pressure on the index actor
        let mut stream = futures::stream::iter(uuids.iter())
            .map(move |uuid| handle.snapshot(*uuid, path.clone()))
            .buffer_unordered(CONCURRENT_INDEX_MSG / 3);

        Handle::current().block_on(async {
            while let Some(res) = stream.next().await {
                res?;
            }
            Ok(()) as Result<()>
        })?;

        Ok(())
    }
}

fn update_uuid_to_file_path(root: impl AsRef<Path>, uuid: Uuid) -> PathBuf {
    root.as_ref()
        .join(UPDATE_DIR)
        .join(format!("update_{}", uuid))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::index_controller::{
        index_actor::{error::IndexActorError, MockIndexActorHandle},
        UpdateResult,
    };

    use futures::future::ok;

    #[actix_rt::test]
    async fn test_next_id() {
        let dir = tempfile::tempdir_in(".").unwrap();
        let mut options = EnvOpenOptions::new();
        let handle = Arc::new(MockIndexActorHandle::new());
        options.map_size(4096 * 100);
        let update_store = UpdateStore::open(
            options,
            dir.path(),
            handle,
            Arc::new(AtomicBool::new(false)),
        )
        .unwrap();

        let index1_uuid = Uuid::new_v4();
        let index2_uuid = Uuid::new_v4();

        let mut txn = update_store.env.write_txn().unwrap();
        let ids = update_store.next_update_id(&mut txn, index1_uuid).unwrap();
        txn.commit().unwrap();
        assert_eq!((0, 0), ids);

        let mut txn = update_store.env.write_txn().unwrap();
        let ids = update_store.next_update_id(&mut txn, index2_uuid).unwrap();
        txn.commit().unwrap();
        assert_eq!((1, 0), ids);

        let mut txn = update_store.env.write_txn().unwrap();
        let ids = update_store.next_update_id(&mut txn, index1_uuid).unwrap();
        txn.commit().unwrap();
        assert_eq!((2, 1), ids);
    }

    #[actix_rt::test]
    async fn test_register_update() {
        let dir = tempfile::tempdir_in(".").unwrap();
        let mut options = EnvOpenOptions::new();
        let handle = Arc::new(MockIndexActorHandle::new());
        options.map_size(4096 * 100);
        let update_store = UpdateStore::open(
            options,
            dir.path(),
            handle,
            Arc::new(AtomicBool::new(false)),
        )
        .unwrap();
        let meta = UpdateMeta::ClearDocuments;
        let uuid = Uuid::new_v4();
        let store_clone = update_store.clone();
        tokio::task::spawn_blocking(move || {
            store_clone.register_update(meta, None, uuid).unwrap();
        })
        .await
        .unwrap();

        let txn = update_store.env.read_txn().unwrap();
        assert!(update_store
            .pending_queue
            .get(&txn, &(0, uuid, 0))
            .unwrap()
            .is_some());
    }

    #[actix_rt::test]
    async fn test_process_update() {
        let dir = tempfile::tempdir_in(".").unwrap();
        let mut handle = MockIndexActorHandle::new();

        handle
            .expect_update()
            .times(2)
            .returning(|_index_uuid, processing, _file| {
                if processing.id() == 0 {
                    Box::pin(ok(Ok(processing.process(UpdateResult::Other))))
                } else {
                    Box::pin(ok(Err(
                        processing.fail(IndexActorError::ExistingPrimaryKey.into())
                    )))
                }
            });

        let handle = Arc::new(handle);

        let mut options = EnvOpenOptions::new();
        options.map_size(4096 * 100);
        let store = UpdateStore::open(
            options,
            dir.path(),
            handle.clone(),
            Arc::new(AtomicBool::new(false)),
        )
        .unwrap();

        // wait a bit for the event loop exit.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let mut txn = store.env.write_txn().unwrap();

        let update = Enqueued::new(UpdateMeta::ClearDocuments, 0, None);
        let uuid = Uuid::new_v4();

        store
            .pending_queue
            .put(&mut txn, &(0, uuid, 0), &update)
            .unwrap();

        let update = Enqueued::new(UpdateMeta::ClearDocuments, 1, None);

        store
            .pending_queue
            .put(&mut txn, &(1, uuid, 1), &update)
            .unwrap();

        txn.commit().unwrap();

        // Process the pending, and check that it has been moved to the update databases, and
        // removed from the pending database.
        let store_clone = store.clone();
        tokio::task::spawn_blocking(move || {
            store_clone.process_pending_update(handle.clone()).unwrap();
            store_clone.process_pending_update(handle).unwrap();
        })
        .await
        .unwrap();

        let txn = store.env.read_txn().unwrap();

        assert!(store.pending_queue.first(&txn).unwrap().is_none());
        let update = store.processed.get(&txn, &(uuid, 0)).unwrap().unwrap();

        assert!(matches!(update, UpdateStatus::Processed(_)));
        let update = store.processed.get(&txn, &(uuid, 1)).unwrap().unwrap();

        assert!(matches!(update, UpdateStatus::Failed(_)));
    }
}
