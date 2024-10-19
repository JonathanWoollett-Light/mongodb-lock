use std::time::{Duration, Instant};

use bson::doc;
use bson::oid::ObjectId;
use bson::{Bson, Document};
use displaydoc::Display;
use mongodb::{
    options::IndexOptions,
    results::{DeleteResult, InsertOneResult},
    Collection, IndexModel,
};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::iter::once;
use std::sync::LazyLock;
use thiserror::Error;
use tokio::runtime::Handle;
use tokio::sync::Mutex as TokioMutex;
use tokio::task;
use tokio::time::sleep;

/// Error type for [`Mutex::new`].
#[derive(Debug, Error, Display)]
pub enum NewMutexError {
    /// Failed to acquire lock due to timeout.
    LockTimeout,
    /// Failed to get [`ObjectId`] from [`InsertOneResult::inserted_id`].
    ObjectId,
    /// Failed attempt to acquire lock: {0}
    Attempt(mongodb::error::Error),
    /// Failed to create index: {0}
    CreateIndex(mongodb::error::Error),
    /// Failed to serialize to bson: {0}
    ToBson(bson::ser::Error),
}

/// Error type for [`Mutex::release`].
#[derive(Debug, Error, Display)]
enum ReleaseError {
    /// Failed to start deleting the lock: {0}
    PreDelete(mongodb::error::Error),
    /// Failed to finish deleting the lock.
    PostDelete,
}

#[derive(Debug)]
pub struct Mutex<Key: Clone + Send + Sync + Serialize + 'static> {
    pub collection: Collection<MutexDocument<Key>>,
    pub lock: ObjectId,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MutexDocument<Key> {
    /// Lock id
    pub _id: ObjectId,
    pub key: Key,
}

impl<Key: Clone + Send + Sync + Serialize + 'static> Mutex<Key> {
    pub async fn new_default(
        collection: Collection<MutexDocument<Key>>,
        key: Key,
    ) -> Result<Self, NewMutexError> {
        Self::new(
            collection,
            Duration::from_secs(60),
            Duration::from_secs(1),
            key,
        )
        .await
    }
    pub async fn new(
        collection: Collection<MutexDocument<Key>>,
        timeout: Duration,
        wait: Duration,
        key: Key,
    ) -> Result<Self, NewMutexError> {
        static COLLECTIONS: LazyLock<TokioMutex<HashSet<String>>> =
            LazyLock::new(|| TokioMutex::new(HashSet::new()));
        if COLLECTIONS
            .lock()
            .await
            .insert(collection.name().to_string())
        {
            collection
                .create_index(
                    IndexModel::builder()
                        .keys(once((String::from("key"), Bson::Int32(1))).collect::<Document>())
                        .options(IndexOptions::builder().unique(true).build())
                        .build(),
                )
                .await
                .map_err(NewMutexError::CreateIndex)?;
        }

        let lock_id = ObjectId::new();
        let lock_doc = MutexDocument {
            _id: lock_id,
            key: key.clone(),
        };

        let start = Instant::now();
        loop {
            if start.elapsed() > timeout {
                return Err(NewMutexError::LockTimeout);
            }
            let insert = collection.insert_one(&lock_doc).await;
            match insert {
                Ok(InsertOneResult { inserted_id, .. }) => {
                    let lock = inserted_id.as_object_id().ok_or(NewMutexError::ObjectId)?;
                    debug_assert_eq!(lock, lock_id);
                    break Ok(Self { collection, lock });
                }
                // Wait to retry acquiring the lock.
                Err(err) if is_duplicate_key_error(&err) => sleep(wait).await,
                Err(err) => break Err(NewMutexError::Attempt(err)),
            }
        }
    }
    async fn release(
        collection: Collection<MutexDocument<Key>>,
        lock: ObjectId,
    ) -> Result<(), ReleaseError> {
        let delete = collection
            .delete_one(doc! { "_id": lock })
            .await
            .map_err(ReleaseError::PreDelete)?;
        if !matches!(
            delete,
            DeleteResult {
                deleted_count: 1,
                ..
            }
        ) {
            return Err(ReleaseError::PostDelete);
        }
        Ok(())
    }
}

// TODO Remove below `expect`.
#[expect(
    clippy::unwrap_used,
    reason = "I do not know a way to propagate the error."
)]
impl<Key: Clone + Send + Sync + Serialize + 'static> Drop for Mutex<Key> {
    fn drop(&mut self) {
        let handle = Handle::current();
        let lock = self.lock;
        let collection = self.collection.clone();
        task::spawn_blocking(move || {
            handle
                .block_on(async { Self::release(collection, lock).await })
                .unwrap();
        });
    }
}

pub fn is_duplicate_key_error(error: &mongodb::error::Error) -> bool {
    if let mongodb::error::ErrorKind::Write(mongodb::error::WriteFailure::WriteError(write_error)) =
        &*error.kind
    {
        write_error.code == 11000 && write_error.message.contains("duplicate key error")
    } else {
        false
    }
}
