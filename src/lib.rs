//! Rusty distributed locking backed by Mongodb.
//!
//! All [`Mutex`]s can share the same collection (even with different `Key`s) so long as all the
//! `Key`s in the collection are unique. I would recommend using different collections for different
//! `Key`s and different collections for each type of operation.
//!
//! All [`RwLock`]s can share the same collection. I would recommend using the same collection.
//!     
//! ## Similar works
//!
//! - <https://github.com/square/mongo-lock>
//!
//! ## Mutex
//!
//! This approach supports the simple case of enforcing mutual exclusion on a single docoument e.g.
//! ```ignore
//! let lock = Mutex::new("my_database", "lock_collection", ["id"]);
//! let guard = lock.lock_default(doc! { "id": my_document_id }).await?;
//! ```
//! While also supporting the more complex case where operations require mutual exclusion over
//! multiple documents e.g.
//! ```ignore
//! use std::cmp;
//! let lock = Mutex::new("my_database", "lock_collection", ["min","max"]);
//! let get_doc = |x,y| {
//!     match (x,y) {
//!         (None, Some(b)) => doc! { "min": b, "max": b },
//!         (Some(a), None) => doc! { "min": a, "max": a },
//!         (Some(a), Some(b)) => doc! { "min": cmp::min(a,b), "max": cmp::max(a,b) }
//!         (None, None) => doc! {}
//!     }
//! };
//!
//! // Both of these guards can be held at the same time.
//! let guard_one = lock.lock_default(get_doc(Some(id_one),None)).await?;
//! let guard_two = lock.lock_default(get_doc(Some(id_two),None)).await?;
//!
//! // None of these guards can be held at the same time.
//! // `guard_three` conflicts on `id_one`.
//! // `guard_four` and `guard_six` conflict on `id_one` and `id_two`.
//! let guard_three = lock.lock_default(get_doc(Some(id_one),None)).await?;
//! let guard_four = lock.lock_default(get_doc(Some(id_two),Some(id_one))).await?;
//! let guard_six = lock.lock_default(get_doc(Some(id_one), Some(id_two))).await?;
//! ```
//! The use-case is where you have operations which require exclusive access to a single user (e.g.
//! deleting a user) and operations which require exclusive access to multiple users (e.g. sending a
//! message between users).

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
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::runtime::Handle;
use tokio::task;
use tokio::time::sleep;

/// The default timeout used by [`Mutex::lock_default`], [`RwLock::read_default`] and
/// [`RwLock::write_default`].
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(60);
/// The default wait used by [`Mutex::lock_default`], [`RwLock::read_default`] and
/// [`RwLock::write_default`].
pub const DEFAULT_WAIT: Duration = Duration::from_millis(500);

/// Error type for [`Mutex::new`].
#[derive(Debug, Error, Display)]
pub enum MutexLockError {
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
    /// A key ({0}) was present in the document given to [`Mutex::lock`] that was not present when the lock was initialized.
    InvalidKey(String),
}

/// Error type for [`Mutex::release`].
#[derive(Debug, Error, Display)]
enum ReleaseError {
    /// Failed to start deleting the lock: {0}
    PreDelete(mongodb::error::Error),
    /// Failed to finish deleting the lock.
    PostDelete,
}

/// A distributed lock guard that acts like [`std::sync::MutexGuard`].
#[derive(Debug)]
pub struct MutexGuard<'a> {
    pub lock: &'a Mutex,
    pub id: ObjectId,
    pub rt: Handle,
}

/// The document used for backing [`Mutex`].
#[derive(Debug, Serialize, Deserialize)]
struct MutexDocument {
    /// Lock id
    pub _id: ObjectId,
    /// Key used for locking.
    pub key: Document,
}

/// A distributed lock that acts like [`std::sync::Mutex`].
#[derive(Debug)]
pub struct Mutex {
    keys: HashSet<String>,
    collection: Collection<MutexDocument>,
}

impl Mutex {
    /// Returns the keys that documents given to [`Mutex::lock`] or
    /// [`Mutex::lock_default`] can contain.
    pub fn keys(&self) -> &HashSet<String> {
        &self.keys
    }
    /// Constructs a new [`Mutex`].
    ///
    /// # Errors
    ///
    /// When [`mongodb::Collection::create_index`] errors.
    #[inline]
    pub async fn new(
        database: &mongodb::Database,
        collection: &str,
        keys: impl IntoIterator<Item = &str>,
    ) -> Result<Self, mongodb::error::Error> {
        let col = database.collection::<MutexDocument>(collection);
        let mut held_keys = HashSet::new();
        for key in keys {
            col.create_index(
                IndexModel::builder()
                    .keys(once((format!("key.{key}"), Bson::Int32(1))).collect::<Document>())
                    .options(IndexOptions::builder().unique(true).build())
                    .build(),
            )
            .await?;
            held_keys.insert(String::from(key));
        }

        Ok(Self {
            collection: col,
            keys: held_keys,
        })
    }
    /// Create [`Mutex`] without initializing the lock.
    ///
    /// This should be used when the lock is already initialized; possibly by another process.
    #[inline]
    pub async fn new_uninit(
        database: &mongodb::Database,
        collection: &str,
        keys: impl IntoIterator<Item = &str>,
    ) -> Self {
        let col = database.collection::<MutexDocument>(collection);
        let held_keys = keys.into_iter().map(String::from).collect();
        Self {
            collection: col,
            keys: held_keys,
        }
    }
    /// Calls [`Mutex::lock`] with [`DEFAULT_TIMEOUT`] and [`DEFAULT_WAIT`].
    /// # Errors
    ///
    /// When [`Mutex::lock`] errors.
    #[inline]
    pub async fn lock_default(&self, document: Document) -> Result<MutexGuard<'_>, MutexLockError> {
        self.lock(DEFAULT_TIMEOUT, DEFAULT_WAIT, document).await
    }
    /// Ensures mutaul exclusion across
    ///
    /// The guard cannot be acquired if any fields in a given [`bson::Document`] match any fields in
    /// any [`bson::Document`]s of any held lock.
    ///
    /// Since the Mongodb Rust driver doesn't fully support change streams see
    /// <https://github.com/mongodb/mongo-rust-driver/issues/1230> a busy polling approach is used
    /// where it will attempt to acquire the lock for `timeout` sleeping `wait` in between attempts.
    ///
    /// In this sense it is like:
    /// ```
    /// # use std::time::Duration;
    /// # use std::time::Instant;
    /// # fn main() -> Result<(),()> {
    /// # let rt = tokio::runtime::Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// let lock = tokio::sync::Mutex::new(());
    /// let timeout = Duration::from_secs(1);
    /// let sleep = Duration::from_millis(100);
    /// let start = Instant::now();
    /// let guard = loop {
    ///     match lock.try_lock() {
    ///         Ok(guard) => break guard,
    ///         Err(err) if start.elapsed() > timeout => return Err(()),
    ///         Err(_) => tokio::time::sleep(sleep).await,
    ///     }
    /// };
    /// // Do some work.
    /// # Ok(())
    /// # })
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// When:
    /// - Timing out.
    /// - [`mongodb::Collection::insert_one`] errors.
    /// - There are keys present in `document` that where not given to [`Mutex::new`] or [`Mutex::new_uninit`].
    #[inline]
    pub async fn lock(
        &self,
        timeout: Duration,
        wait: Duration,
        document: Document,
    ) -> Result<MutexGuard<'_>, MutexLockError> {
        for key in document.keys() {
            if !self.keys.contains(key) {
                return Err(MutexLockError::InvalidKey(key.clone()));
            }
        }

        let lock_id = ObjectId::new();
        let lock_doc = MutexDocument {
            _id: lock_id,
            key: document,
        };
        let start = Instant::now();
        loop {
            if start.elapsed() > timeout {
                return Err(MutexLockError::LockTimeout);
            }
            let insert = self.collection.insert_one(&lock_doc).await;
            match insert {
                Ok(InsertOneResult { inserted_id, .. }) => {
                    let id = inserted_id.as_object_id().ok_or(MutexLockError::ObjectId)?;
                    debug_assert_eq!(id, lock_id, "Document id mismatch");
                    break Ok(MutexGuard {
                        lock: self,
                        id,
                        rt: Handle::current(),
                    });
                }
                // Wait to retry acquiring the lock.
                Err(err) if is_duplicate_key_error(&err) => sleep(wait).await,
                Err(err) => break Err(MutexLockError::Attempt(err)),
            }
        }
    }
    /// Release the lock.
    async fn release(
        collection: Collection<MutexDocument>,
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
impl Drop for MutexGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        let rt = self.rt.clone();
        let id = self.id;
        let collection = self.lock.collection.clone();
        task::spawn_blocking(move || {
            rt.block_on(async { Mutex::release(collection, id).await })
                .unwrap();
        });
    }
}

/// Check if the error is a duplicate key error.
#[must_use]
#[inline]
pub fn is_duplicate_key_error(error: &mongodb::error::Error) -> bool {
    if let mongodb::error::ErrorKind::Write(mongodb::error::WriteFailure::WriteError(write_error)) =
        &*error.kind
    {
        write_error.code == 11000 && write_error.message.contains("duplicate key error")
    } else {
        false
    }
}

/// Error type for [`RwLock::read`].
#[derive(Debug, Error, Display)]
pub enum RwLockReadError {
    /// Failed to query lock: {0}
    Query(mongodb::error::Error),
    /// Failed to acquire lock due to timeout.
    Timeout,
}

/// Error type for [`RwLock::release_read`].
#[derive(Debug, Error, Display)]
enum RwLockReleaseReadError {
    /// Failed to query lock: {0}
    Query(mongodb::error::Error),
    /// Failed to find lock.
    Find,
}

/// Error type for [`RwLock::write`].
#[derive(Debug, Error, Display)]
pub enum RwLockWriteError {
    /// Failed to query lock: {0}
    Query(mongodb::error::Error),
    /// Failed to acquire lock due to timeout.
    Timeout,
}

/// Error type for [`RwLock::release_write`].
#[derive(Debug, Error, Display)]
enum RwLockReleaseWriteError {
    /// Failed to query lock: {0}
    Query(mongodb::error::Error),
    /// Failed to find lock.
    Find,
}

/// A distributed lock that acts like [`std::sync::RwLock`].
pub struct RwLock {
    /// The id of the lock document within the collection.
    id: ObjectId,
    /// The collection within which the lock document is stored.
    collection: Collection<RwLockDocument>,
}
impl RwLock {
    /// Returns the [`ObjectId`] of the underlying lock document stored in the collection.
    ///
    /// Intended for usage with [`RwLock::new_uninit`].
    pub fn id(&self) -> ObjectId {
        self.id
    }
    /// Constructs a new [`RwLock`].
    ///
    /// # Errors
    ///
    /// When [`mongodb::Collection::insert_one`] errors.
    #[inline]
    pub async fn new(
        database: &mongodb::Database,
        collection: &str,
    ) -> Result<Self, mongodb::error::Error> {
        let col = database.collection(collection);
        let id = ObjectId::new();
        col.insert_one(RwLockDocument {
            _id: id,
            reads: 0,
            write: false,
        })
        .await?;
        Ok(Self {
            id,
            collection: col,
        })
    }
    /// Create [`RwLock`] without initializing the lock.
    ///
    /// This should be used when the lock is already initialized; possibly by another process.
    #[inline]
    pub async fn new_uninit(database: &mongodb::Database, collection: &str, id: ObjectId) -> Self {
        let col = database.collection(collection);
        Self {
            id,
            collection: col,
        }
    }
    /// Calls [`RwLock::read`] with [`DEFAULT_TIMEOUT`] and [`DEFAULT_WAIT`].
    ///
    /// # Errors
    ///
    /// When [`RwLock::read`] errors.
    #[inline]
    pub async fn read_default(&self) -> Result<RwLockReadGuard<'_>, RwLockReadError> {
        self.read(DEFAULT_TIMEOUT, DEFAULT_WAIT).await
    }
    /// Locks for reading.
    ///
    /// # Errors
    ///
    /// When:
    /// - Timing out.
    /// - [`mongodb::Collection::find_one_and_update`] errors.
    #[inline]
    pub async fn read(
        &self,
        timeout: Duration,
        wait: Duration,
    ) -> Result<RwLockReadGuard<'_>, RwLockReadError> {
        let now = Instant::now();
        loop {
            if now.elapsed() > timeout {
                return Err(RwLockReadError::Timeout);
            }
            let result = self
                .collection
                .find_one_and_update(
                    doc! { "_id": self.id, "write": false },
                    doc! { "$inc": { "reads": 1i32 } },
                )
                .await
                .map_err(RwLockReadError::Query)?;
            if let Some(RwLockDocument { _id, write, .. }) = result {
                debug_assert_eq!(write, false, "Write should be false.");
                break Ok(RwLockReadGuard {
                    lock: self,
                    rt: Handle::current(),
                });
            }
            sleep(wait).await;
        }
    }
    /// Release a read lock.
    async fn release_read(&self) -> Result<(), RwLockReleaseReadError> {
        let delete = self
            .collection
            .find_one_and_update(doc! { "_id": self.id }, doc! { "$inc": {"reads": -1i32} })
            .await
            .map_err(RwLockReleaseReadError::Query)?
            .ok_or(RwLockReleaseReadError::Find)?;
        debug_assert!(delete.reads > 0i32, "Reads should be greater than 0");
        debug_assert_eq!(delete.write, false, "Write lock should be false");
        Ok(())
    }
    /// Calls [`RwLock::write`] with [`DEFAULT_TIMEOUT`] and [`DEFAULT_WAIT`].
    ///
    /// # Errors
    ///
    /// When [`RwLock::write`] errors.
    #[inline]
    pub async fn write_default(&self) -> Result<RwLockWriteGuard<'_>, RwLockWriteError> {
        self.write(DEFAULT_TIMEOUT, DEFAULT_WAIT).await
    }
    /// Locks for writing.
    ///
    /// # Errors
    ///
    /// When:
    /// - Timing out.
    /// - [`mongodb::Collection::find_one_and_update`] errors.
    #[inline]
    pub async fn write(
        &self,
        timeout: Duration,
        wait: Duration,
    ) -> Result<RwLockWriteGuard<'_>, RwLockWriteError> {
        let now = Instant::now();
        loop {
            if now.elapsed() > timeout {
                return Err(RwLockWriteError::Timeout);
            }
            let result = self
                .collection
                .find_one_and_update(
                    doc! { "_id": self.id, "reads": 0i32, "write": false },
                    doc! { "$set": { "write": true } },
                )
                .await
                .map_err(RwLockWriteError::Query)?;
            if let Some(RwLockDocument { _id, reads, write }) = result {
                debug_assert_eq!(reads, 0i32, "reads should be >0");
                debug_assert_eq!(write, false, "write should be false");
                break Ok(RwLockWriteGuard {
                    lock: self,
                    rt: Handle::current(),
                });
            }
            sleep(wait).await;
        }
    }
    /// Releases the write lock.
    async fn release_write(&self) -> Result<(), RwLockReleaseWriteError> {
        let delete = self
            .collection
            .find_one_and_update(
                doc! { "_id": self.id, "write": true },
                doc! { "$set": {"write": false} },
            )
            .await
            .map_err(RwLockReleaseWriteError::Query)?
            .ok_or(RwLockReleaseWriteError::Find)?;
        debug_assert_eq!(delete.reads, 0i32, "Reads should be zero");
        Ok(())
    }
}

/// A distributed lock guard that acts like [`std::sync::RwLockReadGuard`].
pub struct RwLockReadGuard<'a> {
    /// Lock.
    lock: &'a RwLock,
    /// Tokio runtime handle.
    rt: Handle,
}

// TODO Remove below `expect`.
#[expect(
    clippy::unwrap_used,
    reason = "I do not know a way to propagate the error."
)]
impl Drop for RwLockReadGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        let rt = self.rt.clone();
        let lock = RwLock {
            collection: self.lock.collection.clone(),
            id: self.lock.id,
        };
        task::spawn_blocking(move || {
            rt.block_on(async { lock.release_read().await }).unwrap();
        });
    }
}

/// A distributed lock guard that acts like [`std::sync::RwLockWriteGuard`].
pub struct RwLockWriteGuard<'a> {
    /// Lock.
    lock: &'a RwLock,
    /// Tokio runtime handle.
    rt: Handle,
}

// TODO Remove below `expect`.
#[expect(
    clippy::unwrap_used,
    reason = "I do not know a way to propagate the error."
)]
impl Drop for RwLockWriteGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        let rt = self.rt.clone();
        let lock = RwLock {
            collection: self.lock.collection.clone(),
            id: self.lock.id,
        };
        task::spawn_blocking(move || {
            rt.block_on(async { lock.release_write().await }).unwrap();
        });
    }
}

/// The document used for backing [`RwLock`].
#[derive(Debug, Serialize, Deserialize)]
struct RwLockDocument {
    /// Lock id
    pub _id: ObjectId,
    /// How many read locks are held.
    pub reads: i32,
    /// Is write lock held.
    pub write: bool,
}
