use bson::doc;
use bson::oid::ObjectId;
use mongodb::Client;
use mongodb_lock::Mutex;
use mongodb_lock::RwLock;
use serde::{Deserialize, Serialize};
use std::cmp::{max, min};
use std::net::Ipv4Addr;
use std::process::{Command, Stdio};
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicU16};
use std::sync::Arc;
use std::time::Duration;
use tokio::task;
use tokio::time::sleep;
use tracing::info;

struct MongodbClient {
    client: mongodb::Client,
    _server: Arc<Mongodb>,
}
impl AsRef<Client> for MongodbClient {
    fn as_ref(&self) -> &Client {
        &self.client
    }
}

pub struct Mongodb {
    name: String,
    port: u16,
}

impl Mongodb {
    async fn new() -> Self {
        static PORT: AtomicU16 = AtomicU16::new(27021);
        // atlas deployments setup tester2 --type local --port 8082 --force
        let port = PORT.fetch_add(1, Ordering::SeqCst);
        let name = uuid::Uuid::new_v4().to_string();
        info!("port: {port}");
        info!("name: {name}");
        let _mongodb = Command::new("atlas")
            .args([
                "deployments",
                "setup",
                &name,
                "--type",
                "local",
                "--port",
                &port.to_string(),
                "--force",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .unwrap();
        info!("setup deployment");
        sleep(Duration::from_secs(3)).await;
        Mongodb { name, port }
    }
    fn client(this: Arc<Self>) -> MongodbClient {
        let host = mongodb::options::ServerAddress::Tcp {
            host: Ipv4Addr::LOCALHOST.to_string(),
            port: Some(this.port),
        };
        let opts = mongodb::options::ClientOptions::builder()
            .hosts(vec![host])
            .direct_connection(true)
            .build();
        let client = mongodb::Client::with_options(opts).unwrap();
        MongodbClient {
            client,
            _server: this,
        }
    }
}
impl Drop for Mongodb {
    fn drop(&mut self) {
        info!("deleting deployment");
        let _mongodb = Command::new("atlas")
            .args(["deployments", "delete", &self.name, "--force"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .unwrap();
        info!("deleted");
    }
}

#[tokio::test]
async fn single() {
    use mongodb_lock::*;
    static CHECK: AtomicBool = AtomicBool::new(false);
    #[derive(Clone, Serialize, Deserialize)]
    struct MyDocument {
        _id: ObjectId,
        x: i32,
    }
    let _guard = tracing::subscriber::set_default(
        tracing_subscriber::fmt::Subscriber::builder()
            .with_test_writer()
            .finish(),
    );
    let mongodb = Arc::new(Mongodb::new().await);
    let client = Mongodb::client(mongodb);
    let db = client.as_ref().database("basic");
    let docs = db.collection::<MyDocument>("docs");
    let lock = Arc::new(Mutex::new(&db, "locks", ["two"]).await.unwrap());
    let one = MyDocument {
        _id: ObjectId::new(),
        x: 1,
    };
    let two = MyDocument {
        _id: ObjectId::new(),
        x: 1,
    };
    let three = MyDocument {
        _id: ObjectId::new(),
        x: 1,
    };
    docs.insert_many(vec![one.clone(), two.clone(), three.clone()])
        .await
        .unwrap();

    let one_id = one._id;
    let two_id = two._id;
    let clock = lock.clone();
    let cdocs = docs.clone();
    let first = task::spawn(async move {
        let guard = clock.lock_default(doc! { "two": two_id }).await.unwrap();
        assert!(!CHECK.swap(true, Ordering::SeqCst));
        let a = cdocs
            .find_one(doc! { "_id": one_id })
            .await
            .unwrap()
            .unwrap();
        let b = cdocs
            .find_one(doc! { "_id": two_id })
            .await
            .unwrap()
            .unwrap();
        cdocs
            .update_many(
                doc! { "_id": { "$in": [one_id,two_id] }},
                doc! { "$set": { "x": a.x + b.x } },
            )
            .await
            .unwrap();
        assert!(CHECK.swap(false, Ordering::SeqCst));
        drop(guard)
    });
    let two_id = two._id;
    let three_id = three._id;
    let clock = lock.clone();
    let cdocs = docs.clone();
    let second = task::spawn(async move {
        let guard = clock.lock_default(doc! { "two": two_id }).await.unwrap();
        assert!(!CHECK.swap(true, Ordering::SeqCst));
        let a = cdocs
            .find_one(doc! { "_id": two_id })
            .await
            .unwrap()
            .unwrap();
        let b = cdocs
            .find_one(doc! { "_id": three_id })
            .await
            .unwrap()
            .unwrap();
        cdocs
            .update_many(
                doc! { "_id": { "$in": [two_id,three_id] } },
                doc! { "$set": { "x": a.x + b.x } },
            )
            .await
            .unwrap();
        assert!(CHECK.swap(false, Ordering::SeqCst));
        drop(guard)
    });
    first.await.unwrap();
    second.await.unwrap();
    let a = docs
        .find_one(doc! { "_id": one_id })
        .await
        .unwrap()
        .unwrap()
        .x;
    let b = docs
        .find_one(doc! { "_id": two_id })
        .await
        .unwrap()
        .unwrap()
        .x;
    let c = docs
        .find_one(doc! { "_id": three_id })
        .await
        .unwrap()
        .unwrap()
        .x;
    assert!((a == 2 && b == 3 && c == 3) || (a == 3 && b == 3 && c == 2));
}

/// This tests operates on 4 documents split between 2 collections, suming each pair together a
/// number of times. The pairs never interact with each other. Thus the lock is exclusive over pair
/// matches e.g. `&[[cida1,cidb1]]` and `&[[cida2,cidb2]]`, it would technically be possible to
/// acquire a lock on `&[[cida1,cidb2]]` while holding a lock on `&[[cida1,cidb1]]` since each pair
/// is unique.
///
/// This test sets each document in each pair to the value of the other document +1 `N`
/// times.
#[tokio::test]
async fn adder() {
    #[derive(Debug, Serialize, Deserialize)]
    struct Number {
        _id: ObjectId,
        x: i32,
    }
    const N: i32 = 10;
    const A: i32 = 0;
    const B: i32 = 10;

    static CHECK_ONE: AtomicBool = AtomicBool::new(false);
    static CHECK_TWO: AtomicBool = AtomicBool::new(false);

    let _guard = tracing::subscriber::set_default(
        tracing_subscriber::fmt::Subscriber::builder()
            .with_test_writer()
            .finish(),
    );
    let mongodb = Arc::new(Mongodb::new().await);
    let client = Mongodb::client(mongodb);
    let db = client.as_ref().database("adder");
    let lock = Arc::new(Mutex::new(&db, "locks", ["pair"]).await.unwrap());
    let cola = db.collection::<Number>("first");
    let colb = db.collection::<Number>("second");
    let ida1 = ObjectId::new();
    let ida2 = ObjectId::new();
    cola.insert_one(Number { _id: ida1, x: A }).await.unwrap();
    cola.insert_one(Number { _id: ida2, x: B }).await.unwrap();
    let idb1 = ObjectId::new();
    let idb2 = ObjectId::new();
    colb.insert_one(Number { _id: idb1, x: A }).await.unwrap();
    colb.insert_one(Number { _id: idb2, x: B }).await.unwrap();

    let tasks = (0..N)
        .flat_map(|_| {
            let (clock, ccola, ccolb, cida1, cidb1) =
                (lock.clone(), cola.clone(), colb.clone(), ida1, idb1);
            let one = task::spawn(async move {
                let guard = clock
                    .lock_default(doc! { "pair": [cida1, cidb1] })
                    .await
                    .unwrap();
                assert!(!CHECK_ONE.swap(true, Ordering::SeqCst));
                let num = ccola
                    .find_one(doc! { "_id": cida1 })
                    .await
                    .unwrap()
                    .unwrap();
                ccolb
                    .update_one(doc! { "_id": cidb1 }, doc! { "$set": { "x": num.x + 1 } })
                    .await
                    .unwrap();
                let num = ccolb
                    .find_one(doc! { "_id": cidb1 })
                    .await
                    .unwrap()
                    .unwrap();
                ccola
                    .update_one(doc! { "_id": cida1 }, doc! { "$set": { "x": num.x + 1 } })
                    .await
                    .unwrap();
                assert!(CHECK_ONE.swap(false, Ordering::SeqCst));
                drop(guard);
            });
            let (clock, ccola, ccolb, cida2, cidb2) =
                (lock.clone(), cola.clone(), colb.clone(), ida2, idb2);
            let two = task::spawn(async move {
                let guard = clock
                    .lock_default(doc! { "pair": [cida2, cidb2] })
                    .await
                    .unwrap();
                assert!(!CHECK_TWO.swap(true, Ordering::SeqCst));
                let num = ccola
                    .find_one(doc! { "_id": cida2 })
                    .await
                    .unwrap()
                    .unwrap();
                ccolb
                    .update_one(doc! { "_id": cidb2 }, doc! { "$set": { "x": num.x + 1 } })
                    .await
                    .unwrap();
                let num = ccolb
                    .find_one(doc! { "_id": cidb2 })
                    .await
                    .unwrap()
                    .unwrap();
                ccola
                    .update_one(doc! { "_id": cida2 }, doc! { "$set": { "x": num.x + 1 } })
                    .await
                    .unwrap();
                assert!(CHECK_TWO.swap(false, Ordering::SeqCst));
                drop(guard);
            });
            [one, two]
        })
        .collect::<Vec<_>>();
    for task in tasks {
        task.await.unwrap();
    }

    const T: i32 = N * 2;
    let numa1 = cola.find_one(doc! { "_id": ida1 }).await.unwrap().unwrap();
    let numb1 = colb.find_one(doc! { "_id": idb1 }).await.unwrap().unwrap();
    info!("numa1: {numa1:?}");
    info!("numb1: {numb1:?}");
    assert!(
        (numa1.x == T - A + 1 && numb1.x == T + A) || (numb1.x == T + A - 1 && numa1.x == T + A)
    );
    let numa2 = cola.find_one(doc! { "_id": ida2 }).await.unwrap().unwrap();
    let numb2 = colb.find_one(doc! { "_id": idb2 }).await.unwrap().unwrap();
    info!("numa2: {numa2:?}");
    info!("numb2: {numb2:?}");
    assert!(
        (numa2.x == T + B - 1 && numb2.x == T + B) || (numb2.x == T + B - 1 && numa2.x == T + B)
    );
}

/// This tests operates on 4 documents split between 2 collections, pairs of the documents together.
/// The pairs overlap with each other. Thus the lock is exclusive on each document
/// matche e.g. `&[cida1,cidb1]`, `&[cida2,cidb2]`, `&[cida1,cidb2]` etc. it is not possible to
/// acquire a lock on `&[cida1,cidb2]` while holding a lock on `&[cida1,cidb1]` since `cida1` is
/// exclusive.
///
/// This test operates on the pairs `[cida1, cidb1]`, `[cida2, cidb2]`, `[cida1, cidb2]` and
/// `[cida2, cidb1]` this means there are a large number of possible results so we don't test the
/// result values and only check that none execution concurrently using atomic statics.
#[tokio::test]
async fn adder_complex() {
    static CHECK: AtomicBool = AtomicBool::new(false);
    #[derive(Debug, Serialize, Deserialize)]
    struct Number {
        _id: ObjectId,
        x: i32,
    }
    const N: i32 = 10;
    const A: i32 = 0;
    const B: i32 = 10;

    let _guard = tracing::subscriber::set_default(
        tracing_subscriber::fmt::Subscriber::builder()
            .with_test_writer()
            .finish(),
    );
    let mongodb = Arc::new(Mongodb::new().await);
    let client = Mongodb::client(mongodb);
    let db = client.as_ref().database("adder");
    let lock = Arc::new(Mutex::new(&db, "locks", ["min", "max"]).await.unwrap());
    let cola = db.collection::<Number>("first");
    let colb = db.collection::<Number>("second");
    let ida1 = ObjectId::new();
    let ida2 = ObjectId::new();
    cola.insert_one(Number { _id: ida1, x: A }).await.unwrap();
    cola.insert_one(Number { _id: ida2, x: B }).await.unwrap();
    let idb1 = ObjectId::new();
    let idb2 = ObjectId::new();
    colb.insert_one(Number { _id: idb1, x: A }).await.unwrap();
    colb.insert_one(Number { _id: idb2, x: B }).await.unwrap();

    let lock_doc = |id1, id2| doc! { "min": min(id1, id2), "max": max(id1, id2) };

    let tasks = (0..N)
        .flat_map(|_| {
            let (clock, ccola, ccolb, cida1, cidb1) =
                (lock.clone(), cola.clone(), colb.clone(), ida1, idb1);
            let one = task::spawn(async move {
                let guard = clock.lock_default(lock_doc(cida1, cidb1)).await.unwrap();
                assert!(!CHECK.swap(true, Ordering::SeqCst));
                let num = ccola
                    .find_one(doc! { "_id": cida1 })
                    .await
                    .unwrap()
                    .unwrap();
                ccolb
                    .update_one(doc! { "_id": cidb1 }, doc! { "$set": { "x": num.x + 1 } })
                    .await
                    .unwrap();
                let num = ccolb
                    .find_one(doc! { "_id": cidb1 })
                    .await
                    .unwrap()
                    .unwrap();
                ccola
                    .update_one(doc! { "_id": cida1 }, doc! { "$set": { "x": num.x + 1 } })
                    .await
                    .unwrap();
                assert!(CHECK.swap(false, Ordering::SeqCst));
                drop(guard);
            });
            let (clock, ccola, ccolb, cida2, cidb2) =
                (lock.clone(), cola.clone(), colb.clone(), ida2, idb2);
            let two = task::spawn(async move {
                let guard = clock.lock_default(lock_doc(cida2, cidb2)).await.unwrap();
                assert!(!CHECK.swap(true, Ordering::SeqCst));
                let num = ccola
                    .find_one(doc! { "_id": cida2 })
                    .await
                    .unwrap()
                    .unwrap();
                ccolb
                    .update_one(doc! { "_id": cidb2 }, doc! { "$set": { "x": num.x + 1 } })
                    .await
                    .unwrap();
                let num = ccolb
                    .find_one(doc! { "_id": cidb2 })
                    .await
                    .unwrap()
                    .unwrap();
                ccola
                    .update_one(doc! { "_id": cida2 }, doc! { "$set": { "x": num.x + 1 } })
                    .await
                    .unwrap();
                assert!(CHECK.swap(false, Ordering::SeqCst));
                drop(guard);
            });
            let (clock, ccola, ccolb, cida1, cidb2) =
                (lock.clone(), cola.clone(), colb.clone(), ida1, idb2);
            let three = task::spawn(async move {
                let guard = clock.lock_default(lock_doc(cida1, cidb2)).await.unwrap();
                assert!(!CHECK.swap(true, Ordering::SeqCst));
                let num = ccola
                    .find_one(doc! { "_id": cida1 })
                    .await
                    .unwrap()
                    .unwrap();
                ccolb
                    .update_one(doc! { "_id": cidb2 }, doc! { "$set": { "x": num.x + 1 } })
                    .await
                    .unwrap();
                let num = ccolb
                    .find_one(doc! { "_id": cidb2 })
                    .await
                    .unwrap()
                    .unwrap();
                ccola
                    .update_one(doc! { "_id": cida1 }, doc! { "$set": { "x": num.x + 1 } })
                    .await
                    .unwrap();
                assert!(CHECK.swap(false, Ordering::SeqCst));
                drop(guard);
            });
            let (clock, ccola, ccolb, cida2, cidb1) =
                (lock.clone(), cola.clone(), colb.clone(), ida2, idb1);
            let four = task::spawn(async move {
                let guard = clock.lock_default(lock_doc(cida2, cidb1)).await.unwrap();
                assert!(!CHECK.swap(true, Ordering::SeqCst));
                let num = ccola
                    .find_one(doc! { "_id": cida2 })
                    .await
                    .unwrap()
                    .unwrap();
                ccolb
                    .update_one(doc! { "_id": cidb1 }, doc! { "$set": { "x": num.x + 1 } })
                    .await
                    .unwrap();
                let num = ccolb
                    .find_one(doc! { "_id": cidb1 })
                    .await
                    .unwrap()
                    .unwrap();
                ccola
                    .update_one(doc! { "_id": cida2 }, doc! { "$set": { "x": num.x + 1 } })
                    .await
                    .unwrap();
                assert!(CHECK.swap(false, Ordering::SeqCst));
                drop(guard);
            });
            [one, two, three, four]
        })
        .collect::<Vec<_>>();
    for task in tasks {
        task.await.unwrap();
    }

    let numa1 = cola.find_one(doc! { "_id": ida1 }).await.unwrap().unwrap();
    let numb1 = colb.find_one(doc! { "_id": idb1 }).await.unwrap().unwrap();
    let numa2 = cola.find_one(doc! { "_id": ida2 }).await.unwrap().unwrap();
    let numb2 = colb.find_one(doc! { "_id": idb2 }).await.unwrap().unwrap();
    info!("numa1: {numa1:?}");
    info!("numb1: {numb1:?}");
    info!("numa2: {numa2:?}");
    info!("numb2: {numb2:?}");
    // TODO Check the result values.
}

#[tokio::test]
async fn reader() {
    #[derive(Debug, Serialize, Deserialize)]
    struct Number {
        _id: ObjectId,
        x: i32,
    }
    const READS: usize = 10;
    const WRITE: usize = 10;

    let _guard = tracing::subscriber::set_default(
        tracing_subscriber::fmt::Subscriber::builder()
            .with_test_writer()
            .finish(),
    );
    let mongodb = Arc::new(Mongodb::new().await);
    let client = Mongodb::client(mongodb);
    let db = client.as_ref().database("adder");
    let lock = Arc::new(RwLock::new(&db, "locks").await.unwrap());
    let col = db.collection::<Number>("first");
    let id = ObjectId::new();
    col.insert_one(Number { _id: id, x: 0 }).await.unwrap();

    let reads = (0..READS)
        .map(|_| {
            let clock = lock.clone();
            let ccol = col.clone();
            let cid = id.clone();
            task::spawn(async move {
                let _guard = clock.read_default().await.unwrap();
                let a = ccol.find_one(doc! { "_id": cid }).await.unwrap().unwrap().x;
                assert_eq!(
                    ccol.find_one(doc! { "_id": cid }).await.unwrap().unwrap().x,
                    a
                );
                assert_eq!(
                    ccol.find_one(doc! { "_id": cid }).await.unwrap().unwrap().x,
                    a
                );
                assert_eq!(
                    ccol.find_one(doc! { "_id": cid }).await.unwrap().unwrap().x,
                    a
                );
            })
        })
        .collect::<Vec<_>>();
    let writes = (0..WRITE)
        .map(|_| {
            let clock = lock.clone();
            let ccol = col.clone();
            let cid = id.clone();
            task::spawn(async move {
                let _guard = clock.write_default().await.unwrap();
                let a = ccol.find_one(doc! { "_id": cid }).await.unwrap().unwrap().x;
                ccol.update_one(doc! {"_id": cid}, doc! { "$inc": { "x": 1i32 } })
                    .await
                    .unwrap();
                assert_eq!(
                    ccol.find_one(doc! { "_id": cid }).await.unwrap().unwrap().x,
                    a + 1
                );
            })
        })
        .collect::<Vec<_>>();

    for read in reads {
        read.await.unwrap();
    }
    for write in writes {
        write.await.unwrap();
    }
}
