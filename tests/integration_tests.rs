use bson::doc;
use bson::oid::ObjectId;
use mongodb::Client;
use mongodb_lock::MutexDocument;
use serde::Serialize;
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

struct Mongodb {
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
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
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
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .output()
            .unwrap();
        info!("deleted");
    }
}

#[tokio::test]
async fn basic() {
    let mongodb = Arc::new(Mongodb::new().await);
    let client = Mongodb::client(mongodb);
    let db = client.as_ref().database("basic");
    let lock_col = db.collection::<_>("locks");
    let key = [ObjectId::new(), ObjectId::new()];
    let guard = mongodb_lock::Mutex::new_default(lock_col.clone(), key)
        .await
        .unwrap();
    let lock_doc = MutexDocument {
        _id: ObjectId::new(),
        key,
    };
    let insert = lock_col.insert_one(lock_doc).await.unwrap_err();
    assert!(mongodb_lock::is_duplicate_key_error(&insert));
    drop(guard);

    // It is important to give some leeway to allow the locks to get the handle
    // to the runtime when releasing.
    sleep(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn adder() {
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize)]
    struct Number {
        _id: ObjectId,
        x: i32,
    }
    const N: i32 = 5;
    const A: i32 = 0;
    const B: i32 = 10;

    static CHECK_ONE: AtomicBool = AtomicBool::new(false);
    static CHECK_TWO: AtomicBool = AtomicBool::new(false);

    tracing_subscriber::fmt::init();
    let mongodb = Arc::new(Mongodb::new().await);
    let client = Mongodb::client(mongodb);
    let db = client.as_ref().database("adder");
    let lock_col = db.collection::<_>("locks");
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
            let (clock_col, ccola, ccolb, cida1, cidb1) =
                (lock_col.clone(), cola.clone(), colb.clone(), ida1, idb1);
            let one = task::spawn(async move {
                let guard = mongodb_lock::Mutex::new_default(clock_col, [cida1, cidb1])
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
            let (clock_col, ccola, ccolb, cida2, cidb2) =
                (lock_col.clone(), cola.clone(), colb.clone(), ida2, idb2);
            let two = task::spawn(async move {
                let guard = mongodb_lock::Mutex::new_default(clock_col, [cida2, cidb2])
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
    

    let numa1 = cola.find_one(doc! { "_id": ida1 }).await.unwrap().unwrap();
    let numb1 = colb.find_one(doc! { "_id": idb1 }).await.unwrap().unwrap();
    info!("numa1: {numa1:?}");
    info!("numb1: {numb1:?}");
    assert!(
        (numa1.x == N + A + 1 && numb1.x == N + A) || (numb1.x == N + A + 1 && numa1.x == N + A)
    );
    let numa2 = cola.find_one(doc! { "_id": ida2 }).await.unwrap().unwrap();
    let numb2 = colb.find_one(doc! { "_id": idb2 }).await.unwrap().unwrap();
    info!("numa2: {numa2:?}");
    info!("numb2: {numb2:?}");
    assert!(
        (numa2.x == N + B + 1 && numb2.x == N + B) || (numb2.x == N + B + 1 && numa2.x == N + B)
    );

    // It is important to give some leeway to allow the locks to get the handle
    // to the runtime when releasing.
    sleep(Duration::from_secs(5)).await;
}
