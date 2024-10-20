# mongodb-lock

[![Crates.io](https://img.shields.io/crates/v/mongodb-lock)](https://crates.io/crates/mongodb-lock)
[![docs](https://img.shields.io/crates/v/mongodb-lock?color=yellow&label=docs)](https://docs.rs/mongodb-lock)

Rusty distributed locking backed by Mongodb.

```rust
#[derive(Clone, Serialize, Deserialize)]
struct MyDocument {
    _id: ObjectId,
    x: i32,
}
let db = client.database("basic");
let docs = db.collection::<MyDocument>("docs");
let lock = Arc::new(mongodb_lock::Mutex::new(&db, "locks").await.unwrap());
let one = MyDocument { _id: ObjectId::new(), x: 1 };
let two = MyDocument { _id: ObjectId::new(), x: 1 };
let three = MyDocument { _id: ObjectId::new(), x: 1 };
docs.insert_many(vec![one.clone(), two.clone(), three.clone()]).await.unwrap();

let one_id = one._id;
let two_id = two._id;
let clock = lock.clone();
let cdocs = docs.clone();
let first = task::spawn(async move {
    let _guard = clock.lock_default([one_id, two_id]).await.unwrap();
    let a = cdocs.find_one(doc! { "_id": one_id }).await.unwrap().unwrap();
    let b = cdocs.find_one(doc! { "_id": two_id }).await.unwrap().unwrap();
    cdocs.update_many(
        doc! { "_id": { "$in": [one_id,two_id] }},
        doc! { "$set": { "x": a.x + b.x } }
    ).await.unwrap();
});

let two_id = two._id;
let three_id = three._id;
let clock = lock.clone();
let cdocs = docs.clone();
let second = task::spawn(async move {
    let _guard = lock.lock_default([two_id, three_id]).await.unwrap();
    let a = cdocs.find_one(doc! { "_id": two_id }).await.unwrap().unwrap();
    let b = cdocs.find_one(doc! { "_id": three_id }).await.unwrap().unwrap();
    cdocs.update_many(
        doc! { "_id": { "$in": [two_id,three_id] } },
        doc! { "$set": { "x": a.x + b.x } }
    ).await.unwrap();
});

first.await.unwrap();
second.await.unwrap();

let a = docs.find_one(doc! { "_id": one_id }).await.unwrap().unwrap().x;
let b = docs.find_one(doc! { "_id": two_id }).await.unwrap().unwrap().x;
let c = docs.find_one(doc! { "_id": three_id }).await.unwrap().unwrap().x;
assert!((a == 2 && b == 3 && c == 3) || (a == 3 && b == 3 && c == 2));
```