# mongodb-lock

[![Crates.io](https://img.shields.io/crates/v/mongodb-lock)](https://crates.io/crates/mongodb-lock)
[![docs](https://img.shields.io/crates/v/mongodb-lock?color=yellow&label=docs)](https://docs.rs/mongodb-lock)

Rusty distributed locking backed by Mongodb.

## Mutex

This approach supports the simple case of enforcing mutual exclusion on a single docoument e.g.
```rust
let lock = Mutex::new("my_database", "lock_collection", ["id"]);
let guard = lock.lock_default(doc! { "id": my_document_id }).await?;
```
While also supporting the more complex case where operations require mutual exclusion over
multiple documents e.g.
```rust
use std::cmp;
let lock = Mutex::new("my_database", "lock_collection", ["min","max"]);
let get_doc = |x,y| doc! { "min": cmp::min(x,y), "max": cmp::max(x,y) };

// Both of these guards can be held at the same time.
let guard_one = lock.lock_default(get_doc(Some(id_one),None)).await?;
let guard_two = lock.lock_default(get_doc(Some(id_two),None)).await?;

// None of these guards can be held at the same time.
// `guard_three` conflicts on `id_one`.
// `guard_four` and `guard_six` conflict on `id_one` and `id_two`.
let guard_three = lock.lock_default(get_doc(Some(id_one),None)).await?;
let guard_four = lock.lock_default(get_doc(Some(id_two),Some(id_one))).await?;
let guard_six = lock.lock_default(get_doc(Some(id_one), Some(id_two))).await?;
```
The use-case is where you have operations which require exclusive access to a single user (e.g.
deleting a user) and operations which require exclusive access to multiple users (e.g. sending a
message between users).