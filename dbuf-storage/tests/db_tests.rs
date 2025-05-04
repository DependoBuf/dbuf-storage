use dbuf_storage::Database;
use serde::{Deserialize, Serialize};
use std::fs;

#[derive(Debug, Serialize, Deserialize)]
pub enum ConstructorError {
    MismatchedDependencies,
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct Message<Body, Dependencies> {
    pub body: Body,
    pub dependencies: Dependencies,
}

pub type Box<T> = std::boxed::Box<T>;

pub mod sum {
    use serde::{Deserialize, Serialize};

    mod deps {
        pub use super::super::{ConstructorError, Message};
    }

    #[derive(PartialEq, Eq, Debug, Serialize, Deserialize)]
    pub struct Body {}

    #[derive(PartialEq, Eq, Debug, Serialize, Deserialize)]
    pub struct Dependencies {
        pub a: i32,
    }

    // alias for the generated type
    pub type Sum = deps::Message<Body, Dependencies>;

    // inherit implementation with all constructors
    impl Sum {
        pub fn new(dependencies: Dependencies) -> Result<Self, deps::ConstructorError> {
            let body = Body {};
            Ok(deps::Message { body, dependencies })
        }
    }
}

pub use sum::Sum;

pub mod foo {
    use deps::sum;
    use serde::{Deserialize, Serialize};

    mod deps {
        pub use super::super::{sum, Sum};
        pub use super::super::{ConstructorError, Message};
    }

    #[derive(PartialEq, Eq, Debug, Serialize, Deserialize)]
    pub struct Body {
        pub sum: deps::Sum,
    }

    #[derive(PartialEq, Eq, Debug, Serialize, Deserialize)]
    pub struct Dependencies {
        pub a: i32,
        pub b: i32,
    }

    // alias for the generated type
    pub type Foo = deps::Message<Body, Dependencies>;

    // inherit implementation with all constructors
    impl Foo {
        pub fn new(dependencies: Dependencies) -> Result<Self, deps::ConstructorError> {
            let body = Body {
                sum: deps::Sum::new(sum::Dependencies {
                    a: -dependencies.a + dependencies.b,
                })
                .expect("..."),
            };
            Ok(deps::Message { body, dependencies })
        }
    }
}

pub mod user {
    use serde::{Deserialize, Serialize};

    mod deps {
        pub use super::super::{ConstructorError, Message};
    }

    #[derive(PartialEq, Eq, Debug, Serialize, Deserialize)]
    pub struct Body {
        pub a: i32,
        pub b: i32,
        pub c: i32,
    }

    #[derive(PartialEq, Eq, Debug, Serialize, Deserialize)]
    pub struct Dependencies {}

    // alias for the generated type
    pub type User = deps::Message<Body, Dependencies>;

    // inherit implementation with all constructors
    impl User {
        pub fn new(body: Body) -> Result<Self, deps::ConstructorError> {
            let dependencies = Dependencies {};
            Ok(deps::Message { body, dependencies })
        }
    }
}

pub mod nat {
    // general prelude
    use super::{Box, ConstructorError, Message};
    use serde::{Deserialize, Serialize};

    // optional part where used types are imported

    // body definition
    #[derive(PartialEq, Eq, Debug, Serialize, Deserialize)]
    pub enum Body {
        Zero,
        Suc { pred: Box<Self> },
    }

    // dependencies definition
    #[derive(PartialEq, Eq, Debug, Serialize, Deserialize)]
    pub struct Dependencies {}

    // alias for the generated type
    pub type Nat = Message<Body, Dependencies>;

    // inherit implementation with all constructors
    impl Nat {
        pub fn zero() -> Result<Self, ConstructorError> {
            let body = Body::Zero;
            let dependencies = Dependencies {};
            Ok(Message { body, dependencies })
        }

        pub fn suc(pred: Nat) -> Result<Self, ConstructorError> {
            let body = Body::Suc {
                pred: Box::new(pred.body),
            };
            let dependencies = Dependencies {};
            Ok(Message { body, dependencies })
        }
    }
}

const TEST_DATA: &str = "./tests/test_data";

struct CleanupDir(String);
impl Drop for CleanupDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn test_sum_db_operations() {
    let db_path = format!("{}/test_sum_db_operations", TEST_DATA);
    let db = Database::new(Some(&db_path)).expect("Failed to open database");
    let collection = db.collection("sum").expect("Failed to create collection");

    let _cleanup = CleanupDir(db_path.clone());

    let sum_deps = sum::Dependencies { a: 42 };
    let sum = sum::Sum::new(sum_deps).unwrap();

    let id = collection.insert(&sum).unwrap();

    let retrieved: sum::Sum = collection.get(&id).unwrap();

    assert_eq!(retrieved.dependencies.a, 42);
    assert_eq!(retrieved, sum);

    let updated_deps = sum::Dependencies { a: 100 };
    let updated_sum = sum::Sum::new(updated_deps).unwrap();

    collection.update(&id, &updated_sum).unwrap();

    let retrieved_updated: sum::Sum = collection.get(&id).unwrap();
    assert_eq!(retrieved_updated.dependencies.a, 100);
    assert_eq!(retrieved_updated, updated_sum);
}

#[test]
fn test_foo_db_operations() {
    let db_path = format!("{}/test_foo_db_operations", TEST_DATA);
    let db = Database::new(Some(&db_path)).expect("Failed to open database");
    let collection = db.collection("foo").expect("Failed to create collection");

    let _cleanup = CleanupDir(db_path.clone());

    let foo_deps = foo::Dependencies { a: 10, b: 20 };
    let foo = foo::Foo::new(foo_deps).unwrap();

    let id = collection.insert(&foo).unwrap();

    let retrieved: foo::Foo = collection.get(&id).unwrap();

    assert_eq!(retrieved.dependencies.a, 10);
    assert_eq!(retrieved.dependencies.b, 20);
    assert_eq!(retrieved.body.sum.dependencies.a, 10); // -a + b = -10 + 20 = 10
    assert_eq!(retrieved, foo);

    let updated_deps = foo::Dependencies { a: 30, b: 50 };
    let updated_foo = foo::Foo::new(updated_deps).unwrap();

    collection.update(&id, &updated_foo).unwrap();

    let retrieved_updated: foo::Foo = collection.get(&id).unwrap();
    assert_eq!(retrieved_updated.dependencies.a, 30);
    assert_eq!(retrieved_updated.dependencies.b, 50);
    assert_eq!(retrieved_updated.body.sum.dependencies.a, 20); // -a + b = -30 + 50 = 20
    assert_eq!(retrieved_updated, updated_foo);
}

#[test]
fn test_user_db_operations() {
    let db_path = format!("{}/test_user_db_operations", TEST_DATA);
    let db = Database::new(Some(&db_path)).expect("Failed to open database");
    let collection = db.collection("foo").expect("Failed to create collection");

    let _cleanup = CleanupDir(db_path.clone());

    let user_body = user::Body { a: 1, b: 2, c: 3 };
    let user = user::User::new(user_body).unwrap();

    let id = collection.insert(&user).unwrap();

    let retrieved: user::User = collection.get(&id).unwrap();

    assert_eq!(retrieved.body.a, 1);
    assert_eq!(retrieved.body.b, 2);
    assert_eq!(retrieved.body.c, 3);
    assert_eq!(retrieved, user);

    let updated_body = user::Body { a: 4, b: 5, c: 6 };
    let updated_user = user::User::new(updated_body).unwrap();

    collection.update(&id, &updated_user).unwrap();

    let retrieved_updated: user::User = collection.get(&id).unwrap();
    assert_eq!(retrieved_updated.body.a, 4);
    assert_eq!(retrieved_updated.body.b, 5);
    assert_eq!(retrieved_updated.body.c, 6);
    assert_eq!(retrieved_updated, updated_user);
}

#[test]
fn test_nat_db_operations() {
    let db_path = format!("{}/test_nat_db_operations", TEST_DATA);
    let db = Database::new(Some(&db_path)).expect("Failed to open database");
    let collection = db.collection("nat").expect("Failed to create collection");

    let _cleanup = CleanupDir(db_path.clone());

    let zero = nat::Nat::zero().unwrap();
    let one = nat::Nat::suc(nat::Nat::zero().unwrap()).unwrap();
    let two = nat::Nat::suc(nat::Nat::suc(nat::Nat::zero().unwrap()).unwrap()).unwrap();

    let zero_id = collection.insert(&zero).unwrap();
    let one_id = collection.insert(&one).unwrap();
    let two_id = collection.insert(&two).unwrap();

    let retrieved_zero: nat::Nat = collection.get(&zero_id).unwrap();
    let retrieved_one: nat::Nat = collection.get(&one_id).unwrap();
    let retrieved_two: nat::Nat = collection.get(&two_id).unwrap();

    match &retrieved_zero.body {
        nat::Body::Zero => {}
        _ => panic!("Expected Zero"),
    }

    match &retrieved_one.body {
        nat::Body::Suc { pred } => match &**pred {
            nat::Body::Zero => {}
            _ => panic!("Expected Suc(Zero)"),
        },
        _ => panic!("Expected Suc"),
    }

    match &retrieved_two.body {
        nat::Body::Suc { pred } => match &**pred {
            nat::Body::Suc { pred: inner_pred } => match &**inner_pred {
                nat::Body::Zero => {}
                _ => panic!("Expected Suc(Suc(Zero))"),
            },
            _ => panic!("Expected Suc(Suc(_))"),
        },
        _ => panic!("Expected Suc"),
    }

    collection.update(&one_id, &two).unwrap();
    let updated_one: nat::Nat = collection.get(&one_id).unwrap();

    match &updated_one.body {
        nat::Body::Suc { pred } => match &**pred {
            nat::Body::Suc { pred: inner_pred } => match &**inner_pred {
                nat::Body::Zero => {}
                _ => panic!("Expected Suc(Suc(Zero)) after update"),
            },
            _ => panic!("Expected Suc(Suc(_)) after update"),
        },
        _ => panic!("Expected Suc after update"),
    }

    collection.delete(&zero_id).unwrap();
    let result = collection.get::<nat::Nat>(&zero_id);
    assert!(result.is_err());
}

#[test]
fn test_sum_subcollection() {
    let db_path = format!("{}/test_sum_subcollection", TEST_DATA);
    let db = Database::new(Some(&db_path)).expect("Failed to open database");
    let collection = db.collection("sum_subcol").expect("Failed to create collection");
    
    let _cleanup = CleanupDir(db_path.clone());
    
    let deps42 = sum::Dependencies { a: 42 };
    let deps100 = sum::Dependencies { a: 100 };
    
    let subcollection42 = collection.subcollection(&deps42)
        .expect("Failed to create subcollection for deps42");
        
    let subcollection100 = collection.subcollection(&deps100)
        .expect("Failed to create subcollection for deps100");
    
    let body1 = sum::Body {};
    let body2 = sum::Body {};
    
    let id1 = subcollection42.insert(&body1).expect("Failed to insert body1");
    let id2 = subcollection100.insert(&body2).expect("Failed to insert body2");
    
    let retrieved1: sum::Body = subcollection42.get(&id1).expect("Failed to get body1");
    let retrieved2: sum::Body = subcollection100.get(&id2).expect("Failed to get body2");
    
    assert_eq!(retrieved1, body1);
    assert_eq!(retrieved2, body2);
    
    let wrong_result = subcollection42.get::<sum::Body>(&id2);
    assert!(wrong_result.is_err());
    
    let keys42 = subcollection42.get_keys().expect("Failed to get keys from subcollection42");
    let keys100 = subcollection100.get_keys().expect("Failed to get keys from subcollection100");
    
    assert_eq!(keys42.len(), 1);
    assert_eq!(keys100.len(), 1);
    assert!(keys42.contains(&id1));
    assert!(keys100.contains(&id2));
    
    let full_message1: sum::Sum = collection.get(&id1).expect("Failed to get full message1");
    let full_message2: sum::Sum = collection.get(&id2).expect("Failed to get full message2");
    
    assert_eq!(full_message1.dependencies.a, 42);
    assert_eq!(full_message2.dependencies.a, 100);
    
    subcollection42.delete(&id1).expect("Failed to delete body1");
    
    let subcol_result = subcollection42.get::<sum::Body>(&id1);
    let col_result = collection.get::<sum::Sum>(&id1);
    
    assert!(subcol_result.is_err());
    assert!(col_result.is_err());
}

#[test]
fn test_user_subcollection() {
    let db_path = format!("{}/test_user_subcollection", TEST_DATA);
    let db = Database::new(Some(&db_path)).expect("Failed to open database");
    let collection = db.collection("user_subcol").expect("Failed to create collection");
    
    let _cleanup = CleanupDir(db_path.clone());
    
    let deps = user::Dependencies {};
    
    let users_subcollection = collection.subcollection(&deps)
        .expect("Failed to create users subcollection");
    
    let user1_body = user::Body { a: 1, b: 2, c: 3 };
    let user2_body = user::Body { a: 4, b: 5, c: 6 };
    let user3_body = user::Body { a: 7, b: 8, c: 9 };
    
    let id1 = users_subcollection.insert(&user1_body).expect("Failed to insert user1");
    let id2 = users_subcollection.insert(&user2_body).expect("Failed to insert user2");
    let id3 = users_subcollection.insert(&user3_body).expect("Failed to insert user3");
    
    let retrieved1: user::Body = users_subcollection.get(&id1).expect("Failed to get user1");
    let retrieved2: user::Body = users_subcollection.get(&id2).expect("Failed to get user2");
    let retrieved3: user::Body = users_subcollection.get(&id3).expect("Failed to get user3");
    
    assert_eq!(retrieved1, user1_body);
    assert_eq!(retrieved2, user2_body);
    assert_eq!(retrieved3, user3_body);
    
    let keys = users_subcollection.get_keys().expect("Failed to get keys");
    assert_eq!(keys.len(), 3);
    assert!(keys.contains(&id1) && keys.contains(&id2) && keys.contains(&id3));
    
    let full_user1: user::User = collection.get(&id1).expect("Failed to get full user1");
    let full_user2: user::User = collection.get(&id2).expect("Failed to get full user2");
    
    assert_eq!(full_user1.body, user1_body);
    assert_eq!(full_user2.body, user2_body);
    
    let updated_body = user::Body { a: 10, b: 11, c: 12 };
    users_subcollection.update(&id1, &updated_body).expect("Failed to update user1");
    
    let updated_retrieved: user::Body = users_subcollection.get(&id1).expect("Failed to get updated user");
    assert_eq!(updated_retrieved, updated_body);
    
    users_subcollection.delete(&id2).expect("Failed to delete user2");
    
    let delete_result = users_subcollection.get::<user::Body>(&id2);
    assert!(delete_result.is_err());
    
    let updated_keys = users_subcollection.get_keys().expect("Failed to get updated keys");
    assert_eq!(updated_keys.len(), 2);
    assert!(updated_keys.contains(&id1) && updated_keys.contains(&id3));
    assert!(!updated_keys.contains(&id2));
}
