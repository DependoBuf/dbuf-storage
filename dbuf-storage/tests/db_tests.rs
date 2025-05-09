use dbuf_storage::{Database, DbError};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::fs;
use std::path::Path;
use tempfile::tempdir;

#[derive(Debug, Serialize, Deserialize)]
pub enum ConstructorError {
    MismatchedDependencies,
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, schemars::JsonSchema)]
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

    #[derive(PartialEq, Eq, Debug, Serialize, Deserialize, schemars::JsonSchema)]
    pub struct Body {}

    #[derive(PartialEq, Eq, Debug, Serialize, Deserialize, schemars::JsonSchema)]
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

    #[derive(PartialEq, Eq, Debug, Serialize, Deserialize, schemars::JsonSchema)]
    pub struct Body {
        pub sum: deps::Sum,
    }

    #[derive(PartialEq, Eq, Debug, Serialize, Deserialize, schemars::JsonSchema)]
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

    #[derive(PartialEq, Eq, Debug, Serialize, Deserialize, schemars::JsonSchema)]
    pub struct Body {
        pub a: i32,
        pub b: i32,
        pub c: i32,
    }

    #[derive(PartialEq, Eq, Debug, Serialize, Deserialize, schemars::JsonSchema)]
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
    #[derive(PartialEq, Eq, Debug, Serialize, Deserialize, schemars::JsonSchema)]
    pub enum Body {
        Zero,
        Suc { pred: Box<Self> },
    }

    // dependencies definition
    #[derive(PartialEq, Eq, Debug, Serialize, Deserialize, schemars::JsonSchema)]
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

#[test]
fn test_database_creation() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().to_str().unwrap();

    let _ = Database::new(Some(db_path)).expect("Failed to open database");
    assert!(Path::new(db_path).exists());

    let _ = Database::new(None).expect("Failed to open database with default path");
    assert!(Path::new("./dbuf_db").exists());

    let _ = fs::remove_dir_all("./dbuf_db");
}

#[test]
fn test_collection_management() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().to_str().unwrap();
    let db = Database::new(Some(db_path)).expect("Failed to open database");

    let coll1 = db
        .create_collection("test_collection")
        .expect("Failed to create collection");
    assert_eq!(coll1.get_name(), "test_collection");

    assert!(db.collection_exists("test_collection").unwrap());
    assert!(!db.collection_exists("nonexistent").unwrap());

    let collections = db.list_collections();
    assert!(collections.contains(&"test_collection".to_string()));

    let existing = db
        .get_collection("test_collection")
        .expect("Failed to get existing collection");
    assert_eq!(existing.get_name(), "test_collection");

    let nonexistent = db.get_collection("nonexistent");
    assert!(matches!(nonexistent, Err(DbError::NotFound)));

    db.drop_collection("test_collection")
        .expect("Failed to drop collection");
    assert!(!db.collection_exists("test_collection").unwrap());
}

#[test]
fn test_create_collection_with_schema() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().to_str().unwrap();
    let db = Database::new(Some(db_path)).expect("Failed to open database");

    let coll = db
        .create_collection_with_schema::<sum::Body, sum::Dependencies>("test_schema_collection")
        .expect("Failed to create collection with schema");

    assert!(coll.has_schema());
    assert!(coll.get_body_schema_json().is_some());
    assert!(coll.get_dependencies_schema_json().is_some());

    let body_schema = r#"{"type":"object","properties":{},"additionalProperties":false}"#;
    let deps_schema = r#"{"type":"object","properties":{"a":{"type":"integer"}},"required":["a"],"additionalProperties":false}"#;

    let coll_json = db
        .create_collection_with_schema_json("test_json_schema", body_schema, deps_schema)
        .expect("Failed to create collection with JSON schema");

    assert!(coll_json.has_schema());
    assert_eq!(coll_json.get_body_schema_json().unwrap(), body_schema);
    assert_eq!(
        coll_json.get_dependencies_schema_json().unwrap(),
        deps_schema
    );

    let duplicate = db.create_collection("test_schema_collection");
    assert!(matches!(duplicate, Err(DbError::AlreadyExists(_))));
}

#[test]
fn test_basic_crud_operations() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().to_str().unwrap();
    let db = Database::new(Some(db_path)).expect("Failed to open database");
    let collection = db
        .create_collection("test_crud")
        .expect("Failed to create collection");

    let sum_deps = sum::Dependencies { a: 42 };
    let sum = sum::Sum::new(sum_deps).unwrap();

    let id = collection.insert(&sum).expect("Failed to insert item");
    assert!(!id.is_empty());

    let retrieved: sum::Sum = collection.get(&id).expect("Failed to get item");
    assert_eq!(retrieved, sum);

    let updated_deps = sum::Dependencies { a: 100 };
    let updated_sum = sum::Sum::new(updated_deps).unwrap();

    collection
        .update(&id, &updated_sum)
        .expect("Failed to update item");

    let retrieved_after_update: sum::Sum = collection.get(&id).expect("Failed to get updated item");
    assert_eq!(retrieved_after_update, updated_sum);
    assert_ne!(retrieved, retrieved_after_update);

    collection.delete(&id).expect("Failed to delete item");

    let get_after_delete = collection.get::<sum::Sum>(&id);
    assert!(matches!(get_after_delete, Err(DbError::NotFound)));
}

#[test]
fn test_collection_sum() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().to_str().unwrap();
    let db = Database::new(Some(db_path)).expect("Failed to open database");
    let collection = db
        .create_collection("test_complex")
        .expect("Failed to create collection");

    let foo_deps = foo::Dependencies { a: 10, b: 20 };
    let foo = foo::Foo::new(foo_deps).unwrap();

    let id = collection.insert(&foo).expect("Failed to insert foo");

    let retrieved: foo::Foo = collection.get(&id).expect("Failed to get foo");
    assert_eq!(retrieved.dependencies.a, 10);
    assert_eq!(retrieved.dependencies.b, 20);
    assert_eq!(retrieved.body.sum.dependencies.a, 10); // -a + b = -10 + 20 = 10

    let zero = nat::Nat::zero().unwrap();
    let one = nat::Nat::suc(nat::Nat::zero().unwrap()).unwrap();
    let two = nat::Nat::suc(nat::Nat::suc(nat::Nat::zero().unwrap()).unwrap()).unwrap();

    let zero_id = collection.insert(&zero).expect("Failed to insert zero");
    let one_id = collection.insert(&one).expect("Failed to insert one");
    let two_id = collection.insert(&two).expect("Failed to insert two");

    let retrieved_zero: nat::Nat = collection.get(&zero_id).expect("Failed to get zero");
    let retrieved_one: nat::Nat = collection.get(&one_id).expect("Failed to get one");
    let retrieved_two: nat::Nat = collection.get(&two_id).expect("Failed to get two");

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
}

#[test]
fn test_json_operations() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().to_str().unwrap();
    let db = Database::new(Some(db_path)).expect("Failed to open database");
    let collection = db
        .create_collection("test_json")
        .expect("Failed to create collection");

    let json_data = r#"{"body":{"a":1,"b":2,"c":3},"dependencies":{}}"#;
    let id = collection
        .insert_json(json_data.to_string())
        .expect("Failed to insert JSON");

    let retrieved_json = collection.get_json(&id).expect("Failed to get JSON");
    let retrieved_value: Value = serde_json::from_str(&retrieved_json).unwrap();

    assert_eq!(retrieved_value["body"]["a"], json!(1));
    assert_eq!(retrieved_value["body"]["b"], json!(2));
    assert_eq!(retrieved_value["body"]["c"], json!(3));

    let updated_json = r#"{"body":{"a":10,"b":20,"c":30},"dependencies":{}}"#;
    collection
        .update_json(&id, updated_json.to_string())
        .expect("Failed to update JSON");

    let retrieved_after_update = collection
        .get_json(&id)
        .expect("Failed to get JSON after update");
    let updated_value: Value = serde_json::from_str(&retrieved_after_update).unwrap();

    assert_eq!(updated_value["body"]["a"], json!(10));
    assert_eq!(updated_value["body"]["b"], json!(20));
    assert_eq!(updated_value["body"]["c"], json!(30));
}

#[test]
fn test_error_handling() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().to_str().unwrap();
    let db = Database::new(Some(db_path)).expect("Failed to open database");
    let collection = db
        .create_collection("test_errors")
        .expect("Failed to create collection");

    let nonexistent = collection.get::<sum::Sum>("nonexistent_id");
    assert!(matches!(nonexistent, Err(DbError::NotFound)));

    let sum_deps = sum::Dependencies { a: 42 };
    let sum = sum::Sum::new(sum_deps).unwrap();
    let update_nonexistent = collection.update("nonexistent_id", &sum);
    assert!(matches!(update_nonexistent, Err(DbError::NotFound)));

    let delete_nonexistent = collection.delete("nonexistent_id");
    assert!(matches!(delete_nonexistent, Err(DbError::NotFound)));

    let invalid_json = "{invalid json}";
    let insert_invalid = collection.insert_json(invalid_json.to_string());
    assert!(matches!(
        insert_invalid,
        Err(DbError::DeserializationError(_))
    ));

    let missing_fields = r#"{"only_body":{}}"#;
    let insert_invalid_struct = collection.insert_json(missing_fields.to_string());
    assert!(matches!(
        insert_invalid_struct,
        Err(DbError::DeserializationError(_))
    ));
}

#[test]
fn test_subcollection_operations() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().to_str().unwrap();
    let db = Database::new(Some(db_path)).expect("Failed to open database");
    let collection = db
        .create_collection("test_subcollection")
        .expect("Failed to create collection");

    let deps42 = sum::Dependencies { a: 42 };
    let deps100 = sum::Dependencies { a: 100 };

    let subcollection42 = collection
        .subcollection(&deps42)
        .expect("Failed to create subcollection42");
    let subcollection100 = collection
        .subcollection(&deps100)
        .expect("Failed to create subcollection100");

    let body1 = sum::Body {};
    let body2 = sum::Body {};

    let id1 = subcollection42
        .insert(&body1)
        .expect("Failed to insert body1");
    let id2 = subcollection100
        .insert(&body2)
        .expect("Failed to insert body2");

    let retrieved1: sum::Body = subcollection42.get(&id1).expect("Failed to get body1");
    let retrieved2: sum::Body = subcollection100.get(&id2).expect("Failed to get body2");

    assert_eq!(retrieved1, body1);
    assert_eq!(retrieved2, body2);

    let wrong_result = subcollection42.get::<sum::Body>(&id2);
    assert!(matches!(wrong_result, Err(DbError::NotFound)));

    let full_message1: sum::Sum = collection.get(&id1).expect("Failed to get full message1");
    let full_message2: sum::Sum = collection.get(&id2).expect("Failed to get full message2");

    assert_eq!(full_message1.dependencies.a, 42);
    assert_eq!(full_message2.dependencies.a, 100);

    let keys42 = subcollection42
        .get_keys()
        .expect("Failed to get keys from subcollection42");
    let keys100 = subcollection100
        .get_keys()
        .expect("Failed to get keys from subcollection100");

    assert_eq!(keys42.len(), 1);
    assert_eq!(keys100.len(), 1);
    assert!(keys42.contains(&id1));
    assert!(keys100.contains(&id2));

    subcollection42
        .update(&id1, &body2)
        .expect("Failed to update in subcollection");

    subcollection42
        .delete(&id1)
        .expect("Failed to delete from subcollection");

    let subcol_result = subcollection42.get::<sum::Body>(&id1);
    let col_result = collection.get::<sum::Sum>(&id1);

    assert!(matches!(subcol_result, Err(DbError::NotFound)));
    assert!(matches!(col_result, Err(DbError::NotFound)));
}

#[test]
fn test_subcollection_json_operations() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().to_str().unwrap();
    let db = Database::new(Some(db_path)).expect("Failed to open database");
    let collection = db
        .create_collection("test_subcollection_json")
        .expect("Failed to create collection");

    let deps_json = r#"{"a":42}"#;
    let subcollection = collection
        .subcollection_json(deps_json.to_string())
        .expect("Failed to create subcollection from JSON");

    let body_json = r#"{}"#;
    let id = subcollection
        .insert_json(body_json.to_string())
        .expect("Failed to insert body JSON");

    let retrieved_json = subcollection
        .get_json(&id)
        .expect("Failed to get body JSON");
    let retrieved_value: Value = serde_json::from_str(&retrieved_json).unwrap();

    assert_eq!(retrieved_value, json!({}));

    subcollection
        .update_json(&id, body_json.to_string())
        .expect("Failed to update body JSON");

    subcollection
        .delete_json(&id)
        .expect("Failed to delete body JSON");

    let result = subcollection.get_json(&id);
    assert!(matches!(result, Err(DbError::NotFound)));
}

#[test]
fn test_multiple_items_in_subcollection() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().to_str().unwrap();
    let db = Database::new(Some(db_path)).expect("Failed to open database");
    let collection = db
        .create_collection("test_multi_subcollection")
        .expect("Failed to create collection");

    let deps = user::Dependencies {};
    let users_subcollection = collection
        .subcollection(&deps)
        .expect("Failed to create users subcollection");

    let user1_body = user::Body { a: 1, b: 2, c: 3 };
    let user2_body = user::Body { a: 4, b: 5, c: 6 };
    let user3_body = user::Body { a: 7, b: 8, c: 9 };

    let id1 = users_subcollection
        .insert(&user1_body)
        .expect("Failed to insert user1");
    let id2 = users_subcollection
        .insert(&user2_body)
        .expect("Failed to insert user2");
    let id3 = users_subcollection
        .insert(&user3_body)
        .expect("Failed to insert user3");

    let retrieved1: user::Body = users_subcollection.get(&id1).expect("Failed to get user1");
    let retrieved2: user::Body = users_subcollection.get(&id2).expect("Failed to get user2");
    let retrieved3: user::Body = users_subcollection.get(&id3).expect("Failed to get user3");

    assert_eq!(retrieved1, user1_body);
    assert_eq!(retrieved2, user2_body);
    assert_eq!(retrieved3, user3_body);

    let keys = users_subcollection.get_keys().expect("Failed to get keys");
    assert_eq!(keys.len(), 3);
    assert!(keys.contains(&id1) && keys.contains(&id2) && keys.contains(&id3));

    let updated_body = user::Body {
        a: 10,
        b: 11,
        c: 12,
    };
    users_subcollection
        .update(&id1, &updated_body)
        .expect("Failed to update user1");

    let updated_retrieved: user::Body = users_subcollection
        .get(&id1)
        .expect("Failed to get updated user");
    assert_eq!(updated_retrieved, updated_body);

    users_subcollection
        .delete(&id2)
        .expect("Failed to delete user2");

    let delete_result = users_subcollection.get::<user::Body>(&id2);
    assert!(matches!(delete_result, Err(DbError::NotFound)));

    let updated_keys = users_subcollection
        .get_keys()
        .expect("Failed to get updated keys");
    assert_eq!(updated_keys.len(), 2);
    assert!(updated_keys.contains(&id1) && updated_keys.contains(&id3));
    assert!(!updated_keys.contains(&id2));
}

#[test]
fn test_schema_validation() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().to_str().unwrap();
    let db = Database::new(Some(db_path)).expect("Failed to open database");

    let body_schema = r#"{
        "type": "object",
        "properties": {
            "name": { "type": "string" },
            "age": { "type": "integer", "minimum": 0 }
        },
        "required": ["name", "age"],
        "additionalProperties": false
    }"#;

    let deps_schema = r#"{
        "type": "object",
        "properties": {
            "id": { "type": "string" }
        },
        "required": ["id"],
        "additionalProperties": false
    }"#;

    let collection = db
        .create_collection_with_schema_json("test_schema_validation", body_schema, deps_schema)
        .expect("Failed to create collection with schema");

    let valid_json = r#"{
        "body": { "name": "John", "age": 30 },
        "dependencies": { "id": "123" }
    }"#;

    let id = collection
        .insert_json(valid_json.to_string())
        .expect("Failed to insert valid data");

    let invalid_body_type = r#"{
        "body": { "name": "John", "age": "thirty" },
        "dependencies": { "id": "123" }
    }"#;

    let result = collection.insert_json(invalid_body_type.to_string());
    assert!(matches!(result, Err(DbError::SchemaValidationError(_))));

    let invalid_body_missing = r#"{
        "body": { "age": 30 },
        "dependencies": { "id": "123" }
    }"#;

    let result = collection.insert_json(invalid_body_missing.to_string());
    assert!(matches!(result, Err(DbError::SchemaValidationError(_))));

    let invalid_body_additional = r#"{
        "body": { "name": "John", "age": 30, "extra": true },
        "dependencies": { "id": "123" }
    }"#;

    let result = collection.insert_json(invalid_body_additional.to_string());
    assert!(matches!(result, Err(DbError::SchemaValidationError(_))));

    let invalid_deps = r#"{
        "body": { "name": "John", "age": 30 },
        "dependencies": { "id": 123 }
    }"#;

    let result = collection.insert_json(invalid_deps.to_string());
    assert!(matches!(result, Err(DbError::SchemaValidationError(_))));

    let update_invalid = r#"{
        "body": { "name": "John", "age": -5 },
        "dependencies": { "id": "123" }
    }"#;

    let result = collection.update_json(&id, update_invalid.to_string());
    assert!(matches!(result, Err(DbError::SchemaValidationError(_))));
}

#[test]
fn test_schema_compilation_errors() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().to_str().unwrap();
    let db = Database::new(Some(db_path)).expect("Failed to open database");

    let invalid_schema = r#"{ "type": "object", "properties": { unclosed_object }"#;
    let valid_schema = r#"{ "type": "object" }"#;

    let result =
        db.create_collection_with_schema_json("test_invalid_schema", invalid_schema, valid_schema);

    assert!(matches!(result, Err(DbError::DeserializationError(_))));

    let invalid_schema_semantic = r#"{ "type": "unknown_type" }"#;

    let result = db.create_collection_with_schema_json(
        "test_invalid_schema_semantic",
        invalid_schema_semantic,
        valid_schema,
    );

    assert!(matches!(result, Err(DbError::SchemaCompilationError(_))));
}

#[test]
fn test_sum_db_operations() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().to_str().unwrap();
    let db = Database::new(Some(db_path)).expect("Failed to open database");
    let collection = db
        .create_collection("sum")
        .expect("Failed to create collection");

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
    let retrieved_updated: sum::Sum = collection.get(&id).unwrap();
    assert_eq!(retrieved_updated.dependencies.a, 100);
    assert_eq!(retrieved_updated, updated_sum);
}

#[test]
fn test_foo_db_operations() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().to_str().unwrap();
    let db = Database::new(Some(db_path)).expect("Failed to open database");
    let collection = db
        .create_collection("foo")
        .expect("Failed to create collection");

    let foo_deps = foo::Dependencies { a: 10, b: 20 };
    let foo = foo::Foo::new(foo_deps).unwrap();

    let id = collection.insert(&foo).unwrap();

    let retrieved: foo::Foo = collection.get(&id).unwrap();

    assert_eq!(retrieved.dependencies.a, 10);
    assert_eq!(retrieved.dependencies.b, 20);
    assert_eq!(retrieved, foo);
    assert_eq!(retrieved.body.sum.dependencies.a, 10); // -a + b = -10 + 20 = 10

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
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().to_str().unwrap();
    let db = Database::new(Some(db_path)).expect("Failed to open database");
    let collection = db
        .create_collection("user")
        .expect("Failed to create collection");

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
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().to_str().unwrap();
    let db = Database::new(Some(db_path)).expect("Failed to open database");
    let collection = db
        .create_collection("nat")
        .expect("Failed to create collection");

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
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().to_str().unwrap();
    let db = Database::new(Some(db_path)).expect("Failed to open database");
    let collection = db
        .create_collection("sum_subcol")
        .expect("Failed to create collection");

    let deps42 = sum::Dependencies { a: 42 };
    let deps100 = sum::Dependencies { a: 100 };

    let subcollection42 = collection
        .subcollection(&deps42)
        .expect("Failed to create subcollection for deps42");

    let subcollection100 = collection
        .subcollection(&deps100)
        .expect("Failed to create subcollection for deps100");

    let body1 = sum::Body {};
    let body2 = sum::Body {};

    let id1 = subcollection42
        .insert(&body1)
        .expect("Failed to insert body1");
    let id2 = subcollection100
        .insert(&body2)
        .expect("Failed to insert body2");

    let retrieved1: sum::Body = subcollection42.get(&id1).expect("Failed to get body1");
    let retrieved2: sum::Body = subcollection100.get(&id2).expect("Failed to get body2");

    assert_eq!(retrieved1, body1);
    assert_eq!(retrieved2, body2);

    let wrong_result = subcollection42.get::<sum::Body>(&id2);
    assert!(wrong_result.is_err());

    let keys42 = subcollection42
        .get_keys()
        .expect("Failed to get keys from subcollection42");
    let keys100 = subcollection100
        .get_keys()
        .expect("Failed to get keys from subcollection100");

    assert_eq!(keys42.len(), 1);
    assert_eq!(keys100.len(), 1);
    assert!(keys42.contains(&id1));
    assert!(keys100.contains(&id2));

    let full_message1: sum::Sum = collection.get(&id1).expect("Failed to get full message1");
    let full_message2: sum::Sum = collection.get(&id2).expect("Failed to get full message2");

    assert_eq!(full_message1.dependencies.a, 42);
    assert_eq!(full_message2.dependencies.a, 100);

    subcollection42
        .delete(&id1)
        .expect("Failed to delete body1");

    let subcol_result = subcollection42.get::<sum::Body>(&id1);
    let col_result = collection.get::<sum::Sum>(&id1);

    assert!(subcol_result.is_err());
    assert!(col_result.is_err());
}

#[test]
fn test_user_subcollection() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().to_str().unwrap();
    let db = Database::new(Some(db_path)).expect("Failed to open database");
    let collection = db
        .create_collection("user_subcol")
        .expect("Failed to create collection");

    let deps = user::Dependencies {};

    let users_subcollection = collection
        .subcollection(&deps)
        .expect("Failed to create users subcollection");

    let user1_body = user::Body { a: 1, b: 2, c: 3 };
    let user2_body = user::Body { a: 4, b: 5, c: 6 };
    let user3_body = user::Body { a: 7, b: 8, c: 9 };

    let id1 = users_subcollection
        .insert(&user1_body)
        .expect("Failed to insert user1");
    let id2 = users_subcollection
        .insert(&user2_body)
        .expect("Failed to insert user2");
    let id3 = users_subcollection
        .insert(&user3_body)
        .expect("Failed to insert user3");

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

    let updated_body = user::Body {
        a: 10,
        b: 11,
        c: 12,
    };
    users_subcollection
        .update(&id1, &updated_body)
        .expect("Failed to update user1");

    let updated_retrieved: user::Body = users_subcollection
        .get(&id1)
        .expect("Failed to get updated user");
    assert_eq!(updated_retrieved, updated_body);

    users_subcollection
        .delete(&id2)
        .expect("Failed to delete user2");

    let delete_result = users_subcollection.get::<user::Body>(&id2);
    assert!(delete_result.is_err());

    let updated_keys = users_subcollection
        .get_keys()
        .expect("Failed to get updated keys");
    assert_eq!(updated_keys.len(), 2);
    assert!(updated_keys.contains(&id1) && updated_keys.contains(&id3));
    assert!(!updated_keys.contains(&id2));
}

#[test]
fn test_schema_consistency() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().to_str().unwrap();

    let body_schema = r#"{"type":"object","properties":{"name":{"type":"string"}},"required":["name"],"additionalProperties":false}"#;
    let deps_schema = r#"{"type":"object","properties":{"id":{"type":"string"}},"required":["id"],"additionalProperties":false}"#;

    {
        let db = Database::new(Some(db_path)).expect("Failed to open database");

        db.create_collection_with_schema_json("test_schema", body_schema, deps_schema)
            .expect("Failed to create collection with schema");
    }

    let db2 = Database::new(Some(db_path)).expect("Failed to open database again");
    let collection = db2
        .get_collection("test_schema")
        .expect("Failed to get collection");

    assert!(collection.has_schema());

    let body_schema_from_db = collection.get_body_schema_json().unwrap();
    let deps_schema_from_db = collection.get_dependencies_schema_json().unwrap();

    let body_value: Value = serde_json::from_str(body_schema_from_db).unwrap();
    let deps_value: Value = serde_json::from_str(deps_schema_from_db).unwrap();

    assert_eq!(body_value["type"], "object");
    assert_eq!(body_value["required"][0], "name");
    assert_eq!(body_value["additionalProperties"], false);

    assert_eq!(deps_value["type"], "object");
    assert_eq!(deps_value["required"][0], "id");
    assert_eq!(deps_value["additionalProperties"], false);

    let valid_json = r#"{"body":{"name":"Test"},"dependencies":{"id":"123"}}"#;
    let result = collection.insert_json(valid_json.to_string());
    assert!(
        result.is_ok(),
        "Valid data should be accepted: {:?}",
        result
    );

    let invalid_json = r#"{"body":{"name":123},"dependencies":{"id":"123"}}"#;
    let result = collection.insert_json(invalid_json.to_string());
    assert!(matches!(result, Err(DbError::SchemaValidationError(_))));
}
