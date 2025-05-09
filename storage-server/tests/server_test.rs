// tests/api_tests.rs
use reqwest;
use serde_json::{json, Value};
use std::fs;
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use tokio::time;
use uuid::Uuid;

fn setup_test_dir() -> String {
    let test_dir = format!("./test_db_{}", Uuid::new_v4());
    if Path::new(&test_dir).exists() {
        fs::remove_dir_all(&test_dir).unwrap();
    }
    fs::create_dir_all(&test_dir).unwrap();
    test_dir
}

fn cleanup_test_dir(dir: &str) {
    if Path::new(dir).exists() {
        fs::remove_dir_all(dir).unwrap();
    }
}

async fn start_test_server(db_path: &str, port: u16) -> std::process::Child {
    let child = Command::new("cargo")
        .arg("run")
        .arg(format!("--manifest-path={}", "../Cargo.toml"))
        .arg("--")
        .arg("--db-path")
        .arg(db_path)
        .arg("--bind-address")
        .arg(format!("127.0.0.1:{}", port))
        .spawn()
        .expect("Failed to start server");

    time::sleep(Duration::from_secs(2)).await;

    child
}

#[tokio::test]
async fn test_collection_crud() {
    let test_dir = setup_test_dir();
    let port = 8081;

    let mut server = start_test_server(&test_dir, port).await;

    let client = reqwest::Client::new();
    let base_url = format!("http://127.0.0.1:{}", port);

    let response = client
        .post(&format!("{}/collections", base_url))
        .json(&json!({
            "name": "test_collection"
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let json: Value = response.json().await.unwrap();
    assert_eq!(json["success"], true);

    let response = client
        .get(&format!("{}/collections", base_url))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let json: Value = response.json().await.unwrap();
    assert_eq!(json["success"], true);
    assert!(json["data"]
        .as_array()
        .unwrap()
        .iter()
        .any(|x| x == "test_collection"));

    let response = client
        .get(&format!("{}/collections/test_collection/exists", base_url))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let json: Value = response.json().await.unwrap();
    assert_eq!(json["success"], true);
    assert_eq!(json["data"], true);

    let response = client
        .delete(&format!("{}/collections", base_url))
        .json(&json!({
            "name": "test_collection"
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let json: Value = response.json().await.unwrap();
    assert_eq!(json["success"], true);

    let response = client
        .get(&format!("{}/collections/test_collection/exists", base_url))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let json: Value = response.json().await.unwrap();
    assert_eq!(json["success"], true);
    assert_eq!(json["data"], false);

    server.kill().unwrap();
    cleanup_test_dir(&test_dir);
}

#[tokio::test]
async fn test_collection_with_schema() {
    let test_dir = setup_test_dir();
    let port = 8082;

    let mut server = start_test_server(&test_dir, port).await;

    let client = reqwest::Client::new();
    let base_url = format!("http://127.0.0.1:{}", port);

    let body_schema = json!({
        "type": "object",
        "properties": {
            "name": { "type": "string" },
            "age": { "type": "integer" }
        },
        "required": ["name"]
    })
    .to_string();

    let deps_schema = json!({
        "type": "object",
        "properties": {
            "user_id": { "type": "string" }
        },
        "required": ["user_id"]
    })
    .to_string();

    let response = client
        .post(&format!("{}/collections/schema", base_url))
        .json(&json!({
            "name": "users",
            "body_schema": body_schema,
            "dependencies_schema": deps_schema
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let json: Value = response.json().await.unwrap();
    assert_eq!(json["success"], true);

    let response = client
        .get(&format!("{}/collections/users", base_url))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let json: Value = response.json().await.unwrap();
    assert_eq!(json["success"], true);
    assert_eq!(json["data"]["has_schema"], true);

    let response = client
        .post(&format!("{}/collections/users", base_url))
        .json(&json!({
            "body": {
                "name": "John Doe",
                "age": 30
            },
            "dependencies": {
                "user_id": "123456"
            }
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let json: Value = response.json().await.unwrap();
    assert_eq!(json["success"], true);
    let id = json["data"]["id"].as_str().unwrap().to_string();

    let response = client
        .post(&format!("{}/collections/users", base_url))
        .json(&json!({
            "body": {
                "age": 30
            },
            "dependencies": {
                "user_id": "123456"
            }
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 400);

    let response = client
        .get(&format!("{}/collections/users/{}", base_url, id))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let json: Value = response.json().await.unwrap();
    assert_eq!(json["success"], true);
    assert_eq!(json["data"]["body"]["name"], "John Doe");
    assert_eq!(json["data"]["body"]["age"], 30);
    assert_eq!(json["data"]["dependencies"]["user_id"], "123456");

    let response = client
        .put(&format!("{}/collections/users/{}", base_url, id))
        .json(&json!({
            "body": {
                "name": "Jane Doe",
                "age": 29
            },
            "dependencies": {
                "user_id": "123456"
            }
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let response = client
        .get(&format!("{}/collections/users/{}", base_url, id))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let json: Value = response.json().await.unwrap();
    assert_eq!(json["data"]["body"]["name"], "Jane Doe");
    assert_eq!(json["data"]["body"]["age"], 29);

    let response = client
        .delete(&format!("{}/collections/users/{}", base_url, id))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let response = client
        .get(&format!("{}/collections/users/{}", base_url, id))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 404);

    server.kill().unwrap();
    cleanup_test_dir(&test_dir);
}

#[tokio::test]
async fn test_batch_operations() {
    let test_dir = setup_test_dir();
    let port = 8083;

    let mut server = start_test_server(&test_dir, port).await;

    let client = reqwest::Client::new();
    let base_url = format!("http://127.0.0.1:{}", port);

    let response = client
        .post(&format!("{}/collections", base_url))
        .json(&json!({
            "name": "batch_test"
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let response = client
        .post(&format!("{}/collections/batch_test/batch", base_url))
        .json(&json!([
            {
                "body": {
                    "item": "Item 1"
                },
                "dependencies": {
                    "category": "test"
                }
            },
            {
                "body": {
                    "item": "Item 2"
                },
                "dependencies": {
                    "category": "test"
                }
            },
            {
                "body": {
                    "item": "Item 3"
                },
                "dependencies": {
                    "category": "test"
                }
            }
        ]))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let json: Value = response.json().await.unwrap();
    assert_eq!(json["success"], true);
    assert_eq!(json["data"]["count"], 3);
    let ids = json["data"]["ids"].as_array().unwrap();
    assert_eq!(ids.len(), 3);

    let response = client
        .get(&format!("{}/collections/batch_test/batch", base_url))
        .json(&json!(ids
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect::<Vec<&str>>()))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let json: Value = response.json().await.unwrap();
    assert_eq!(json["success"], true);
    assert_eq!(json["data"]["found"].as_array().unwrap().len(), 3);

    let response = client
        .delete(&format!("{}/collections/batch_test/batch", base_url))
        .json(&json!(ids
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect::<Vec<&str>>()))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let json: Value = response.json().await.unwrap();
    assert_eq!(json["success"], true);
    assert_eq!(json["data"]["deleted"].as_array().unwrap().len(), 3);

    server.kill().unwrap();
    cleanup_test_dir(&test_dir);
}

#[tokio::test]
async fn test_subcollections() {
    let test_dir = setup_test_dir();
    let port = 8084;

    let mut server = start_test_server(&test_dir, port).await;

    let client = reqwest::Client::new();
    let base_url = format!("http://127.0.0.1:{}", port);

    let response = client
        .post(&format!("{}/collections", base_url))
        .json(&json!({
            "name": "products"
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let response = client
        .post(&format!("{}/subcollections", base_url))
        .json(&json!({
            "collection": "products",
            "dependencies": {
                "category": "electronics"
            }
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let response = client
        .post(&format!(
            "{}/subcollections/products?collection=products&dependencies={}",
            base_url,
            serde_json::to_string(&json!({"category": "electronics"})).unwrap()
        ))
        .json(&json!({
            "name": "Laptop",
            "price": 999.99
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let json: Value = response.json().await.unwrap();
    let id = json["data"]["id"].as_str().unwrap().to_string();

    let response = client
        .get(&format!(
            "{}/subcollections/products/{}?collection=products&dependencies={}",
            base_url,
            id,
            serde_json::to_string(&json!({"category": "electronics"})).unwrap()
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let json: Value = response.json().await.unwrap();
    assert_eq!(json["data"]["name"], "Laptop");
    assert_eq!(json["data"]["price"], 999.99);

    let response = client
        .get(&format!(
            "{}/subcollections/products/keys?collection=products&dependencies={}",
            base_url,
            serde_json::to_string(&json!({"category": "electronics"})).unwrap()
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let json: Value = response.json().await.unwrap();
    assert_eq!(json["data"].as_array().unwrap().len(), 1);
    assert!(json["data"].as_array().unwrap().contains(&json!(id)));

    let response = client
        .put(&format!(
            "{}/subcollections/products/{}?collection=products&dependencies={}",
            base_url,
            id,
            serde_json::to_string(&json!({"category": "electronics"})).unwrap()
        ))
        .json(&json!({
            "name": "Gaming Laptop",
            "price": 1299.99
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let response = client
        .get(&format!(
            "{}/subcollections/products/{}?collection=products&dependencies={}",
            base_url,
            id,
            serde_json::to_string(&json!({"category": "electronics"})).unwrap()
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let json: Value = response.json().await.unwrap();
    assert_eq!(json["data"]["name"], "Gaming Laptop");
    assert_eq!(json["data"]["price"], 1299.99);

    let response = client
        .delete(&format!(
            "{}/subcollections/products/{}?collection=products&dependencies={}",
            base_url,
            id,
            serde_json::to_string(&json!({"category": "electronics"})).unwrap()
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let response = client
        .get(&format!(
            "{}/subcollections/products/{}?collection=products&dependencies={}",
            base_url,
            id,
            serde_json::to_string(&json!({"category": "electronics"})).unwrap()
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 404);

    server.kill().unwrap();
    cleanup_test_dir(&test_dir);
}

#[tokio::test]
async fn test_error_handling() {
    let test_dir = setup_test_dir();
    let port = 8085;

    let mut server = start_test_server(&test_dir, port).await;

    let client = reqwest::Client::new();
    let base_url = format!("http://127.0.0.1:{}", port);

    let response = client
        .get(&format!("{}/collections/nonexistent", base_url))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 404);
    let json: Value = response.json().await.unwrap();
    assert_eq!(json["success"], false);
    assert!(json["error"].as_str().unwrap().contains("not found"));

    let body_schema = json!({
        "type": "object",
        "properties": {
            "email": {
                "type": "string",
                "format": "email"
            }
        },
        "required": ["email"]
    })
    .to_string();

    let deps_schema = json!({
        "type": "object",
        "properties": {
            "user_type": {
                "type": "string",
                "enum": ["admin", "user", "guest"]
            }
        },
        "required": ["user_type"]
    })
    .to_string();

    let response = client
        .post(&format!("{}/collections/schema", base_url))
        .json(&json!({
            "name": "users_with_validation",
            "body_schema": body_schema,
            "dependencies_schema": deps_schema
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let response = client
        .post(&format!("{}/collections/users_with_validation", base_url))
        .json(&json!({
            "body": {
                "email": "not-an-email"
            },
            "dependencies": {
                "user_type": "admin"
            }
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 400);
    let json: Value = response.json().await.unwrap();
    assert_eq!(json["success"], false);

    let response = client
        .post(&format!("{}/collections/users_with_validation", base_url))
        .json(&json!({
            "body": {
                "email": "test@example.com"
            },
            "dependencies": {
                "user_type": "not-a-valid-type"
            }
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 400);
    let json: Value = response.json().await.unwrap();
    assert_eq!(json["success"], false);

    server.kill().unwrap();
    cleanup_test_dir(&test_dir);
}
