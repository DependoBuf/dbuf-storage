use jsonschema::{Draft, JSONSchema};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use schemars::{schema_for, JsonSchema};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};
use sled;
use sled::transaction::TransactionError;
use std::error::Error;
use std::fmt;
use std::sync::Arc;

mod helper;
use helper::get_json_hash;

mod schema;
use schema::Schema;

const METADATA_KEY: &str = "*metadata*";

#[derive(Debug)]
pub enum DbError {
    SerializationError(String),
    DeserializationError(String),
    NotFound,
    DatabaseError(String),
    AlreadyExists(String),
    SchemaError(String),
    SchemaValidationError(String),
    SchemaCompilationError(String),
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DbError::SerializationError(msg) => write!(f, "Serialization error: {}", msg),
            DbError::DeserializationError(msg) => write!(f, "Deserialization error: {}", msg),
            DbError::NotFound => write!(f, "Item not found"),
            DbError::DatabaseError(msg) => write!(f, "Database error: {}", msg),
            DbError::AlreadyExists(msg) => write!(f, "Item already exists: {}", msg),
            DbError::SchemaError(msg) => write!(f, "Schema error: {}", msg),
            DbError::SchemaValidationError(msg) => write!(f, "Schema validation error: {}", msg),
            DbError::SchemaCompilationError(msg) => write!(f, "Schema compilation error: {}", msg),
        }
    }
}

impl Error for DbError {}

impl From<sled::Error> for DbError {
    fn from(err: sled::Error) -> Self {
        DbError::DatabaseError(err.to_string())
    }
}

pub struct Collection {
    tree: sled::Tree,
    metadata: CollectionMetadata,
}

pub struct Database {
    db: sled::Db,
}

impl Database {
    pub fn new(path: Option<&str>) -> Result<Self, DbError> {
        let db_path = path.unwrap_or("./dbuf_db");
        let db = sled::open(db_path).map_err(|e| DbError::DatabaseError(e.to_string()))?;

        Ok(Database { db })
    }

    pub fn create_collection(&self, name: &str) -> Result<Collection, DbError> {
        if self
            .db
            .tree_names()
            .iter()
            .any(|tree_name| tree_name == name.as_bytes())
        {
            return Err(DbError::AlreadyExists(format!(
                "Collection {} already exists",
                name
            )));
        }

        let tree = self
            .db
            .open_tree(name.as_bytes())
            .map_err(|e| DbError::DatabaseError(e.to_string()))?;

        let metadata = CollectionMetadata {
            name: name.to_string(),
            body_schema: None,
            dependencies_schema: None,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        };
        let metadata_json = serde_json::to_string(&metadata).map_err(|e| {
            DbError::SerializationError(format!("Failed to serialize metadata: {}", e))
        })?;

        tree.insert(METADATA_KEY.as_bytes(), metadata_json.as_bytes())
            .map_err(|e| DbError::DatabaseError(e.to_string()))?;
        tree.flush()
            .map_err(|e| DbError::DatabaseError(e.to_string()))?;

        Ok(Collection { tree, metadata })
    }

    pub fn create_collection_with_schema<B, D>(&self, name: &str) -> Result<Collection, DbError>
    where
        B: JsonSchema + 'static,
        D: JsonSchema + 'static,
    {
        let body_schema_obj = schema_for!(B);
        let deps_schema_obj = schema_for!(D);

        let body_schema_json = serde_json::to_string(&body_schema_obj).map_err(|e| {
            DbError::SerializationError(format!("Failed to serialize body schema: {}", e))
        })?;

        let deps_schema_json = serde_json::to_string(&deps_schema_obj).map_err(|e| {
            DbError::SerializationError(format!("Failed to serialize dependencies schema: {}", e))
        })?;

        self.create_collection_with_schema_json(name, &body_schema_json, &deps_schema_json)
    }

    pub fn create_collection_with_schema_json(
        &self,
        name: &str,
        body_schema_json: &str,
        deps_schema_json: &str,
    ) -> Result<Collection, DbError> {
        if self
            .db
            .tree_names()
            .iter()
            .any(|tree_name| tree_name == name.as_bytes())
        {
            return Err(DbError::AlreadyExists(format!(
                "Collection {} already exists",
                name
            )));
        }

        let body_schema_value: Value = serde_json::from_str(body_schema_json).map_err(|e| {
            DbError::DeserializationError(format!("Body schema JSON parsing error: {}", e))
        })?;

        let deps_schema_value: Value = serde_json::from_str(deps_schema_json).map_err(|e| {
            DbError::DeserializationError(format!("Dependencies schema JSON parsing error: {}", e))
        })?;

        let compiled_body_schema = JSONSchema::options()
            .with_draft(Draft::Draft7)
            .compile(&body_schema_value)
            .map_err(|e| {
                DbError::SchemaCompilationError(format!("Failed to compile body schema: {}", e))
            })?;

        let compiled_deps_schema = JSONSchema::options()
            .with_draft(Draft::Draft7)
            .compile(&deps_schema_value)
            .map_err(|e| {
                DbError::SchemaCompilationError(format!(
                    "Failed to compile dependencies schema: {}",
                    e
                ))
            })?;

        let body_schema = Schema {
            schema_json: body_schema_json.to_string(),
            schema_hash: crate::helper::get_json_hash(body_schema_json),
            compiled: Some(Arc::new(compiled_body_schema)),
        };

        let deps_schema = Schema {
            schema_json: deps_schema_json.to_string(),
            schema_hash: crate::helper::get_json_hash(deps_schema_json),
            compiled: Some(Arc::new(compiled_deps_schema)),
        };

        let tree = self
            .db
            .open_tree(name.as_bytes())
            .map_err(|e| DbError::DatabaseError(e.to_string()))?;

        let metadata = CollectionMetadata {
            name: name.to_string(),
            body_schema: Some(body_schema),
            dependencies_schema: Some(deps_schema),
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        };

        let metadata_json = serde_json::to_string(&metadata).map_err(|e| {
            DbError::SerializationError(format!("Failed to serialize metadata: {}", e))
        })?;

        tree.insert(METADATA_KEY.as_bytes(), metadata_json.as_bytes())
            .map_err(|e| DbError::DatabaseError(e.to_string()))?;
        tree.flush()
            .map_err(|e| DbError::DatabaseError(e.to_string()))?;

        Ok(Collection { tree, metadata })
    }

    pub fn get_collection(&self, name: &str) -> Result<Collection, DbError> {
        if !self
            .db
            .tree_names()
            .iter()
            .any(|tree_name| tree_name == name.as_bytes())
        {
            return Err(DbError::NotFound);
        }

        let tree = self
            .db
            .open_tree(name.as_bytes())
            .map_err(|e| DbError::DatabaseError(e.to_string()))?;

        let metadata = match tree.get(METADATA_KEY.as_bytes())? {
            Some(metadata_bytes) => {
                let metadata: CollectionMetadata = serde_json::from_slice(&metadata_bytes)
                    .map_err(|e| {
                        DbError::DeserializationError(format!(
                            "Failed to deserialize collection metadata: {}",
                            e
                        ))
                    })?;

                metadata
            }
            None => {
                return Err(DbError::DatabaseError(
                    format!("Collection {} exists but metadata is missing. The collection might be corrupted.", name)
                ));
            }
        };

        let mut collection = Collection { tree, metadata };

        collection.compile_schemas()?;

        Ok(collection)
    }

    pub fn collection_exists(&self, name: &str) -> Result<bool, DbError> {
        let tree_names = self.db.tree_names();
        Ok(tree_names
            .iter()
            .any(|tree_name| tree_name == name.as_bytes()))
    }

    pub fn list_collections(&self) -> Vec<String> {
        self.db
            .tree_names()
            .iter()
            .filter_map(|bytes| String::from_utf8(bytes.to_vec()).ok())
            .collect()
    }

    pub fn drop_collection(&self, name: &str) -> Result<(), DbError> {
        self.db.drop_tree(name.as_bytes())?;
        Ok(())
    }
}

pub struct Subcollection<'a> {
    collection: &'a Collection,
    dependencies: Value,
    dependencies_hash: String,
}

#[derive(Serialize, Deserialize, Clone)]
struct CollectionMetadata {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    body_schema: Option<Schema>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dependencies_schema: Option<Schema>,
    created_at: u64,
}

impl Collection {
    fn generate_id(&self) -> String {
        let rand_string: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(16)
            .map(char::from)
            .collect();

        rand_string
    }

    fn generate_unique_id(&self) -> Result<String, DbError> {
        const MAX_ATTEMPTS: usize = 10;

        for attempt in 0..MAX_ATTEMPTS {
            let id = self.generate_id();

            if !self.tree.contains_key(id.as_bytes())? {
                return Ok(id);
            }

            println!("ID collision detected (attempt {}): {}", attempt + 1, id);
        }

        Err(DbError::DatabaseError(format!(
            "Failed to generate unique ID after {} attempts",
            MAX_ATTEMPTS
        )))
    }

    fn compile_schemas(&mut self) -> Result<(), DbError> {
        if let Some(ref mut body_schema) = self.metadata.body_schema {
            if body_schema.compiled.is_none() {
                let schema_value: Value =
                    serde_json::from_str(&body_schema.schema_json).map_err(|e| {
                        DbError::DeserializationError(format!("Invalid body schema JSON: {}", e))
                    })?;

                let compiled = JSONSchema::options()
                    .with_draft(Draft::Draft7)
                    .compile(&schema_value)
                    .map_err(|e| {
                        DbError::SchemaCompilationError(format!(
                            "Failed to compile body schema: {}",
                            e
                        ))
                    })?;

                body_schema.compiled = Some(Arc::new(compiled));
            }
        }

        if let Some(ref mut deps_schema) = self.metadata.dependencies_schema {
            if deps_schema.compiled.is_none() {
                let schema_value: Value =
                    serde_json::from_str(&deps_schema.schema_json).map_err(|e| {
                        DbError::DeserializationError(format!(
                            "Invalid dependencies schema JSON: {}",
                            e
                        ))
                    })?;

                let compiled = JSONSchema::options()
                    .with_draft(Draft::Draft7)
                    .compile(&schema_value)
                    .map_err(|e| {
                        DbError::SchemaCompilationError(format!(
                            "Failed to compile dependencies schema: {}",
                            e
                        ))
                    })?;

                deps_schema.compiled = Some(Arc::new(compiled));
            }
        }

        Ok(())
    }

    fn validate_body(&self, body: &Value) -> Result<(), DbError> {
        if let Some(ref body_schema) = self.metadata.body_schema {
            return body_schema.validate_data(body);
        }

        Ok(())
    }

    fn validate_dependencies(&self, dependencies: &Value) -> Result<(), DbError> {
        if let Some(ref deps_schema) = self.metadata.dependencies_schema {
            return deps_schema.validate_data(dependencies);
        }

        Ok(())
    }

    pub fn insert<T: Serialize>(&self, value: &T) -> Result<String, DbError> {
        let json =
            serde_json::to_string(value).map_err(|e| DbError::SerializationError(e.to_string()))?;

        self.insert_json(json)
    }

    pub fn insert_json(&self, json: String) -> Result<String, DbError> {
        let value: Value = serde_json::from_str(&json)
            .map_err(|e| DbError::DeserializationError(format!("JSON parsing error: {}", e)))?;

        let obj = match value.as_object() {
            Some(obj) => obj,
            None => {
                return Err(DbError::DeserializationError(
                    "Invalid JSON structure: Not an object".to_string(),
                ))
            }
        };

        if obj.len() != 2 || !obj.contains_key("body") || !obj.contains_key("dependencies") {
            return Err(DbError::DeserializationError(
                "Invalid JSON structure: Message must contains exactly 2 keys: 'body' and 'dependencies'".to_string()
            ));
        }

        let body = &value["body"];
        let dependencies = &value["dependencies"];

        self.validate_body(body)?;
        self.validate_dependencies(dependencies)?;

        let dependencies_json = serde_json::to_string(dependencies).map_err(|e| {
            DbError::SerializationError(format!("Failed to serialize dependencies: {}", e))
        })?;

        let deps_hash = get_json_hash(&dependencies_json);

        let id = self.generate_unique_id()?;

        let storage_value = json!({
            "deps": deps_hash,
            "body": body
        });

        let storage_json = serde_json::to_string(&storage_value).map_err(|e| {
            DbError::SerializationError(format!("Failed to serialize storage value: {}", e))
        })?;

        let deps_exists = self.tree.contains_key(deps_hash.as_bytes())?;

        let result = self.tree.transaction(|tx_tree| {
            tx_tree.insert(id.as_bytes(), storage_json.as_bytes())?;

            if !deps_exists {
                tx_tree.insert(deps_hash.as_bytes(), dependencies_json.as_bytes())?;
            }

            let marker_key = format!("{}_{}", deps_hash, id);
            tx_tree.insert(marker_key.as_bytes(), &[])?;

            Ok(())
        });

        result.map_err(|e: TransactionError| DbError::DatabaseError(e.to_string()))?;

        self.tree.flush()?;

        Ok(id)
    }

    pub fn get<T: DeserializeOwned>(&self, id: &str) -> Result<T, DbError> {
        let json = self.get_json(id)?;

        serde_json::from_str(&json).map_err(|e| DbError::DeserializationError(e.to_string()))
    }

    pub fn get_json(&self, id: &str) -> Result<String, DbError> {
        let item_data = match self.tree.get(id.as_bytes())? {
            Some(data) => data,
            None => return Err(DbError::NotFound),
        };

        let storage_value: Value = serde_json::from_slice(&item_data).map_err(|e| {
            DbError::DeserializationError(format!("Failed to deserialize storage value: {}", e))
        })?;

        if !storage_value.is_object()
            || !storage_value.as_object().unwrap().contains_key("deps")
            || !storage_value.as_object().unwrap().contains_key("body")
        {
            return Err(DbError::DeserializationError(
                "Invalid storage structure: missing 'deps' or 'body'".to_string(),
            ));
        }

        let body = &storage_value["body"];
        let deps_hash = storage_value["deps"]
            .as_str()
            .ok_or_else(|| DbError::DeserializationError("Invalid deps_hash format".to_string()))?;

        let deps_data = match self.tree.get(deps_hash.as_bytes())? {
            Some(data) => data,
            None => {
                return Err(DbError::DeserializationError(format!(
                    "Dependencies with hash {} not found",
                    deps_hash
                )))
            }
        };

        let dependencies: Value = serde_json::from_slice(&deps_data).map_err(|e| {
            DbError::DeserializationError(format!("Failed to deserialize dependencies: {}", e))
        })?;

        let result = json!({
            "body": body,
            "dependencies": dependencies
        });

        serde_json::to_string(&result)
            .map_err(|e| DbError::SerializationError(format!("Failed to serialize result: {}", e)))
    }

    pub fn update<T: Serialize>(&self, id: &str, value: &T) -> Result<(), DbError> {
        let json =
            serde_json::to_string(value).map_err(|e| DbError::SerializationError(e.to_string()))?;

        self.update_json(id, json)?;

        Ok(())
    }

    pub fn update_json(&self, id: &str, json: String) -> Result<(), DbError> {
        let old_item_data = match self.tree.get(id.as_bytes())? {
            Some(data) => data,
            None => return Err(DbError::NotFound),
        };

        let old_storage_value: Value = serde_json::from_slice(&old_item_data).map_err(|e| {
            DbError::DeserializationError(format!("Failed to deserialize old item: {}", e))
        })?;

        if !old_storage_value.is_object()
            || !old_storage_value.as_object().unwrap().contains_key("deps")
            || !old_storage_value.as_object().unwrap().contains_key("body")
        {
            return Err(DbError::DeserializationError(
                "Invalid storage structure: missing 'deps' or 'body'".to_string(),
            ));
        }

        let old_body = &old_storage_value["body"];
        let old_deps_hash = old_storage_value["deps"]
            .as_str()
            .ok_or_else(|| DbError::DeserializationError("Invalid deps_hash format".to_string()))?;

        let new_value: Value = serde_json::from_str(&json)
            .map_err(|e| DbError::DeserializationError(format!("JSON parsing error: {}", e)))?;

        let obj = match new_value.as_object() {
            Some(obj) => obj,
            None => {
                return Err(DbError::DeserializationError(
                    "Invalid JSON structure: Not an object".to_string(),
                ))
            }
        };

        if obj.len() != 2 || !obj.contains_key("body") || !obj.contains_key("dependencies") {
            return Err(DbError::DeserializationError(
                "Invalid JSON structure: Message must contains exactly 2 keys: 'body' and 'dependencies'".to_string()
            ));
        }

        let new_body = &new_value["body"];
        let new_dependencies = &new_value["dependencies"];

        self.validate_body(new_body)?;
        self.validate_dependencies(new_dependencies)?;

        let new_dependencies_json = serde_json::to_string(new_dependencies).map_err(|e| {
            DbError::SerializationError(format!("Failed to serialize dependencies: {}", e))
        })?;

        let new_deps_hash = get_json_hash(&new_dependencies_json);

        let body_changed = serde_json::to_string(old_body)
            .map_err(|e| DbError::SerializationError(e.to_string()))?
            != serde_json::to_string(new_body)
                .map_err(|e| DbError::SerializationError(e.to_string()))?;

        let deps_changed = old_deps_hash != new_deps_hash;

        if !body_changed && !deps_changed {
            return Ok(());
        }

        let mut new_storage_value = old_storage_value.clone();

        if body_changed {
            new_storage_value["body"] = new_body.clone();
        }

        if deps_changed {
            new_storage_value["deps"] = json!(new_deps_hash);
        }

        let updated_storage_json = serde_json::to_string(&new_storage_value).map_err(|e| {
            DbError::SerializationError(format!("Failed to serialize updated item: {}", e))
        })?;

        let new_deps_exists = self.tree.contains_key(new_deps_hash.as_bytes())?;

        let result = self.tree.transaction(|tx_tree| {
            if deps_changed {
                let old_marker_key = format!("{}_{}", old_deps_hash, id);
                tx_tree.remove(old_marker_key.as_bytes())?;

                if !new_deps_exists {
                    tx_tree.insert(new_deps_hash.as_bytes(), new_dependencies_json.as_bytes())?;
                }

                let new_marker_key = format!("{}_{}", new_deps_hash, id);
                tx_tree.insert(new_marker_key.as_bytes(), &[])?;
            }

            tx_tree.insert(id.as_bytes(), updated_storage_json.as_bytes())?;

            Ok(())
        });

        result.map_err(|e: TransactionError| DbError::DatabaseError(e.to_string()))?;

        self.tree.flush()?;
        Ok(())
    }

    pub fn delete(&self, id: &str) -> Result<(), DbError> {
        self.delete_json(id)
    }

    pub fn delete_json(&self, id: &str) -> Result<(), DbError> {
        let item_data = match self.tree.get(id.as_bytes())? {
            Some(data) => data,
            None => return Err(DbError::NotFound),
        };

        let storage_value: Value = serde_json::from_slice(&item_data).map_err(|e| {
            DbError::DeserializationError(format!("Failed to deserialize item: {}", e))
        })?;

        if !storage_value.is_object() || !storage_value.as_object().unwrap().contains_key("deps") {
            return Err(DbError::DeserializationError(
                "Invalid storage structure: missing 'deps'".to_string(),
            ));
        }

        let deps_hash = storage_value["deps"]
            .as_str()
            .ok_or_else(|| DbError::DeserializationError("Invalid deps_hash format".to_string()))?;

        let prefix = format!("{}_", deps_hash);
        let mut items_cnt = 0;
        for result in self.tree.scan_prefix(prefix.as_bytes()) {
            if result.is_ok() {
                items_cnt += 1;
                if items_cnt >= 2 {
                    break;
                }
            }
        }

        let result = self.tree.transaction(|tx_tree| {
            tx_tree.remove(id.as_bytes())?;

            let marker_key = format!("{}_{}", deps_hash, id);
            tx_tree.remove(marker_key.as_bytes())?;

            if items_cnt >= 2 {
                tx_tree.remove(deps_hash.as_bytes())?;
            }

            Ok(())
        });

        result.map_err(|e: TransactionError| DbError::DatabaseError(e.to_string()))?;

        self.tree.flush()?;
        Ok(())
    }

    pub fn has_schema(&self) -> bool {
        self.metadata.body_schema.is_some()
    }

    pub fn get_body_schema_json(&self) -> Option<&str> {
        self.metadata
            .body_schema
            .as_ref()
            .map(|schema| schema.schema_json.as_str())
    }

    pub fn get_dependencies_schema_json(&self) -> Option<&str> {
        self.metadata
            .dependencies_schema
            .as_ref()
            .map(|schema| schema.schema_json.as_str())
    }

    pub fn get_created_at(&self) -> u64 {
        self.metadata.created_at
    }

    pub fn get_name(&self) -> String {
        return self.metadata.name.clone();
    }

    pub fn subcollection<T: Serialize>(&self, dependencies: &T) -> Result<Subcollection, DbError> {
        let deps_json = serde_json::to_string(dependencies).map_err(|e| {
            DbError::SerializationError(format!("Failed to serialize dependencies: {}", e))
        })?;

        self.subcollection_json(deps_json)
    }

    pub fn subcollection_json(&self, dependencies_json: String) -> Result<Subcollection, DbError> {
        let dependencies: Value = serde_json::from_str(&dependencies_json)
            .map_err(|e| DbError::DeserializationError(format!("JSON parsing error: {}", e)))?;

        let dependencies_hash = crate::helper::get_json_hash(&dependencies_json);

        if !self.tree.contains_key(dependencies_hash.as_bytes())? {
            self.tree
                .insert(dependencies_hash.as_bytes(), dependencies_json.as_bytes())?;
            self.tree.flush()?;
        }

        Ok(Subcollection {
            collection: self,
            dependencies,
            dependencies_hash,
        })
    }
}

impl<'a> Subcollection<'a> {
    pub fn insert<T: Serialize>(&self, body: &T) -> Result<String, DbError> {
        let body_json = serde_json::to_string(body)
            .map_err(|e| DbError::SerializationError(format!("Failed to serialize body: {}", e)))?;

        self.insert_json(body_json)
    }

    pub fn insert_json(&self, body_json: String) -> Result<String, DbError> {
        let body: Value = serde_json::from_str(&body_json)
            .map_err(|e| DbError::DeserializationError(format!("JSON parsing error: {}", e)))?;

        let full_object = serde_json::json!({
            "body": body,
            "dependencies": self.dependencies
        });

        let full_json = serde_json::to_string(&full_object).map_err(|e| {
            DbError::SerializationError(format!("Failed to serialize full object: {}", e))
        })?;

        self.collection.insert_json(full_json)
    }

    pub fn get<T: DeserializeOwned>(&self, id: &str) -> Result<T, DbError> {
        let body_json = self.get_json(id)?;

        serde_json::from_str(&body_json).map_err(|e| {
            DbError::DeserializationError(format!("Failed to deserialize body: {}", e))
        })
    }

    pub fn get_json(&self, id: &str) -> Result<String, DbError> {
        self.check_belongs_to_subcollection(id)?;

        let item_data = match self.collection.tree.get(id.as_bytes())? {
            Some(data) => data,
            None => return Err(DbError::NotFound),
        };

        let storage_value: Value = serde_json::from_slice(&item_data).map_err(|e| {
            DbError::DeserializationError(format!("Failed to deserialize storage value: {}", e))
        })?;

        if !storage_value.is_object() || !storage_value.as_object().unwrap().contains_key("body") {
            return Err(DbError::DeserializationError(
                "Invalid storage structure: missing 'body'".to_string(),
            ));
        }

        let body = &storage_value["body"];
        serde_json::to_string(body)
            .map_err(|e| DbError::SerializationError(format!("Failed to serialize body: {}", e)))
    }

    pub fn update<T: Serialize>(&self, id: &str, body: &T) -> Result<(), DbError> {
        let body_json = serde_json::to_string(body)
            .map_err(|e| DbError::SerializationError(format!("Failed to serialize body: {}", e)))?;

        self.update_json(id, body_json)
    }

    pub fn update_json(&self, id: &str, body_json: String) -> Result<(), DbError> {
        self.check_belongs_to_subcollection(id)?;

        let item_data = match self.collection.tree.get(id.as_bytes())? {
            Some(data) => data,
            None => return Err(DbError::NotFound),
        };

        let mut storage_value: Value = serde_json::from_slice(&item_data).map_err(|e| {
            DbError::DeserializationError(format!("Failed to deserialize storage value: {}", e))
        })?;

        if !storage_value.is_object()
            || !storage_value.as_object().unwrap().contains_key("deps")
            || !storage_value.as_object().unwrap().contains_key("body")
        {
            return Err(DbError::DeserializationError(
                "Invalid storage structure: missing 'deps' or 'body'".to_string(),
            ));
        }

        let old_body = &storage_value["body"];

        let new_body: Value = serde_json::from_str(&body_json).map_err(|e| {
            DbError::DeserializationError(format!("JSON parsing error for new body: {}", e))
        })?;

        let body_changed = serde_json::to_string(old_body)
            .map_err(|e| DbError::SerializationError(e.to_string()))?
            != serde_json::to_string(&new_body)
                .map_err(|e| DbError::SerializationError(e.to_string()))?;

        if !body_changed {
            return Ok(());
        }

        storage_value["body"] = new_body;

        let updated_json = serde_json::to_string(&storage_value).map_err(|e| {
            DbError::SerializationError(format!("Failed to serialize updated storage value: {}", e))
        })?;

        self.collection
            .tree
            .insert(id.as_bytes(), updated_json.as_bytes())?;
        self.collection.tree.flush()?;

        Ok(())
    }

    pub fn delete(&self, id: &str) -> Result<(), DbError> {
        self.delete_json(id)
    }

    pub fn delete_json(&self, id: &str) -> Result<(), DbError> {
        self.check_belongs_to_subcollection(id)?;

        self.collection.delete(id)
    }

    pub fn get_keys(&self) -> Result<Vec<String>, DbError> {
        let mut keys = Vec::new();
        let prefix = format!("{}_", self.dependencies_hash);

        for result in self.collection.tree.scan_prefix(prefix.as_bytes()) {
            match result {
                Ok((key_bytes, _)) => {
                    if let Ok(marker_key) = String::from_utf8(key_bytes.to_vec()) {
                        let parts: Vec<&str> = marker_key.split('_').collect();
                        if parts.len() > 1 {
                            let id = parts[1..].join("_");
                            keys.push(id);
                        }
                    }
                }
                Err(e) => return Err(DbError::from(e)),
            }
        }

        Ok(keys)
    }

    fn check_belongs_to_subcollection(&self, id: &str) -> Result<(), DbError> {
        let marker_key = format!("{}_{}", self.dependencies_hash, id);

        if !self.collection.tree.contains_key(marker_key.as_bytes())? {
            return Err(DbError::NotFound);
        }

        Ok(())
    }
}
