use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{Value, json};
use sled;
use std::error::Error;
use std::fmt;

mod helper;
use helper::get_json_hash;

#[derive(Debug)]
pub enum DbError {
    SerializationError(String),
    DeserializationError(String),
    NotFound,
    DatabaseError(String),
    AlreadyExists(String),
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DbError::SerializationError(msg) => write!(f, "Serialization error: {}", msg),
            DbError::DeserializationError(msg) => write!(f, "Deserialization error: {}", msg),
            DbError::NotFound => write!(f, "Item not found"),
            DbError::DatabaseError(msg) => write!(f, "Database error: {}", msg),
            DbError::AlreadyExists(msg) => write!(f, "Item already exists: {}", msg),
        }
    }
}

impl Error for DbError {}

impl From<sled::Error> for DbError {
    fn from(err: sled::Error) -> Self {
        DbError::DatabaseError(err.to_string())
    }
}

pub struct  Collection {
    tree: sled::Tree,
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

    pub fn collection(&self, name: &str) -> Result<Collection, DbError> {
        let tree = self.db.open_tree(name.as_bytes())
            .map_err(|e| DbError::DatabaseError(e.to_string()))?;
        
        Ok(Collection {
            tree: tree,
        })
    }

    pub fn collection_exists(&self, name: &str) -> Result<bool, DbError> {
        let tree_names = self.db.tree_names();
        Ok(tree_names.iter().any(|tree_name| tree_name == name.as_bytes()))
    }

    pub fn list_collections(&self) -> Vec<String> {
        self.db.tree_names()
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

impl Collection {
    fn generate_id(&self) -> String {
        let rand_string: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(16)
            .map(char::from)
            .collect();

            rand_string
    }

    // fn extract_dependencies_hash(&self, id: &str) -> Result<String, DbError> {
    //     let parts: Vec<&str> = id.split('_').collect();
    //     if parts.is_empty() {
    //         return Err(DbError::DeserializationError(
    //             "Invalid ID format".to_string(),
    //         ));
    //     }
    //     Ok(parts[0].to_string())
    // }

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
            None => return Err(DbError::DeserializationError(
                "Invalid JSON structure: Not an object".to_string()
            )),
        };

        if obj.len() != 2 || !obj.contains_key("body") || !obj.contains_key("dependencies") {
            return Err(DbError::DeserializationError(
                "Invalid JSON structure: Message must contains exactly 2 keys: 'body' and 'dependencies'".to_string()
            ));
        }

        let body = &value["body"];
        let dependencies = &value["dependencies"];

        let dependencies_json = serde_json::to_string(dependencies)
            .map_err(|e| DbError::SerializationError(format!("Failed to serialize dependencies: {}", e)))?;

        let deps_hash = get_json_hash(&dependencies_json);
        
        let id = self.generate_id();

        if self.tree.contains_key(id.as_bytes())? {
            return Err(DbError::AlreadyExists(format!("Item with ID {} already exists", id)));
        }

        let storage_value = json!({
            "deps": deps_hash,
            "body": body
        });
        
        let storage_json = serde_json::to_string(&storage_value)
            .map_err(|e| DbError::SerializationError(format!("Failed to serialize storage value: {}", e)))?;

        self.tree.insert(id.as_bytes(), storage_json.as_bytes())?;

        if !self.tree.contains_key(deps_hash.as_bytes())? {
            self.tree.insert(deps_hash.as_bytes(), dependencies_json.as_bytes())?;
        }

        let marker_key = format!("{}_{}", deps_hash, id);
        self.tree.insert(marker_key.as_bytes(), &[])?;

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

        let storage_value: Value = serde_json::from_slice(&item_data)
            .map_err(|e| DbError::DeserializationError(format!("Failed to deserialize storage value: {}", e)))?;
        
        if !storage_value.is_object() || 
           !storage_value.as_object().unwrap().contains_key("deps") || 
           !storage_value.as_object().unwrap().contains_key("body") {
            return Err(DbError::DeserializationError(
                "Invalid storage structure: missing 'deps' or 'body'".to_string()
            ));
        }

        let body = &storage_value["body"];
        let deps_hash = storage_value["deps"].as_str().ok_or_else(|| 
            DbError::DeserializationError("Invalid deps_hash format".to_string()))?;

        let deps_data = match self.tree.get(deps_hash.as_bytes())? {
            Some(data) => data,
            None => return Err(DbError::DeserializationError(
                format!("Dependencies with hash {} not found", deps_hash)
            )),
        };

        let dependencies: Value = serde_json::from_slice(&deps_data)
            .map_err(|e| DbError::DeserializationError(format!("Failed to deserialize dependencies: {}", e)))?;
        
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
        
        let old_storage_value: Value = serde_json::from_slice(&old_item_data)
            .map_err(|e| DbError::DeserializationError(format!("Failed to deserialize old item: {}", e)))?;
        
        if !old_storage_value.is_object() || 
           !old_storage_value.as_object().unwrap().contains_key("deps") || 
           !old_storage_value.as_object().unwrap().contains_key("body") {
            return Err(DbError::DeserializationError(
                "Invalid storage structure: missing 'deps' or 'body'".to_string()
            ));
        }
        
        let old_body = &old_storage_value["body"];
        let old_deps_hash = old_storage_value["deps"].as_str().ok_or_else(|| 
            DbError::DeserializationError("Invalid deps_hash format".to_string()))?;
            
        let new_value: Value = serde_json::from_str(&json)
            .map_err(|e| DbError::DeserializationError(format!("JSON parsing error: {}", e)))?;

        let obj = match new_value.as_object() {
            Some(obj) => obj,
            None => return Err(DbError::DeserializationError(
                "Invalid JSON structure: Not an object".to_string()
            )),
        };

        if obj.len() != 2 || !obj.contains_key("body") || !obj.contains_key("dependencies") {
            return Err(DbError::DeserializationError(
                "Invalid JSON structure: Message must contains exactly 2 keys: 'body' and 'dependencies'".to_string()
            ));
        }

        let new_body = &new_value["body"];
        let new_dependencies = &new_value["dependencies"];

        let new_dependencies_json = serde_json::to_string(new_dependencies)
            .map_err(|e| DbError::SerializationError(format!("Failed to serialize dependencies: {}", e)))?;
        
        let new_deps_hash = get_json_hash(&new_dependencies_json);
        
        let body_changed = serde_json::to_string(old_body)
            .map_err(|e| DbError::SerializationError(e.to_string()))? != 
            serde_json::to_string(new_body)
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
            let old_marker_key = format!("{}_{}", old_deps_hash, id);
            self.tree.remove(old_marker_key.as_bytes())?;
            
            if !self.tree.contains_key(new_deps_hash.as_bytes())? {
                self.tree.insert(new_deps_hash.as_bytes(), new_dependencies_json.as_bytes())?;
            }
            
            let new_marker_key = format!("{}_{}", new_deps_hash, id);
            self.tree.insert(new_marker_key.as_bytes(), &[])?;
            
            new_storage_value["deps"] = json!(new_deps_hash);
        }
        
        let updated_storage_json = serde_json::to_string(&new_storage_value)
            .map_err(|e| DbError::SerializationError(format!("Failed to serialize updated item: {}", e)))?;
        
        self.tree.insert(id.as_bytes(), updated_storage_json.as_bytes())?;
        
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
        
        let storage_value: Value = serde_json::from_slice(&item_data)
            .map_err(|e| DbError::DeserializationError(format!("Failed to deserialize item: {}", e)))?;
        
        if !storage_value.is_object() || 
           !storage_value.as_object().unwrap().contains_key("deps") {
            return Err(DbError::DeserializationError(
                "Invalid storage structure: missing 'deps'".to_string()
            ));
        }

        let deps_hash = storage_value["deps"].as_str().ok_or_else(|| 
            DbError::DeserializationError("Invalid deps_hash format".to_string()))?;
        
        self.tree.remove(id.as_bytes())?;
        
        let marker_key = format!("{}_{}", deps_hash, id);
        self.tree.remove(marker_key.as_bytes())?;
        
        let mut has_other_items = false;
        let prefix = format!("{}_", deps_hash);
        
        for result in self.tree.scan_prefix(prefix.as_bytes()) {
            if result.is_ok() {
                has_other_items = true;
                break;
            }
        }
        
        if !has_other_items {
            self.tree.remove(deps_hash.as_bytes())?;
        }
        
        self.tree.flush()?;
        Ok(())
    }

    pub fn subcollection<T: Serialize>(&self, dependencies: &T) -> Result<Subcollection, DbError> {
        let deps_json = serde_json::to_string(dependencies)
            .map_err(|e| DbError::SerializationError(format!("Failed to serialize dependencies: {}", e)))?;
        
        self.subcollection_json(deps_json)
    }
    
    pub fn subcollection_json(&self, dependencies_json: String) -> Result<Subcollection, DbError> {
        let dependencies: Value = serde_json::from_str(&dependencies_json)
            .map_err(|e| DbError::DeserializationError(format!("JSON parsing error: {}", e)))?;
        
        let dependencies_hash = crate::helper::get_json_hash(&dependencies_json);
        
        if !self.tree.contains_key(dependencies_hash.as_bytes())? {
            self.tree.insert(dependencies_hash.as_bytes(), dependencies_json.as_bytes())?;
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
        
        let full_json = serde_json::to_string(&full_object)
            .map_err(|e| DbError::SerializationError(format!("Failed to serialize full object: {}", e)))?;
        
        self.collection.insert_json(full_json)
    }
    
    pub fn get<T: DeserializeOwned>(&self, id: &str) -> Result<T, DbError> {
        let body_json = self.get_json(id)?;
        
        serde_json::from_str(&body_json)
            .map_err(|e| DbError::DeserializationError(format!("Failed to deserialize body: {}", e)))
    }
    
    pub fn get_json(&self, id: &str) -> Result<String, DbError> {
        self.check_belongs_to_subcollection(id)?;
        
        let item_data = match self.collection.tree.get(id.as_bytes())? {
            Some(data) => data,
            None => return Err(DbError::NotFound),
        };

        let storage_value: Value = serde_json::from_slice(&item_data)
            .map_err(|e| DbError::DeserializationError(format!("Failed to deserialize storage value: {}", e)))?;
        
        if !storage_value.is_object() || !storage_value.as_object().unwrap().contains_key("body") {
            return Err(DbError::DeserializationError(
                "Invalid storage structure: missing 'body'".to_string()
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
        
        let mut storage_value: Value = serde_json::from_slice(&item_data)
            .map_err(|e| DbError::DeserializationError(format!("Failed to deserialize storage value: {}", e)))?;
        
        if !storage_value.is_object() || 
           !storage_value.as_object().unwrap().contains_key("deps") || 
           !storage_value.as_object().unwrap().contains_key("body") {
            return Err(DbError::DeserializationError(
                "Invalid storage structure: missing 'deps' or 'body'".to_string()
            ));
        }
        
        let old_body = &storage_value["body"];
        
        let new_body: Value = serde_json::from_str(&body_json)
            .map_err(|e| DbError::DeserializationError(format!("JSON parsing error for new body: {}", e)))?;
        
        let body_changed = serde_json::to_string(old_body)
            .map_err(|e| DbError::SerializationError(e.to_string()))? != 
            serde_json::to_string(&new_body)
            .map_err(|e| DbError::SerializationError(e.to_string()))?;
            
        if !body_changed {
            return Ok(());
        }
        
        storage_value["body"] = new_body;
        
        let updated_json = serde_json::to_string(&storage_value)
            .map_err(|e| DbError::SerializationError(format!("Failed to serialize updated storage value: {}", e)))?;
        
        self.collection.tree.insert(id.as_bytes(), updated_json.as_bytes())?;
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
