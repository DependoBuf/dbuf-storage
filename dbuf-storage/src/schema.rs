use jsonschema::JSONSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

use crate::DbError;

#[derive(Serialize, Deserialize, Clone)]
pub struct Schema {
    pub schema_json: String,
    pub schema_hash: String,
    #[serde(skip)]
    pub compiled: Option<Arc<JSONSchema>>,
}

impl Schema {
    pub fn validate_data(&self, data: &Value) -> Result<(), DbError> {
        let compiled_schema = match &self.compiled {
            Some(schema) => schema.clone(),
            None => return Err(DbError::SchemaError("Body schema not compiled".to_string())),
        };

        let validation_result = compiled_schema.validate(data);
        if let Err(errors) = validation_result {
            let error_messages: Vec<String> = errors
                .into_iter()
                .map(|err| format!("{} at path: {}", err, err.instance_path))
                .collect();

            return Err(DbError::SchemaValidationError(format!(
                "Data validation failed: {}",
                error_messages.join(", ")
            )));
        }

        Ok(())
    }
}
