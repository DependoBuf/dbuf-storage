use actix_web::http::StatusCode;
use actix_web::{middleware, web, App, HttpResponse, HttpServer, Responder, ResponseError};
use clap::{Arg, Command};
use dbuf_storage::{Database, DbError};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;
use std::sync::Arc;

#[derive(Serialize, Deserialize)]
struct ApiResponse<T> {
    success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct InsertResponse {
    id: String,
}

#[derive(Serialize, Deserialize)]
struct CollectionRequest {
    name: String,
}

#[derive(Serialize, Deserialize)]
struct CollectionWithSchemaRequest {
    name: String,
    body_schema: String,
    dependencies_schema: String,
}

#[derive(Serialize, Deserialize)]
struct SubcollectionRequest {
    collection: String,
    dependencies: Value,
}

struct AppState {
    db: Arc<Database>,
}

#[derive(Debug)]
struct AppError(DbError);

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for AppError {}

impl From<DbError> for AppError {
    fn from(err: DbError) -> Self {
        AppError(err)
    }
}

impl ResponseError for AppError {
    fn error_response(&self) -> HttpResponse {
        let response = ApiResponse::<()> {
            success: false,
            data: None,
            error: Some(self.0.to_string()),
        };

        HttpResponse::build(self.status_code())
            .content_type("application/json")
            .json(response)
    }

    fn status_code(&self) -> StatusCode {
        match self.0 {
            DbError::NotFound => StatusCode::NOT_FOUND,
            DbError::AlreadyExists(_) => StatusCode::CONFLICT,
            DbError::DeserializationError(_)
            | DbError::SerializationError(_)
            | DbError::SchemaValidationError(_)
            | DbError::SchemaError(_)
            | DbError::SchemaCompilationError(_) => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

async fn create_collection(
    app_state: web::Data<AppState>,
    req: web::Json<CollectionRequest>,
) -> Result<impl Responder, AppError> {
    app_state.db.create_collection(&req.name)?;

    let response = ApiResponse {
        success: true,
        data: Some(req.into_inner()),
        error: None,
    };

    Ok(web::Json(response))
}

async fn create_collection_with_schema(
    app_state: web::Data<AppState>,
    req: web::Json<CollectionWithSchemaRequest>,
) -> Result<impl Responder, AppError> {
    app_state.db.create_collection_with_schema_json(
        &req.name,
        &req.body_schema,
        &req.dependencies_schema,
    )?;

    let response = ApiResponse {
        success: true,
        data: Some(req.into_inner()),
        error: None,
    };

    Ok(web::Json(response))
}

async fn list_collections(app_state: web::Data<AppState>) -> Result<impl Responder, AppError> {
    let collections = app_state.db.list_collections();

    let response = ApiResponse {
        success: true,
        data: Some(collections),
        error: None,
    };

    Ok(web::Json(response))
}

async fn check_collection_exists(
    app_state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<impl Responder, AppError> {
    let collection_name = path.into_inner();
    let exists = app_state.db.collection_exists(&collection_name)?;

    let response = ApiResponse {
        success: true,
        data: Some(exists),
        error: None,
    };

    Ok(web::Json(response))
}

async fn get_collection_info(
    app_state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<impl Responder, AppError> {
    let collection_name = path.into_inner();
    let collection = app_state.db.get_collection(&collection_name)?;

    #[derive(Serialize)]
    struct CollectionInfo {
        name: String,
        has_schema: bool,
        created_at: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        body_schema: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        dependencies_schema: Option<String>,
    }

    let info = CollectionInfo {
        name: collection_name,
        has_schema: collection.has_schema(),
        created_at: collection.get_created_at(),
        body_schema: collection.get_body_schema_json().map(|s| s.to_string()),
        dependencies_schema: collection
            .get_dependencies_schema_json()
            .map(|s| s.to_string()),
    };

    let response = ApiResponse {
        success: true,
        data: Some(info),
        error: None,
    };

    Ok(web::Json(response))
}

async fn drop_collection(
    app_state: web::Data<AppState>,
    req: web::Json<CollectionRequest>,
) -> Result<impl Responder, AppError> {
    app_state.db.drop_collection(&req.name)?;

    let response = ApiResponse::<()> {
        success: true,
        data: None,
        error: None,
    };

    Ok(web::Json(response))
}

async fn insert_to_collection(
    app_state: web::Data<AppState>,
    path: web::Path<String>,
    json_data: web::Json<Value>,
) -> Result<impl Responder, AppError> {
    let collection_name = path.into_inner();
    let collection = app_state.db.get_collection(&collection_name)?;

    let json_string = serde_json::to_string(&json_data.into_inner())
        .map_err(|e| DbError::SerializationError(e.to_string()))?;

    let id = collection.insert_json(json_string)?;

    let response = ApiResponse {
        success: true,
        data: Some(InsertResponse { id }),
        error: None,
    };

    Ok(web::Json(response))
}

async fn batch_insert_to_collection(
    app_state: web::Data<AppState>,
    path: web::Path<String>,
    json_data: web::Json<Vec<Value>>,
) -> Result<impl Responder, AppError> {
    let collection_name = path.into_inner();
    let collection = app_state.db.get_collection(&collection_name)?;

    let mut ids = Vec::with_capacity(json_data.len());

    for item in json_data.iter() {
        let json_string =
            serde_json::to_string(item).map_err(|e| DbError::SerializationError(e.to_string()))?;

        let id = collection.insert_json(json_string)?;
        ids.push(id);
    }

    #[derive(Serialize)]
    struct BatchInsertResponse {
        count: usize,
        ids: Vec<String>,
    }

    let response = ApiResponse {
        success: true,
        data: Some(BatchInsertResponse {
            count: ids.len(),
            ids,
        }),
        error: None,
    };

    Ok(web::Json(response))
}

async fn get_from_collection(
    app_state: web::Data<AppState>,
    path: web::Path<(String, String)>,
) -> Result<impl Responder, AppError> {
    let (collection_name, id) = path.into_inner();
    let collection = app_state.db.get_collection(&collection_name)?;

    let json_string = collection.get_json(&id)?;
    let json_value: Value = serde_json::from_str(&json_string)
        .map_err(|e| DbError::DeserializationError(e.to_string()))?;

    let response = ApiResponse {
        success: true,
        data: Some(json_value),
        error: None,
    };

    Ok(web::Json(response))
}

async fn batch_get_from_collection(
    app_state: web::Data<AppState>,
    path: web::Path<String>,
    ids: web::Json<Vec<String>>,
) -> Result<impl Responder, AppError> {
    let collection_name = path.into_inner();
    let collection = app_state.db.get_collection(&collection_name)?;

    let mut results = Vec::with_capacity(ids.len());
    let mut errors = Vec::new();

    for id in ids.iter() {
        match collection.get_json(id) {
            Ok(json_string) => {
                let json_value: Value = serde_json::from_str(&json_string)
                    .map_err(|e| DbError::DeserializationError(e.to_string()))?;
                results.push((id.clone(), json_value));
            }
            Err(e) => {
                errors.push((id.clone(), e.to_string()));
            }
        }
    }

    #[derive(Serialize)]
    struct BatchGetResponse {
        found: Vec<(String, Value)>,
        not_found: Vec<(String, String)>,
    }

    let response = ApiResponse {
        success: true,
        data: Some(BatchGetResponse {
            found: results,
            not_found: errors,
        }),
        error: None,
    };

    Ok(web::Json(response))
}

async fn update_in_collection(
    app_state: web::Data<AppState>,
    path: web::Path<(String, String)>,
    json_data: web::Json<Value>,
) -> Result<impl Responder, AppError> {
    let (collection_name, id) = path.into_inner();
    let collection = app_state.db.get_collection(&collection_name)?;

    let json_string = serde_json::to_string(&json_data.into_inner())
        .map_err(|e| DbError::SerializationError(e.to_string()))?;

    collection.update_json(&id, json_string)?;

    let response = ApiResponse::<()> {
        success: true,
        data: None,
        error: None,
    };

    Ok(web::Json(response))
}

async fn delete_from_collection(
    app_state: web::Data<AppState>,
    path: web::Path<(String, String)>,
) -> Result<impl Responder, AppError> {
    let (collection_name, id) = path.into_inner();
    let collection = app_state.db.get_collection(&collection_name)?;

    collection.delete_json(&id)?;

    let response = ApiResponse::<()> {
        success: true,
        data: None,
        error: None,
    };

    Ok(web::Json(response))
}

async fn batch_delete_from_collection(
    app_state: web::Data<AppState>,
    path: web::Path<String>,
    ids: web::Json<Vec<String>>,
) -> Result<impl Responder, AppError> {
    let collection_name = path.into_inner();
    let collection = app_state.db.get_collection(&collection_name)?;

    let mut deleted = Vec::new();
    let mut errors = Vec::new();

    for id in ids.iter() {
        match collection.delete_json(id) {
            Ok(_) => deleted.push(id.clone()),
            Err(e) => errors.push((id.clone(), e.to_string())),
        }
    }

    #[derive(Serialize)]
    struct BatchDeleteResponse {
        deleted: Vec<String>,
        failed: Vec<(String, String)>,
    }

    let response = ApiResponse {
        success: true,
        data: Some(BatchDeleteResponse {
            deleted,
            failed: errors,
        }),
        error: None,
    };

    Ok(web::Json(response))
}

async fn create_subcollection(
    app_state: web::Data<AppState>,
    req: web::Json<SubcollectionRequest>,
) -> Result<impl Responder, AppError> {
    let collection = app_state.db.get_collection(&req.collection)?;

    let dependencies_json = serde_json::to_string(&req.dependencies)
        .map_err(|e| DbError::SerializationError(e.to_string()))?;

    collection.subcollection_json(dependencies_json)?;

    let response = ApiResponse {
        success: true,
        data: Some(req.into_inner()),
        error: None,
    };

    Ok(web::Json(response))
}

async fn insert_to_subcollection(
    app_state: web::Data<AppState>,
    path: web::Path<String>,
    query: web::Query<SubcollectionRequest>,
    json_data: web::Json<Value>,
) -> Result<impl Responder, AppError> {
    let collection_name = path.into_inner();
    let collection = app_state.db.get_collection(&collection_name)?;

    let dependencies_json = serde_json::to_string(&query.dependencies)
        .map_err(|e| DbError::SerializationError(e.to_string()))?;

    let subcollection = collection.subcollection_json(dependencies_json)?;

    let body_json = serde_json::to_string(&json_data.into_inner())
        .map_err(|e| DbError::SerializationError(e.to_string()))?;

    let id = subcollection.insert_json(body_json)?;

    let response = ApiResponse {
        success: true,
        data: Some(InsertResponse { id }),
        error: None,
    };

    Ok(web::Json(response))
}

async fn batch_insert_to_subcollection(
    app_state: web::Data<AppState>,
    path: web::Path<String>,
    query: web::Query<SubcollectionRequest>,
    json_data: web::Json<Vec<Value>>,
) -> Result<impl Responder, AppError> {
    let collection_name = path.into_inner();
    let collection = app_state.db.get_collection(&collection_name)?;

    let dependencies_json = serde_json::to_string(&query.dependencies)
        .map_err(|e| DbError::SerializationError(e.to_string()))?;

    let subcollection = collection.subcollection_json(dependencies_json)?;

    let mut ids = Vec::with_capacity(json_data.len());

    for item in json_data.iter() {
        let body_json =
            serde_json::to_string(item).map_err(|e| DbError::SerializationError(e.to_string()))?;

        let id = subcollection.insert_json(body_json)?;
        ids.push(id);
    }

    #[derive(Serialize)]
    struct BatchInsertResponse {
        count: usize,
        ids: Vec<String>,
    }

    let response = ApiResponse {
        success: true,
        data: Some(BatchInsertResponse {
            count: ids.len(),
            ids,
        }),
        error: None,
    };

    Ok(web::Json(response))
}

async fn get_from_subcollection(
    app_state: web::Data<AppState>,
    path: web::Path<(String, String)>,
    query: web::Query<SubcollectionRequest>,
) -> Result<impl Responder, AppError> {
    let (collection_name, id) = path.into_inner();
    let collection = app_state.db.get_collection(&collection_name)?;

    let dependencies_json = serde_json::to_string(&query.dependencies)
        .map_err(|e| DbError::SerializationError(e.to_string()))?;

    let subcollection = collection.subcollection_json(dependencies_json)?;

    let json_string = subcollection.get_json(&id)?;
    let json_value: Value = serde_json::from_str(&json_string)
        .map_err(|e| DbError::DeserializationError(e.to_string()))?;

    let response = ApiResponse {
        success: true,
        data: Some(json_value),
        error: None,
    };

    Ok(web::Json(response))
}

async fn batch_get_from_subcollection(
    app_state: web::Data<AppState>,
    path: web::Path<String>,
    query: web::Query<SubcollectionRequest>,
    ids: web::Json<Vec<String>>,
) -> Result<impl Responder, AppError> {
    let collection_name = path.into_inner();
    let collection = app_state.db.get_collection(&collection_name)?;

    let dependencies_json = serde_json::to_string(&query.dependencies)
        .map_err(|e| DbError::SerializationError(e.to_string()))?;

    let subcollection = collection.subcollection_json(dependencies_json)?;

    let mut results = Vec::with_capacity(ids.len());
    let mut errors = Vec::new();

    for id in ids.iter() {
        match subcollection.get_json(id) {
            Ok(json_string) => {
                let json_value: Value = serde_json::from_str(&json_string)
                    .map_err(|e| DbError::DeserializationError(e.to_string()))?;
                results.push((id.clone(), json_value));
            }
            Err(e) => {
                errors.push((id.clone(), e.to_string()));
            }
        }
    }

    #[derive(Serialize)]
    struct BatchGetResponse {
        found: Vec<(String, Value)>,
        not_found: Vec<(String, String)>,
    }

    let response = ApiResponse {
        success: true,
        data: Some(BatchGetResponse {
            found: results,
            not_found: errors,
        }),
        error: None,
    };

    Ok(web::Json(response))
}

async fn update_in_subcollection(
    app_state: web::Data<AppState>,
    path: web::Path<(String, String)>,
    query: web::Query<SubcollectionRequest>,
    json_data: web::Json<Value>,
) -> Result<impl Responder, AppError> {
    let (collection_name, id) = path.into_inner();
    let collection = app_state.db.get_collection(&collection_name)?;

    let dependencies_json = serde_json::to_string(&query.dependencies)
        .map_err(|e| DbError::SerializationError(e.to_string()))?;

    let subcollection = collection.subcollection_json(dependencies_json)?;

    let body_json = serde_json::to_string(&json_data.into_inner())
        .map_err(|e| DbError::SerializationError(e.to_string()))?;

    subcollection.update_json(&id, body_json)?;

    let response = ApiResponse::<()> {
        success: true,
        data: None,
        error: None,
    };

    Ok(web::Json(response))
}

async fn delete_from_subcollection(
    app_state: web::Data<AppState>,
    path: web::Path<(String, String)>,
    query: web::Query<SubcollectionRequest>,
) -> Result<impl Responder, AppError> {
    let (collection_name, id) = path.into_inner();
    let collection = app_state.db.get_collection(&collection_name)?;

    let dependencies_json = serde_json::to_string(&query.dependencies)
        .map_err(|e| DbError::SerializationError(e.to_string()))?;

    let subcollection = collection.subcollection_json(dependencies_json)?;

    subcollection.delete_json(&id)?;

    let response = ApiResponse::<()> {
        success: true,
        data: None,
        error: None,
    };

    Ok(web::Json(response))
}

async fn batch_delete_from_subcollection(
    app_state: web::Data<AppState>,
    path: web::Path<String>,
    query: web::Query<SubcollectionRequest>,
    ids: web::Json<Vec<String>>,
) -> Result<impl Responder, AppError> {
    let collection_name = path.into_inner();
    let collection = app_state.db.get_collection(&collection_name)?;

    let dependencies_json = serde_json::to_string(&query.dependencies)
        .map_err(|e| DbError::SerializationError(e.to_string()))?;

    let subcollection = collection.subcollection_json(dependencies_json)?;

    let mut deleted = Vec::new();
    let mut errors = Vec::new();

    for id in ids.iter() {
        match subcollection.delete_json(id) {
            Ok(_) => deleted.push(id.clone()),
            Err(e) => errors.push((id.clone(), e.to_string())),
        }
    }

    #[derive(Serialize)]
    struct BatchDeleteResponse {
        deleted: Vec<String>,
        failed: Vec<(String, String)>,
    }

    let response = ApiResponse {
        success: true,
        data: Some(BatchDeleteResponse {
            deleted,
            failed: errors,
        }),
        error: None,
    };

    Ok(web::Json(response))
}

async fn get_subcollection_keys(
    app_state: web::Data<AppState>,
    path: web::Path<String>,
    query: web::Query<SubcollectionRequest>,
) -> Result<impl Responder, AppError> {
    let collection_name = path.into_inner();
    let collection = app_state.db.get_collection(&collection_name)?;

    let dependencies_json = serde_json::to_string(&query.dependencies)
        .map_err(|e| DbError::SerializationError(e.to_string()))?;

    let subcollection = collection.subcollection_json(dependencies_json)?;

    let keys = subcollection.get_keys()?;

    let response = ApiResponse {
        success: true,
        data: Some(keys),
        error: None,
    };

    Ok(web::Json(response))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    let matches = Command::new("DBuf Storage API Server")
        .version("1.0")
        .author("Your Name")
        .about("REST API for DBuf Storage database")
        .arg(
            Arg::new("db-path")
                .long("db-path")
                .short('d')
                .help("Path to the database directory")
                .value_name("PATH"),
        )
        .arg(
            Arg::new("bind-address")
                .long("bind-address")
                .short('b')
                .help("Address to bind the server (format: host:port)")
                .value_name("ADDRESS"),
        )
        .get_matches();

    let db_path = matches
        .get_one::<String>("db-path")
        .map(|s| s.clone())
        .or_else(|| std::env::var("DB_PATH").ok())
        .unwrap_or_else(|| "./data".to_string());

    let bind_address = matches
        .get_one::<String>("bind-address")
        .map(|s| s.clone())
        .or_else(|| std::env::var("BIND_ADDRESS").ok())
        .unwrap_or_else(|| "127.0.0.1:8080".to_string());

    println!("Database path: {}", db_path);
    println!("Binding to: {}", bind_address);

    let db = match Database::new(Some(&db_path)) {
        Ok(db) => {
            println!("Successfully opened database at: {}", db_path);
            Arc::new(db)
        }
        Err(e) => {
            eprintln!("Failed to open database: {}", e);
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string(),
            ));
        }
    };

    let app_state = web::Data::new(AppState { db });

    println!("Starting server at: {}", bind_address);

    HttpServer::new(move || {
        App::new()
            .app_data(app_state.clone())
            .wrap(middleware::Logger::default())
            .service(
                web::resource("/collections")
                    .route(web::get().to(list_collections))
                    .route(web::post().to(create_collection))
                    .route(web::delete().to(drop_collection)),
            )
            .service(
                web::resource("/collections/schema")
                    .route(web::post().to(create_collection_with_schema)),
            )
            .service(
                web::resource("/collections/{name}/exists")
                    .route(web::get().to(check_collection_exists)),
            )
            .service(
                web::resource("/collections/{name}/batch")
                    .route(web::post().to(batch_insert_to_collection))
                    .route(web::get().to(batch_get_from_collection))
                    .route(web::delete().to(batch_delete_from_collection)),
            )
            .service(
                web::resource("/collections/{name}")
                    .route(web::get().to(get_collection_info))
                    .route(web::post().to(insert_to_collection)),
            )
            .service(
                web::resource("/collections/{name}/{id}")
                    .route(web::get().to(get_from_collection))
                    .route(web::put().to(update_in_collection))
                    .route(web::delete().to(delete_from_collection)),
            )
            .service(web::resource("/subcollections").route(web::post().to(create_subcollection)))
            .service(
                web::resource("/subcollections/{name}/keys")
                    .route(web::get().to(get_subcollection_keys)),
            )
            .service(
                web::resource("/subcollections/{name}/batch")
                    .route(web::post().to(batch_insert_to_subcollection))
                    .route(web::get().to(batch_get_from_subcollection))
                    .route(web::delete().to(batch_delete_from_subcollection)),
            )
            .service(
                web::resource("/subcollections/{name}")
                    .route(web::post().to(insert_to_subcollection)),
            )
            .service(
                web::resource("/subcollections/{name}/{id}")
                    .route(web::get().to(get_from_subcollection))
                    .route(web::put().to(update_in_subcollection))
                    .route(web::delete().to(delete_from_subcollection)),
            )
    })
    .bind(bind_address)?
    .run()
    .await
}
