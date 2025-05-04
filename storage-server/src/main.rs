use actix_web::{middleware, web, App, Error, HttpResponse, HttpServer, Responder, ResponseError};
use actix_web::http::StatusCode;
use serde::{Serialize, Deserialize};
use serde_json::Value;
use std::fmt;
use std::sync::Arc;
use dbuf_storage::{Database, DbError};

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
            DbError::DeserializationError(_) | DbError::SerializationError(_) => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}


async fn create_collection(
    app_state: web::Data<AppState>,
    req: web::Json<CollectionRequest>,
) -> Result<impl Responder, Error> {
    app_state.db.collection(&req.name).map_err(AppError)?;
    
    let response = ApiResponse {
        success: true,
        data: Some(req.into_inner()),
        error: None,
    };
    
    Ok(web::Json(response))
}

async fn list_collections(
    app_state: web::Data<AppState>,
) -> Result<impl Responder, Error> {
    let collections = app_state.db.list_collections();
    
    let response = ApiResponse {
        success: true,
        data: Some(collections),
        error: None,
    };
    
    Ok(web::Json(response))
}

async fn drop_collection(
    app_state: web::Data<AppState>,
    req: web::Json<CollectionRequest>,
) -> Result<impl Responder, Error> {
    app_state.db.drop_collection(&req.name).map_err(AppError)?;
    
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
) -> Result<impl Responder, Error> {
    let collection_name = path.into_inner();
    let collection = app_state.db.collection(&collection_name).map_err(AppError)?;
    
    let json_string = serde_json::to_string(&json_data.into_inner())
        .map_err(|e| DbError::SerializationError(e.to_string())).map_err(AppError)?;
        
    let id = collection.insert_json(json_string).map_err(AppError)?;
    
    let response = ApiResponse {
        success: true,
        data: Some(InsertResponse { id }),
        error: None,
    };
    
    Ok(web::Json(response))
}

async fn get_from_collection(
    app_state: web::Data<AppState>,
    path: web::Path<(String, String)>,
) -> Result<impl Responder, Error> {
    let (collection_name, id) = path.into_inner();
    let collection = app_state.db.collection(&collection_name).map_err(AppError)?;
    
    let json_string = collection.get_json(&id).map_err(AppError)?;
    let json_value: Value = serde_json::from_str(&json_string)
        .map_err(|e| DbError::DeserializationError(e.to_string())).map_err(AppError)?;
    
    let response = ApiResponse {
        success: true,
        data: Some(json_value),
        error: None,
    };
    
    Ok(web::Json(response))
}

async fn update_in_collection(
    app_state: web::Data<AppState>,
    path: web::Path<(String, String)>,
    json_data: web::Json<Value>,
) -> Result<impl Responder, Error> {
    let (collection_name, id) = path.into_inner();
    let collection = app_state.db.collection(&collection_name).map_err(AppError)?;
    
    let json_string = serde_json::to_string(&json_data.into_inner())
        .map_err(|e| DbError::SerializationError(e.to_string())).map_err(AppError)?;
        
    collection.update_json(&id, json_string).map_err(AppError)?;
    
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
) -> Result<impl Responder, Error> {
    let (collection_name, id) = path.into_inner();
    let collection = app_state.db.collection(&collection_name).map_err(AppError)?;
    
    collection.delete(&id).map_err(AppError)?;
    
    let response = ApiResponse::<()> {
        success: true,
        data: None,
        error: None,
    };
    
    Ok(web::Json(response))
}

async fn create_subcollection(
    app_state: web::Data<AppState>,
    req: web::Json<SubcollectionRequest>,
) -> Result<impl Responder, Error> {
    let collection = app_state.db.collection(&req.collection).map_err(AppError)?;
    
    let dependencies_json = serde_json::to_string(&req.dependencies)
        .map_err(|e| DbError::SerializationError(e.to_string())).map_err(AppError)?;
        
    collection.subcollection_json(dependencies_json).map_err(AppError)?;
    
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
) -> Result<impl Responder, Error> {
    let collection_name = path.into_inner();
    let collection = app_state.db.collection(&collection_name).map_err(AppError)?;
    
    let dependencies_json = serde_json::to_string(&query.dependencies)
        .map_err(|e| DbError::SerializationError(e.to_string())).map_err(AppError)?;
        
    let subcollection = collection.subcollection_json(dependencies_json).map_err(AppError)?;
    
    let body_json = serde_json::to_string(&json_data.into_inner())
        .map_err(|e| DbError::SerializationError(e.to_string())).map_err(AppError)?;
        
    let id = subcollection.insert_json(body_json).map_err(AppError)?;
    
    let response = ApiResponse {
        success: true,
        data: Some(InsertResponse { id }),
        error: None,
    };
    
    Ok(web::Json(response))
}

async fn get_from_subcollection(
    app_state: web::Data<AppState>,
    path: web::Path<(String, String)>,
    query: web::Query<SubcollectionRequest>,
) -> Result<impl Responder, Error> {
    let (collection_name, id) = path.into_inner();
    let collection = app_state.db.collection(&collection_name).map_err(AppError)?;
    
    let dependencies_json = serde_json::to_string(&query.dependencies)
        .map_err(|e| DbError::SerializationError(e.to_string())).map_err(AppError)?;
        
    let subcollection = collection.subcollection_json(dependencies_json).map_err(AppError)?;
    
    let json_string = subcollection.get_json(&id).map_err(AppError)?;
    let json_value: Value = serde_json::from_str(&json_string)
        .map_err(|e| DbError::DeserializationError(e.to_string())).map_err(AppError)?;
    
    let response = ApiResponse {
        success: true,
        data: Some(json_value),
        error: None,
    };
    
    Ok(web::Json(response))
}

async fn update_in_subcollection(
    app_state: web::Data<AppState>,
    path: web::Path<(String, String)>,
    query: web::Query<SubcollectionRequest>,
    json_data: web::Json<Value>,
) -> Result<impl Responder, Error> {
    let (collection_name, id) = path.into_inner();
    let collection = app_state.db.collection(&collection_name).map_err(AppError)?;
    
    let dependencies_json = serde_json::to_string(&query.dependencies)
        .map_err(|e| DbError::SerializationError(e.to_string())).map_err(AppError)?;
        
    let subcollection = collection.subcollection_json(dependencies_json).map_err(AppError)?;
    
    let body_json = serde_json::to_string(&json_data.into_inner())
        .map_err(|e| DbError::SerializationError(e.to_string())).map_err(AppError)?;
        
    subcollection.update_json(&id, body_json).map_err(AppError)?;
    
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
) -> Result<impl Responder, Error> {
    let (collection_name, id) = path.into_inner();
    let collection = app_state.db.collection(&collection_name).map_err(AppError)?;
    
    let dependencies_json = serde_json::to_string(&query.dependencies)
        .map_err(|e| DbError::SerializationError(e.to_string())).map_err(AppError)?;
        
    let subcollection = collection.subcollection_json(dependencies_json).map_err(AppError)?;
    
    subcollection.delete_json(&id).map_err(AppError)?;
    
    let response = ApiResponse::<()> {
        success: true,
        data: None,
        error: None,
    };
    
    Ok(web::Json(response))
}

async fn get_subcollection_keys(
    app_state: web::Data<AppState>,
    path: web::Path<String>,
    query: web::Query<SubcollectionRequest>,
) -> Result<impl Responder, Error> {
    let collection_name = path.into_inner();
    let collection = app_state.db.collection(&collection_name).map_err(AppError)?;
    
    let dependencies_json = serde_json::to_string(&query.dependencies)
        .map_err(|e| DbError::SerializationError(e.to_string())).map_err(AppError)?;
        
    let subcollection = collection.subcollection_json(dependencies_json).map_err(AppError)?;
    
    let keys = subcollection.get_keys().map_err(AppError)?;
    
    let response = ApiResponse {
        success: true,
        data: Some(keys),
        error: None,
    };
    
    Ok(web::Json(response))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let db_path = std::env::var("DB_PATH").unwrap_or_else(|_| "./data".to_string());
    
    let db = match Database::new(Some(&db_path)) {
        Ok(db) => {
            Arc::new(db)
        },
        Err(e) => {
            return Err(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()));
        }
    };
    
    let app_state = web::Data::new(AppState { db });
    
    let bind_address = std::env::var("BIND_ADDRESS").unwrap_or_else(|_| "127.0.0.1:8080".to_string());
    
    HttpServer::new(move || {
        App::new()
            .app_data(app_state.clone())
            .wrap(middleware::Logger::default())
            .service(web::resource("/collections")
                .route(web::get().to(list_collections))
                .route(web::post().to(create_collection))
                .route(web::delete().to(drop_collection))
            )
            .service(web::resource("/collections/{name}")
                .route(web::post().to(insert_to_collection))
            )
            .service(web::resource("/collections/{name}/{id}")
                .route(web::get().to(get_from_collection))
                .route(web::put().to(update_in_collection))
                .route(web::delete().to(delete_from_collection))
            )

            .service(web::resource("/subcollections")
                .route(web::post().to(create_subcollection))
            )
            .service(web::resource("/subcollections/{name}/keys")
                .route(web::get().to(get_subcollection_keys))
            )
            .service(web::resource("/subcollections/{name}")
                .route(web::post().to(insert_to_subcollection))
            )
            .service(web::resource("/subcollections/{name}/{id}")
                .route(web::get().to(get_from_subcollection))
                .route(web::put().to(update_in_subcollection))
                .route(web::delete().to(delete_from_subcollection))
            )
    })
    .bind(bind_address)?
    .run()
    .await
}
