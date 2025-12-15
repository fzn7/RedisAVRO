#[macro_use]
extern crate redis_module;
#[macro_use]
extern crate lazy_static;

use crc32fast::Hasher;
use redis_module::native_types::RedisType;
use redis_module::{raw, Context, RedisError, RedisResult, RedisString, RedisValue};
use serde_avro_fast::ser::SerializerConfig;
use serde_avro_fast::{self, Schema};
use serde_json::Value;
use std::collections::HashMap;
use std::os::raw::{c_char, c_int, c_void};
use std::slice;
use std::sync::{Arc, RwLock};

struct SchemaEntry {
    id: u32,
    schema: Arc<Schema>,
    json: String,
}

lazy_static! {
    static ref SCHEMA_STORE: RwLock<HashMap<String, SchemaEntry>> = RwLock::new(HashMap::new());
    static ref SCHEMA_NAME_BY_ID: RwLock<HashMap<u32, String>> = RwLock::new(HashMap::new());
    static ref SCHEMA_ID_BY_NAME: RwLock<HashMap<String, u32>> = RwLock::new(HashMap::new());
}

#[derive(Debug)]
struct AvroValue {
    schema_id: u32,
    payload: Vec<u8>,
}

impl AvroValue {
    fn new(schema_id: u32, payload: Vec<u8>) -> Self {
        Self { schema_id, payload }
    }
}

static REDIS_AVRO_TYPE: RedisType = RedisType::new(
    "redisavro",
    0,
    raw::RedisModuleTypeMethods {
        version: raw::REDISMODULE_TYPE_METHOD_VERSION as u64,
        rdb_load: Some(avro_rdb_load),
        rdb_save: Some(avro_rdb_save),
        aof_rewrite: Some(avro_aof_rewrite),
        mem_usage: Some(avro_mem_usage),
        digest: None,
        free: Some(avro_value_free),
        aux_load: Some(avro_aux_load),
        aux_save: Some(avro_aux_save),
        aux_save2: None,
        aux_save_triggers: (raw::Aux::Before as c_int) | (raw::Aux::After as c_int),
        free_effort: None,
        unlink: None,
        copy: None,
        defrag: None,
        copy2: None,
        free_effort2: None,
        mem_usage2: None,
        unlink2: None,
    },
);

fn as_redis_err(message: impl Into<String>) -> RedisError {
    let msg = message.into();
    if msg.starts_with("ERR ") {
        RedisError::String(msg)
    } else {
        RedisError::String(format!("ERR {}", msg))
    }
}

fn default_schema_id(schema_name: &str) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(schema_name.as_bytes());
    hasher.finalize()
}

fn remember_schema_name(schema_id: u32, schema_name: &str) {
    let mut registry = SCHEMA_NAME_BY_ID.write().unwrap();
    registry.insert(schema_id, schema_name.to_string());
}

fn remember_schema_id(schema_name: &str, schema_id: u32) {
    let mut registry = SCHEMA_ID_BY_NAME.write().unwrap();
    registry.insert(schema_name.to_string(), schema_id);
}

fn insert_schema_entry(
    schema_name: &str,
    schema_id: u32,
    schema_json: String,
    schema: Arc<Schema>,
) {
    let mut store = SCHEMA_STORE.write().unwrap();
    store.insert(
        schema_name.to_string(),
        SchemaEntry {
            id: schema_id,
            schema: Arc::clone(&schema),
            json: schema_json,
        },
    );
    remember_schema_name(schema_id, schema_name);
    remember_schema_id(schema_name, schema_id);
}

fn schema_entry(schema_name: &str) -> Option<Arc<Schema>> {
    let store = SCHEMA_STORE.read().unwrap();
    store
        .get(schema_name)
        .map(|entry| Arc::clone(&entry.schema))
}

fn snapshot_schemas() -> Vec<(String, u32, String)> {
    let store = SCHEMA_STORE.read().unwrap();
    store
        .iter()
        .map(|(name, entry)| (name.clone(), entry.id, entry.json.clone()))
        .collect()
}

fn clear_schema_store() {
    SCHEMA_STORE.write().unwrap().clear();
    SCHEMA_NAME_BY_ID.write().unwrap().clear();
    SCHEMA_ID_BY_NAME.write().unwrap().clear();
}

fn ensure_schema_id_slot_available(schema_id: u32, schema_name: &str) -> RedisResult<()> {
    let registry = SCHEMA_NAME_BY_ID.read().unwrap();
    if let Some(existing) = registry.get(&schema_id) {
        if existing != schema_name {
            return Err(as_redis_err(format!(
                "Schema id collision: '{}' conflicts with existing schema '{}'",
                schema_name, existing
            )));
        }
    }
    Ok(())
}

fn ensure_schema_name_available(schema_name: &str, schema_id: u32) -> RedisResult<()> {
    let registry = SCHEMA_ID_BY_NAME.read().unwrap();
    if let Some(existing) = registry.get(schema_name) {
        if *existing != schema_id {
            return Err(as_redis_err(format!(
                "Schema name '{}' already registered with id {}",
                schema_name, existing
            )));
        }
    }
    Ok(())
}

fn schema_id_from_cache(schema_name: &str) -> Option<u32> {
    let registry = SCHEMA_ID_BY_NAME.read().unwrap();
    registry.get(schema_name).copied()
}

/// Resolves a schema ID from a logical schema name, registering the mapping if needed.
fn schema_id_from_name(_ctx: &Context, schema_name: &str) -> RedisResult<u32> {
    if let Some(id) = schema_id_from_cache(schema_name) {
        return Ok(id);
    }
    Err(as_redis_err(format!("Schema '{}' not found", schema_name)))
}

fn schema_name_from_id(schema_id: u32) -> RedisResult<String> {
    let registry = SCHEMA_NAME_BY_ID.read().unwrap();
    if let Some(name) = registry.get(&schema_id) {
        return Ok(name.clone());
    }
    Err(as_redis_err(format!(
        "Schema id '{}' not registered. Run AVRO.SCHEMA.ADD to register the schema again.",
        schema_id
    )))
}

fn avro_roundtrip_matches(
    schema: &Schema,
    stored_bytes: &[u8],
    value: &Value,
) -> Result<bool, serde_avro_fast::ser::SerError> {
    let mut serializer = SerializerConfig::new(schema);
    let encoded = serde_avro_fast::to_datum_vec(value, &mut serializer)?;
    Ok(encoded == stored_bytes)
}

const SCHEMA_AUX_VERSION: u64 = 1;

unsafe extern "C" fn avro_value_free(value: *mut c_void) {
    if !value.is_null() {
        drop(Box::from_raw(value.cast::<AvroValue>()));
    }
}

unsafe extern "C" fn avro_mem_usage(value: *const c_void) -> usize {
    if value.is_null() {
        return 0;
    }
    let value = &*(value.cast::<AvroValue>());
    std::mem::size_of::<AvroValue>() + value.payload.len()
}

unsafe extern "C" fn avro_rdb_save(rdb: *mut raw::RedisModuleIO, value: *mut c_void) {
    if value.is_null() {
        return;
    }
    let value = &*(value.cast::<AvroValue>());
    raw::RedisModule_SaveUnsigned.unwrap()(rdb, value.schema_id as u64);
    raw::RedisModule_SaveStringBuffer.unwrap()(
        rdb,
        value.payload.as_ptr() as *const c_char,
        value.payload.len(),
    );
}

unsafe extern "C" fn avro_rdb_load(rdb: *mut raw::RedisModuleIO, _encver: c_int) -> *mut c_void {
    let schema_id = raw::RedisModule_LoadUnsigned.unwrap()(rdb) as u32;
    let mut payload_len = 0usize;
    let payload_ptr = raw::RedisModule_LoadStringBuffer.unwrap()(rdb, &mut payload_len);
    if payload_ptr.is_null() {
        return std::ptr::null_mut();
    }
    let payload = slice::from_raw_parts(payload_ptr as *const u8, payload_len).to_vec();
    raw::RedisModule_Free.unwrap()(payload_ptr.cast());

    Box::into_raw(Box::new(AvroValue::new(schema_id, payload))).cast::<c_void>()
}

unsafe extern "C" fn avro_aux_save(rdb: *mut raw::RedisModuleIO, _when: c_int) {
    raw::RedisModule_SaveUnsigned.unwrap()(rdb, SCHEMA_AUX_VERSION);
    let entries = snapshot_schemas();
    raw::RedisModule_SaveUnsigned.unwrap()(rdb, entries.len() as u64);
    for (name, schema_id, json) in entries {
        raw::RedisModule_SaveUnsigned.unwrap()(rdb, schema_id as u64);
        raw::RedisModule_SaveStringBuffer.unwrap()(rdb, name.as_ptr() as *const c_char, name.len());
        raw::RedisModule_SaveStringBuffer.unwrap()(rdb, json.as_ptr() as *const c_char, json.len());
    }
}

unsafe extern "C" fn avro_aux_load(
    rdb: *mut raw::RedisModuleIO,
    encver: c_int,
    _when: c_int,
) -> c_int {
    if encver != 0 {
        return raw::Status::Err as c_int;
    }
    let version = raw::RedisModule_LoadUnsigned.unwrap()(rdb);
    if version != SCHEMA_AUX_VERSION {
        return raw::Status::Err as c_int;
    }
    let count = raw::RedisModule_LoadUnsigned.unwrap()(rdb);
    clear_schema_store();
    for _ in 0..count {
        let schema_id = raw::RedisModule_LoadUnsigned.unwrap()(rdb) as u32;

        let mut name_len = 0usize;
        let name_ptr = raw::RedisModule_LoadStringBuffer.unwrap()(rdb, &mut name_len);
        if name_ptr.is_null() {
            return raw::Status::Err as c_int;
        }
        let name_slice = slice::from_raw_parts(name_ptr as *const u8, name_len);
        let schema_name = match String::from_utf8(name_slice.to_vec()) {
            Ok(s) => s,
            Err(_) => {
                raw::RedisModule_Free.unwrap()(name_ptr.cast());
                return raw::Status::Err as c_int;
            }
        };
        raw::RedisModule_Free.unwrap()(name_ptr.cast());

        let mut json_len = 0usize;
        let json_ptr = raw::RedisModule_LoadStringBuffer.unwrap()(rdb, &mut json_len);
        if json_ptr.is_null() {
            return raw::Status::Err as c_int;
        }
        let json_slice = slice::from_raw_parts(json_ptr as *const u8, json_len);
        let schema_json = match String::from_utf8(json_slice.to_vec()) {
            Ok(s) => s,
            Err(_) => {
                raw::RedisModule_Free.unwrap()(json_ptr.cast());
                return raw::Status::Err as c_int;
            }
        };
        raw::RedisModule_Free.unwrap()(json_ptr.cast());

        let schema: Schema = match schema_json.parse() {
            Ok(sc) => sc,
            Err(_) => return raw::Status::Err as c_int,
        };
        insert_schema_entry(&schema_name, schema_id, schema_json, Arc::new(schema));
    }
    raw::Status::Ok as c_int
}

unsafe extern "C" fn avro_aof_rewrite(
    aof: *mut raw::RedisModuleIO,
    key: *mut raw::RedisModuleString,
    value: *mut c_void,
) {
    if value.is_null() {
        return;
    }
    let value = &*(value.cast::<AvroValue>());
    let ctx_ptr = raw::RedisModule_GetContextFromIO.unwrap()(aof);
    let ctx = Context::new(ctx_ptr);
    ctx.auto_memory();
    let schema_name = match schema_name_from_id(value.schema_id) {
        Ok(name) => name,
        Err(err) => {
            ctx.log_warning(
                format!(
                    "AVRO.AOF: failed to resolve schema id {} during rewrite: {}",
                    value.schema_id, err
                )
                .as_str(),
            );
            return;
        }
    };
    let schema = raw::RedisModule_CreateString.unwrap()(
        ctx_ptr,
        schema_name.as_ptr() as *const c_char,
        schema_name.len(),
    );
    let payload = raw::RedisModule_CreateString.unwrap()(
        ctx_ptr,
        value.payload.as_ptr() as *const c_char,
        value.payload.len(),
    );
    raw::RedisModule_EmitAOF.unwrap()(
        aof,
        c"AVRO.SET".as_ptr(),
        c"sss".as_ptr(),
        key,
        schema,
        payload,
    );
    raw::RedisModule_FreeString.unwrap()(ctx_ptr, schema);
    raw::RedisModule_FreeString.unwrap()(ctx_ptr, payload);
}

fn ensure_schema_matches(
    ctx: &Context,
    stored_id: u32,
    requested: &str,
    key: &str,
) -> RedisResult<()> {
    let requested_id = schema_id_from_name(ctx, requested)?;
    if stored_id == requested_id {
        return Ok(());
    }

    let stored_label = match schema_name_from_id(stored_id) {
        Ok(name) => name,
        Err(_) => format!("#{}", stored_id),
    };

    Err(as_redis_err(format!(
        "Schema mismatch for key '{}': stored '{}', requested '{}'",
        key, stored_label, requested
    )))
}

fn get_schema(_ctx: &Context, schema_name: &str) -> Result<Arc<Schema>, RedisError> {
    if let Some(schema) = schema_entry(schema_name) {
        return Ok(schema);
    }

    Err(as_redis_err(format!("Schema '{}' not found", schema_name)))
}

/// Registers or updates an Avro schema under a logical name.
fn schema_add(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() != 3 && args.len() != 5 {
        return Err(RedisError::WrongArity);
    }
    let schema_name = args[1].to_string();
    let schema_json = args[2].to_string();
    let schema_id = if args.len() == 5 {
        let option = args[3].to_string();
        if !option.eq_ignore_ascii_case("ID") {
            return Err(as_redis_err("Expected optional 'ID <number>' suffix"));
        }
        args[4]
            .to_string()
            .parse::<u32>()
            .map_err(|_| as_redis_err("Invalid schema id"))?
    } else {
        default_schema_id(&schema_name)
    };

    // Validate schema JSON before storing.
    let schema: Schema = schema_json
        .parse()
        .map_err(|e| as_redis_err(format!("Invalid Avro schema: {}", e)))?;
    let schema = Arc::new(schema);

    ensure_schema_id_slot_available(schema_id, &schema_name)?;
    ensure_schema_name_available(&schema_name, schema_id)?;

    insert_schema_entry(&schema_name, schema_id, schema_json, Arc::clone(&schema));

    ctx.log_notice(format!("Registered schema: {}", schema_name).as_str());
    Ok(RedisValue::SimpleString("OK".to_string()))
}

/// Writes or updates a key with the provided schema id and payload.
fn upsert_avro_value(
    ctx: &Context,
    key_name: &RedisString,
    schema_id: u32,
    payload: Vec<u8>,
) -> Result<(), RedisError> {
    let key = ctx.open_key_writable(key_name);
    match key.get_value::<AvroValue>(&REDIS_AVRO_TYPE)? {
        Some(value) => {
            value.schema_id = schema_id;
            value.payload = payload;
        }
        None => {
            key.set_value(&REDIS_AVRO_TYPE, AvroValue::new(schema_id, payload))?;
        }
    }
    Ok(())
}

/// AVRO.SET key schema [TRUSTED] payload – writes raw Avro bytes.
fn avro_raw_set(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 4 {
        return Err(RedisError::WrongArity);
    }
    if args.len() > 5 {
        return Err(RedisError::WrongArity);
    }

    let key = &args[1];
    let schema_name = args[2].to_string();
    let schema_id = schema_id_from_name(ctx, &schema_name)?;

    let (payload_idx, trusted) = if args.len() == 5 {
        let flag = args[3].to_string();
        if flag.eq_ignore_ascii_case("TRUSTED") {
            (4, true)
        } else {
            return Err(as_redis_err("Invalid flag. Use TRUSTED or omit flag."));
        }
    } else {
        (3, false)
    };

    let payload = args[payload_idx].to_vec();

    let schema = get_schema(ctx, &schema_name)?;

    if !trusted {
        let decoded: Value = serde_avro_fast::from_datum_slice(&payload, schema.as_ref())
            .map_err(|e| as_redis_err(format!("Invalid Avro payload: {}", e)))?;
        let matches = avro_roundtrip_matches(schema.as_ref(), &payload, &decoded)
            .map_err(|e| as_redis_err(format!("Invalid Avro payload: {}", e)))?;
        if !matches {
            return Err(as_redis_err("Invalid Avro payload: Corrupted Avro payload"));
        }
    }

    upsert_avro_value(ctx, key, schema_id, payload)?;
    Ok(RedisValue::SimpleString("OK".to_string()))
}

/// AVRO.JSON.SET key schema json – parses JSON and stores it as Avro.
fn avro_json_set(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 4 {
        return Err(RedisError::WrongArity);
    }
    let key = &args[1];
    let schema_name = args[2].to_string();
    let schema_id = schema_id_from_name(ctx, &schema_name)?;
    let json_input = args[3].to_string();

    // 1. Load schema or return “Schema not found”.
    let schema = get_schema(ctx, &schema_name)?;

    // 2. Parse JSON input.
    let json_val: Value = serde_json::from_str(&json_input)
        .map_err(|e| as_redis_err(format!("Invalid JSON: {}", e)))?;

    // 3. Serialize to Avro bytes.
    let mut serializer = SerializerConfig::new(schema.as_ref());
    let mut encoded_bytes = serde_avro_fast::to_datum_vec(&json_val, &mut serializer)
        .map_err(|e| as_redis_err(format!("Serialization failed: {}", e)))?;
    encoded_bytes.shrink_to_fit();

    upsert_avro_value(ctx, key, schema_id, encoded_bytes)?;

    Ok(RedisValue::SimpleString("OK".to_string()))
}

/// AVRO.GET key schema – returns the stored Avro payload.
fn avro_raw_get(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    let key_name = args[1].to_string();
    let schema_name = args[2].to_string();

    let key = ctx.open_key(&args[1]);
    let value = match key.get_value::<AvroValue>(&REDIS_AVRO_TYPE)? {
        Some(v) => v,
        None => return Ok(RedisValue::Null),
    };
    ensure_schema_matches(ctx, value.schema_id, &schema_name, &key_name)?;

    let schema = get_schema(ctx, &schema_name)?;
    let decoded: Value = serde_avro_fast::from_datum_slice(&value.payload, schema.as_ref())
        .map_err(|e| as_redis_err(format!("Deserialization failed: {}", e)))?;
    let matches = avro_roundtrip_matches(schema.as_ref(), &value.payload, &decoded)
        .map_err(|e| as_redis_err(format!("Deserialization failed: {}", e)))?;
    if !matches {
        return Err(as_redis_err(
            "Deserialization failed: Corrupted Avro payload",
        ));
    }

    Ok(RedisValue::StringBuffer(value.payload.clone()))
}

/// AVRO.JSON.GET key schema [path] – returns the value as JSON.
fn avro_json_get(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    let key_arg = &args[1];
    let schema_name = args[2].to_string();
    let path = if args.len() > 3 {
        Some(args[3].to_string())
    } else {
        None
    };

    let key_name = key_arg.to_string();
    let key = ctx.open_key(key_arg);
    let value = match key.get_value::<AvroValue>(&REDIS_AVRO_TYPE)? {
        Some(v) => v,
        None => return Ok(RedisValue::Null),
    };
    ensure_schema_matches(ctx, value.schema_id, &schema_name, &key_name)?;

    let schema = get_schema(ctx, &schema_name)?;

    // 3. Deserialize Avro → serde_json::Value.
    let json_val: Value = serde_avro_fast::from_datum_slice(&value.payload, schema.as_ref())
        .map_err(|e| as_redis_err(format!("Deserialization failed: {}", e)))?;
    let matches = avro_roundtrip_matches(schema.as_ref(), &value.payload, &json_val)
        .map_err(|e| as_redis_err(format!("Deserialization failed: {}", e)))?;
    if !matches {
        return Err(as_redis_err(
            "Deserialization failed: Corrupted Avro payload",
        ));
    }

    // 5. Apply JSON Pointer path if provided.
    let result_json = if let Some(p) = path {
        match json_val.pointer(&p) {
            Some(v) => v,
            None => return Ok(RedisValue::Null),
        }
    } else {
        &json_val
    };

    // 6. Return as a JSON string.
    let json_str = serde_json::to_string(result_json).unwrap();
    Ok(RedisValue::SimpleString(json_str))
}

/// AVRO.JSON.MGET schema path key1 [key2 ...] – batched JSON fetch.
fn avro_json_mget(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    // Usage: AVRO.JSON.MGET <schema_name> <path> <key1> [key2 ...]
    if args.len() < 4 {
        return Err(RedisError::WrongArity);
    }

    let schema_name = args[1].to_string();
    let path_str = args[2].to_string();
    let keys = &args[3..];

    // Load schema once per batch.
    let schema = get_schema(ctx, &schema_name)?;

    let mut response = Vec::with_capacity(keys.len());

    for key_arg in keys {
        let key_name = key_arg.to_string();
        let key = ctx.open_key(key_arg);
        let value = match key.get_value::<AvroValue>(&REDIS_AVRO_TYPE)? {
            Some(v) => v,
            None => {
                response.push(RedisValue::Null);
                continue;
            }
        };
        ensure_schema_matches(ctx, value.schema_id, &schema_name, &key_name)?;

        match serde_avro_fast::from_datum_slice::<Value>(&value.payload, schema.as_ref()) {
            Ok(json_val) => {
                match avro_roundtrip_matches(schema.as_ref(), &value.payload, &json_val) {
                    Ok(true) => {}
                    Ok(false) => {
                        ctx.log_warning(
                            format!(
                                "AVRO.JSON.MGET: corrupted Avro at key '{}': roundtrip mismatch. Returning NULL.",
                                key_name
                            )
                            .as_str(),
                        );
                        response.push(RedisValue::Null);
                        continue;
                    }
                    Err(e) => {
                        ctx.log_warning(
                            format!(
                                "AVRO.JSON.MGET: failed to verify Avro bytes for key '{}': {}. Returning NULL.",
                                key_name, e
                            )
                            .as_str(),
                        );
                        response.push(RedisValue::Null);
                        continue;
                    }
                }

                let target_val = if path_str == "." || path_str.is_empty() {
                    &json_val
                } else {
                    match json_val.pointer(&path_str) {
                        Some(v) => v,
                        None => &Value::Null,
                    }
                };

                match serde_json::to_string(target_val) {
                    Ok(s) => response.push(RedisValue::SimpleString(s)),
                    Err(e) => {
                        ctx.log_warning(
                            format!(
                                "AVRO.JSON.MGET: failed to serialize JSON for key '{}': {}. Returning NULL.",
                                key_name, e
                            )
                            .as_str(),
                        );
                        response.push(RedisValue::Null);
                    }
                }
            }
            Err(e) => {
                ctx.log_warning(
                    format!(
                        "AVRO.JSON.MGET: corrupted Avro at key '{}': {}. Returning NULL.",
                        key_name, e
                    )
                    .as_str(),
                );
                response.push(RedisValue::Null);
            }
        }
    }

    Ok(RedisValue::Array(response))
}

/// AVRO.MGET schema key1 [key2 ...] – batched Avro payload fetch.
fn avro_raw_mget(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    let schema_name = args[1].to_string();
    let keys = &args[2..];

    let schema = get_schema(ctx, &schema_name)?;
    let mut response = Vec::with_capacity(keys.len());

    for key_arg in keys {
        let key_name = key_arg.to_string();
        let key = ctx.open_key(key_arg);
        let value = match key.get_value::<AvroValue>(&REDIS_AVRO_TYPE)? {
            Some(v) => v,
            None => {
                response.push(RedisValue::Null);
                continue;
            }
        };
        ensure_schema_matches(ctx, value.schema_id, &schema_name, &key_name)?;

        match serde_avro_fast::from_datum_slice::<Value>(&value.payload, schema.as_ref()) {
            Ok(decoded) => {
                match avro_roundtrip_matches(schema.as_ref(), &value.payload, &decoded) {
                    Ok(true) => response.push(RedisValue::StringBuffer(value.payload.clone())),
                    Ok(false) => {
                        ctx.log_warning(
                            format!(
                                "AVRO.MGET: corrupted Avro at key '{}': roundtrip mismatch. Returning NULL.",
                                key_name
                            )
                            .as_str(),
                        );
                        response.push(RedisValue::Null);
                    }
                    Err(e) => {
                        ctx.log_warning(
                            format!(
                                "AVRO.MGET: failed to verify Avro bytes for key '{}': {}. Returning NULL.",
                                key_name, e
                            )
                            .as_str(),
                        );
                        response.push(RedisValue::Null);
                    }
                }
            }
            Err(e) => {
                ctx.log_warning(
                    format!(
                        "AVRO.MGET: corrupted Avro at key '{}': {}. Returning NULL.",
                        key_name, e
                    )
                    .as_str(),
                );
                response.push(RedisValue::Null);
            }
        }
    }

    Ok(RedisValue::Array(response))
}

fn avro_ping(_ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    Ok(RedisValue::SimpleString("PONG from AVRO".to_string()))
}

redis_module! {
    name: "redis_avro",
    version: 1,
    allocator: (redis_module::alloc::RedisAlloc, redis_module::alloc::RedisAlloc),
    data_types: [
        REDIS_AVRO_TYPE,
    ],
    commands: [
        ["avro.ping", avro_ping, "readonly", 0, 0, 0],
        ["avro.schema.add", schema_add, "write", 0, 0, 0],
        ["avro.set", avro_raw_set, "write", 1, 1, 1],
        ["avro.json.set", avro_json_set, "write", 1, 1, 1],
        ["avro.get", avro_raw_get, "readonly", 1, 1, 1],
        ["avro.json.get", avro_json_get, "readonly", 1, 1, 1],
        ["avro.mget", avro_raw_mget, "readonly", 2, -1, 1],
        ["avro.json.mget", avro_json_mget, "readonly", 3, -1, 1],
    ],
}
