use std::env;
use std::path::PathBuf;
use std::process::{self, Child, Command};
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Once;
use std::thread;
use std::time::Duration;

use redis::{ConnectionAddr, ConnectionInfo, RedisConnectionInfo};
use serde_avro_fast::{ser::SerializerConfig, Schema};

// Глобальные переменные для генерации портов
static NEXT_PORT: AtomicU16 = AtomicU16::new(0);
static INIT: Once = Once::new();

// Хелпер: выдает уникальный порт на основе PID процесса
fn get_free_port() -> u16 {
    INIT.call_once(|| {
        let pid = process::id();
        let start_port = 10000 + (pid % 10000) as u16;
        NEXT_PORT.store(start_port, Ordering::Relaxed);
    });

    NEXT_PORT.fetch_add(1, Ordering::Relaxed)
}

struct RedisServer {
    process: Child,
    client: redis::Client,
    socket_path: PathBuf,
}

impl RedisServer {
    fn new() -> Self {
        let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();

        #[cfg(target_os = "macos")]
        let ext = "dylib";
        #[cfg(target_os = "linux")]
        let ext = "so";
        #[cfg(windows)]
        let ext = "dll";

        let module_path = PathBuf::from(&manifest_dir)
            .join("target/release")
            .join(format!("libredis_avro.{}", ext));

        if !module_path.exists() {
            panic!(
                "Module not found at {:?}. Did you run 'cargo build --release'?",
                module_path
            );
        }

        let unique = get_free_port();
        let socket_path = std::env::temp_dir().join(format!("redis_avro_test_{}.sock", unique));
        let _ = std::fs::remove_file(&socket_path);

        // --save "" отключает дамп на диск
        let process = Command::new("redis-server")
            .arg("--port")
            .arg("0")
            .arg("--unixsocket")
            .arg(&socket_path)
            .arg("--unixsocketperm")
            .arg("700")
            .arg("--loadmodule")
            .arg(module_path)
            .arg("--save")
            .arg("")
            .spawn()
            .expect("Failed to start redis-server. Make sure it is installed and in PATH");

        // Даем время на старт
        thread::sleep(Duration::from_millis(500));

        let connection_info = ConnectionInfo {
            addr: ConnectionAddr::Unix(socket_path.clone()),
            redis: RedisConnectionInfo::default(),
        };
        let client = redis::Client::open(connection_info).unwrap();

        RedisServer {
            process,
            client,
            socket_path,
        }
    }

    fn get_con(&self) -> redis::Connection {
        self.client
            .get_connection()
            .expect("Failed to connect to Redis")
    }
}

impl Drop for RedisServer {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
        let _ = std::fs::remove_file(&self.socket_path);
    }
}

fn encode_avro_payload(schema_json: &str, json_payload: &str) -> Vec<u8> {
    let schema: Schema = schema_json.parse().expect("schema parse failed");
    let json_val: serde_json::Value =
        serde_json::from_str(json_payload).expect("json payload parse failed");
    let mut serializer = SerializerConfig::new(&schema);
    serde_avro_fast::to_datum_vec(&json_val, &mut serializer).expect("avro serialization failed")
}
// --- ТЕСТЫ ---

#[test]
fn test_ping_module() {
    let server = RedisServer::new();
    let mut con = server.get_con();
    let response: String = redis::cmd("AVRO.PING").query(&mut con).expect("Cmd failed");
    assert_eq!(response, "PONG from AVRO");
}

#[test]
fn test_schema_add() {
    let server = RedisServer::new();
    let mut con = server.get_con();
    let schema_json = r#"{"type":"record","name":"User","fields":[{"name":"a","type":"int"}]}"#;
    let response: String = redis::cmd("AVRO.SCHEMA.ADD")
        .arg("user_v1")
        .arg(schema_json)
        .query(&mut con)
        .expect("Cmd failed");
    assert_eq!(response, "OK");
}

#[test]
fn test_schema_registry_hidden_from_keyspace() {
    let server = RedisServer::new();
    let mut con = server.get_con();
    let schema_json = r#"{"type":"record","name":"Secret","fields":[{"name":"id","type":"int"}]}"#;
    let _: () = redis::cmd("AVRO.SCHEMA.ADD")
        .arg("hidden_v1")
        .arg(schema_json)
        .query(&mut con)
        .expect("Schema add failed");

    let keys: Vec<String> = redis::cmd("KEYS").arg("*").query(&mut con).unwrap();
    assert!(
        keys.is_empty(),
        "Expected schema registry to stay hidden, but keys exist: {:?}",
        keys
    );
}

#[test]
fn test_set_get_cycle() {
    let server = RedisServer::new();
    let mut con = server.get_con();
    let schema = r#"{"type":"record","name":"User","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"}]}"#;
    let _: () = redis::cmd("AVRO.SCHEMA.ADD")
        .arg("user_v1")
        .arg(schema)
        .query(&mut con)
        .unwrap();

    let json_data = r#"{"name": "Alice", "age": 30}"#;
    let _: () = redis::cmd("AVRO.JSON.SET")
        .arg("u:1")
        .arg("user_v1")
        .arg(json_data)
        .query(&mut con)
        .unwrap();

    let response: String = redis::cmd("AVRO.JSON.GET")
        .arg("u:1")
        .arg("user_v1")
        .query(&mut con)
        .unwrap();
    assert!(response.contains("Alice"));
}

#[test]
fn test_path_access() {
    let server = RedisServer::new();
    let mut con = server.get_con();
    let schema = r#"{"type":"record","name":"User","fields":[{"name":"stats","type":{"type":"record","name":"Stats","fields":[{"name":"score","type":"int"}]}}]}"#;
    let _: () = redis::cmd("AVRO.SCHEMA.ADD")
        .arg("stats_v1")
        .arg(schema)
        .query(&mut con)
        .unwrap();

    let json_data = r#"{"stats": {"score": 999}}"#;
    let _: () = redis::cmd("AVRO.JSON.SET")
        .arg("p:1")
        .arg("stats_v1")
        .arg(json_data)
        .query(&mut con)
        .unwrap();

    let response: String = redis::cmd("AVRO.JSON.GET")
        .arg("p:1")
        .arg("stats_v1")
        .arg("/stats/score")
        .query(&mut con)
        .unwrap();
    assert_eq!(response, "999");
}

#[test]
fn test_mget() {
    let server = RedisServer::new();
    let mut con = server.get_con();

    // 1. Создаем схему
    let schema = r#"{"type":"record","name":"Item","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}"#;
    let _: () = redis::cmd("AVRO.SCHEMA.ADD")
        .arg("item_v1")
        .arg(schema)
        .query(&mut con)
        .unwrap();

    // 2. Пишем два ключа
    let item1 = r#"{"id": 100, "name": "Apple"}"#;
    let item2 = r#"{"id": 101, "name": "Banana"}"#;
    let _: () = redis::cmd("AVRO.JSON.SET")
        .arg("i:100")
        .arg("item_v1")
        .arg(item1)
        .query(&mut con)
        .unwrap();
    let _: () = redis::cmd("AVRO.JSON.SET")
        .arg("i:101")
        .arg("item_v1")
        .arg(item2)
        .query(&mut con)
        .unwrap();

    // 3. AVRO.JSON.MGET: запрос двух существующих ключей и одного несуществующего (i:999)
    // Формат: AVRO.JSON.MGET schema path key1 key2 ...
    let result: Vec<Option<String>> = redis::cmd("AVRO.JSON.MGET")
        .arg("item_v1")
        .arg(".") // . означает "весь объект"
        .arg("i:100")
        .arg("i:101")
        .arg("i:999") // ключа нет
        .query(&mut con)
        .expect("MGET failed");

    assert_eq!(result.len(), 3);

    // Проверка i:100
    let val1 = result[0].as_ref().expect("i:100 should exist");
    assert!(val1.contains("Apple"));
    assert!(val1.contains("100"));

    // Проверка i:101
    let val2 = result[1].as_ref().expect("i:101 should exist");
    assert!(val2.contains("Banana"));
    assert!(val2.contains("101"));

    // Проверка i:999 (должен быть None/Nil)
    assert!(result[2].is_none(), "i:999 should be nil");

    // 4. AVRO.JSON.MGET с JSON Path (только поле name)
    let names: Vec<Option<String>> = redis::cmd("AVRO.JSON.MGET")
        .arg("item_v1")
        .arg("/name")
        .arg("i:100")
        .arg("i:101")
        .query(&mut con)
        .expect("MGET path failed");

    assert_eq!(names[0].as_ref().unwrap(), "\"Apple\"");
    assert_eq!(names[1].as_ref().unwrap(), "\"Banana\"");
}

#[test]
fn test_get_missing_key_returns_null() {
    let server = RedisServer::new();
    let mut con = server.get_con();

    let schema = r#"{"type":"record","name":"User","fields":[{"name":"name","type":"string"}]}"#;
    let _: () = redis::cmd("AVRO.SCHEMA.ADD")
        .arg("user_v1")
        .arg(schema)
        .query(&mut con)
        .unwrap();

    // Ключ не существует
    let res: Option<String> = redis::cmd("AVRO.JSON.GET")
        .arg("missing_key")
        .arg("user_v1")
        .query(&mut con)
        .unwrap();

    assert!(res.is_none(), "Expected NULL for missing key");
}

#[test]
fn test_get_corrupted_avro_returns_error() {
    let server = RedisServer::new();
    let mut con = server.get_con();

    let schema = r#"{"type":"record","name":"User","fields":[{"name":"name","type":"string"}]}"#;
    let _: () = redis::cmd("AVRO.SCHEMA.ADD")
        .arg("user_v1")
        .arg(schema)
        .query(&mut con)
        .unwrap();

    // Пишем в ключ невалидный бинарь (не Avro)
    let _: () = redis::cmd("AVRO.SET")
        .arg("broken:1")
        .arg("user_v1")
        .arg("TRUSTED")
        .arg("this is not avro")
        .query(&mut con)
        .unwrap();

    let res: redis::RedisResult<String> = redis::cmd("AVRO.JSON.GET")
        .arg("broken:1")
        .arg("user_v1")
        .query(&mut con);

    match res {
        Ok(_) => panic!("Expected deserialization error, got OK"),
        Err(e) => {
            let msg = format!("{}", e);
            assert!(
                msg.contains("Deserialization failed"),
                "Unexpected error: {}",
                msg
            );
        }
    }
}

#[test]
fn test_raw_get_corrupted_avro_returns_error() {
    let server = RedisServer::new();
    let mut con = server.get_con();

    let schema = r#"{"type":"record","name":"User","fields":[{"name":"name","type":"string"}]}"#;
    let _: () = redis::cmd("AVRO.SCHEMA.ADD")
        .arg("user_v1")
        .arg(schema)
        .query(&mut con)
        .unwrap();

    let _: () = redis::cmd("AVRO.SET")
        .arg("broken:raw")
        .arg("user_v1")
        .arg("TRUSTED")
        .arg("this is not avro")
        .query(&mut con)
        .unwrap();

    let res: redis::RedisResult<Vec<u8>> = redis::cmd("AVRO.GET")
        .arg("broken:raw")
        .arg("user_v1")
        .query(&mut con);

    match res {
        Ok(_) => panic!("Expected deserialization error, got OK"),
        Err(e) => {
            let msg = format!("{}", e);
            assert!(
                msg.contains("Deserialization failed"),
                "Unexpected error: {}",
                msg
            );
        }
    }
}

#[test]
fn test_mget_corrupted_entries_return_null() {
    let server = RedisServer::new();
    let mut con = server.get_con();

    // Схема
    let schema = r#"{"type":"record","name":"Item","fields":[{"name":"id","type":"int"}]}"#;
    let _: () = redis::cmd("AVRO.SCHEMA.ADD")
        .arg("item_v1")
        .arg(schema)
        .query(&mut con)
        .unwrap();

    // Валидная запись
    let item = r#"{"id": 1}"#;
    let _: () = redis::cmd("AVRO.JSON.SET")
        .arg("ok:1")
        .arg("item_v1")
        .arg(item)
        .query(&mut con)
        .unwrap();

    // Битая запись
    let _: () = redis::cmd("AVRO.SET")
        .arg("bad:1")
        .arg("item_v1")
        .arg("TRUSTED")
        .arg("not avro at all")
        .query(&mut con)
        .unwrap();

    let result: Vec<Option<String>> = redis::cmd("AVRO.JSON.MGET")
        .arg("item_v1")
        .arg(".")
        .arg("ok:1")
        .arg("bad:1")
        .arg("missing:1")
        .query(&mut con)
        .expect("MGET failed");

    assert_eq!(result.len(), 3);

    // ok:1 — должен вернуться
    assert!(result[0].is_some());
    // bad:1 — NULL
    assert!(result[1].is_none(), "Corrupted entry should be NULL");
    // missing:1 — NULL
    assert!(result[2].is_none(), "Missing key should be NULL");
}

#[test]
fn test_raw_mget_corrupted_entries_return_null() {
    let server = RedisServer::new();
    let mut con = server.get_con();

    let schema = r#"{"type":"record","name":"Item","fields":[{"name":"id","type":"int"}]}"#;
    let _: () = redis::cmd("AVRO.SCHEMA.ADD")
        .arg("item_v1")
        .arg(schema)
        .query(&mut con)
        .unwrap();

    let item = r#"{"id": 42}"#;
    let encoded = encode_avro_payload(schema, item);
    let _: () = redis::cmd("AVRO.SET")
        .arg("raw_ok:1")
        .arg("item_v1")
        .arg(encoded.as_slice())
        .query(&mut con)
        .unwrap();

    let _: () = redis::cmd("AVRO.SET")
        .arg("raw_bad:1")
        .arg("item_v1")
        .arg("TRUSTED")
        .arg("invalid data")
        .query(&mut con)
        .unwrap();

    let ok_bytes = encoded.clone();

    let result: Vec<Option<Vec<u8>>> = redis::cmd("AVRO.MGET")
        .arg("item_v1")
        .arg("raw_ok:1")
        .arg("raw_bad:1")
        .arg("raw_missing:1")
        .query(&mut con)
        .expect("AVRO.MGET failed");

    assert_eq!(result.len(), 3);
    assert_eq!(result[0].as_ref().unwrap(), &ok_bytes);
    assert!(result[1].is_none(), "Corrupted entry should be NULL");
    assert!(result[2].is_none(), "Missing entry should be NULL");
}

#[test]
fn test_raw_set_trusted_skips_validation() {
    let server = RedisServer::new();
    let mut con = server.get_con();

    let schema = r#"{"type":"record","name":"User","fields":[{"name":"name","type":"string"}]}"#;
    let _: () = redis::cmd("AVRO.SCHEMA.ADD")
        .arg("user_v1")
        .arg(schema)
        .query(&mut con)
        .unwrap();

    let bad_payload = b"not avro";

    let res: redis::RedisResult<()> = redis::cmd("AVRO.SET")
        .arg("raw_invalid")
        .arg("user_v1")
        .arg(&bad_payload[..])
        .query(&mut con);
    assert!(
        res.is_err(),
        "Expected validation error for invalid payload"
    );

    let _: () = redis::cmd("AVRO.SET")
        .arg("raw_trusted")
        .arg("user_v1")
        .arg("TRUSTED")
        .arg(&bad_payload[..])
        .query(&mut con)
        .expect("Trusted write should succeed");

    let res: redis::RedisResult<Vec<u8>> = redis::cmd("AVRO.GET")
        .arg("raw_trusted")
        .arg("user_v1")
        .query(&mut con);
    assert!(
        res.is_err(),
        "Reading corrupted payload should still fail even if trusted"
    );
}

#[test]
fn test_raw_set_get_cycle() {
    let server = RedisServer::new();
    let mut con = server.get_con();

    let schema = r#"{"type":"record","name":"User","fields":[{"name":"name","type":"string"}]}"#;
    let _: () = redis::cmd("AVRO.SCHEMA.ADD")
        .arg("user_v1")
        .arg(schema)
        .query(&mut con)
        .unwrap();

    let json_data = r#"{"name": "RawUser"}"#;
    let encoded = encode_avro_payload(schema, json_data);

    let _: () = redis::cmd("AVRO.SET")
        .arg("raw:copy")
        .arg("user_v1")
        .arg(encoded.as_slice())
        .query(&mut con)
        .unwrap();

    let fetched_bytes: Vec<u8> = redis::cmd("AVRO.GET")
        .arg("raw:copy")
        .arg("user_v1")
        .query(&mut con)
        .expect("AVRO.GET failed");
    assert_eq!(fetched_bytes, encoded);

    let json_back: String = redis::cmd("AVRO.JSON.GET")
        .arg("raw:copy")
        .arg("user_v1")
        .query(&mut con)
        .expect("AVRO.JSON.GET failed");
    assert!(json_back.contains("RawUser"));
}

#[test]
fn test_set_with_missing_schema_fails() {
    let server = RedisServer::new();
    let mut con = server.get_con();

    let json_data = r#"{"name": "Alice"}"#;

    let res: redis::RedisResult<String> = redis::cmd("AVRO.JSON.SET")
        .arg("user:1")
        .arg("unknown_schema")
        .arg(json_data)
        .query(&mut con);

    assert!(res.is_err(), "Expected error for missing schema");
    let msg = format!("{}", res.unwrap_err());
    assert!(msg.contains("Schema 'unknown_schema' not found"));
}

#[test]
fn test_set_with_invalid_json_fails() {
    let server = RedisServer::new();
    let mut con = server.get_con();

    let schema = r#"{"type":"record","name":"User","fields":[{"name":"name","type":"string"}]}"#;
    let _: () = redis::cmd("AVRO.SCHEMA.ADD")
        .arg("user_v1")
        .arg(schema)
        .query(&mut con)
        .unwrap();

    let bad_json = r#"{invalid json"#;

    let res: redis::RedisResult<String> = redis::cmd("AVRO.JSON.SET")
        .arg("user:1")
        .arg("user_v1")
        .arg(bad_json)
        .query(&mut con);

    assert!(res.is_err(), "Expected JSON parse error");
    let msg = format!("{}", res.unwrap_err());
    assert!(msg.contains("Invalid JSON"));
}
