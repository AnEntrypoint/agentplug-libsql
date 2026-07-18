use libsql_ffi as ffi;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::ptr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;

use crate::abi::{ok_err, return_json};

static DBS: Mutex<Option<HashMap<String, DbHandle>>> = Mutex::new(None);

struct DbHandle(*mut ffi::sqlite3);
unsafe impl Send for DbHandle {}

fn open(name: &str, path: &str) -> Result<(), String> {
    let mut guard = DBS.lock().map_err(|e| e.to_string())?;
    let map = guard.get_or_insert_with(HashMap::new);
    if map.contains_key(name) {
        return Ok(());
    }
    let cpath = CString::new(path).map_err(|e| e.to_string())?;
    let mut db: *mut ffi::sqlite3 = ptr::null_mut();
    let rc = unsafe { ffi::sqlite3_open_v2(cpath.as_ptr(), &mut db, ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE, ptr::null()) };
    if rc != ffi::SQLITE_OK {
        let msg = if db.is_null() {
            format!("rc={rc}")
        } else {
            let m = unsafe { CStr::from_ptr(ffi::sqlite3_errmsg(db)).to_string_lossy().into_owned() };
            unsafe {
                ffi::sqlite3_close(db);
            }
            format!("rc={rc} msg={m}")
        };
        return Err(format!("sqlite3_open_v2 {msg}"));
    }
    map.insert(name.to_string(), DbHandle(db));
    Ok(())
}

fn close(name: &str) -> Result<(), String> {
    let mut guard = DBS.lock().map_err(|e| e.to_string())?;
    let map = match guard.as_mut() {
        Some(m) => m,
        None => return Ok(()),
    };
    if let Some(h) = map.remove(name) {
        unsafe {
            ffi::sqlite3_close(h.0);
        }
    }
    Ok(())
}

fn with_db<F, R>(name: &str, f: F) -> Result<R, String>
where
    F: FnOnce(*mut ffi::sqlite3) -> Result<R, String>,
{
    let guard = DBS.lock().map_err(|e| e.to_string())?;
    let map = guard.as_ref().ok_or_else(|| "no dbs open".to_string())?;
    let h = map.get(name).ok_or_else(|| format!("db '{name}' not open"))?;
    f(h.0)
}

fn exec(name: &str, sql: &str) -> Result<(), String> {
    with_db(name, |db| {
        let csql = CString::new(sql).map_err(|e| e.to_string())?;
        let mut err_ptr: *mut i8 = ptr::null_mut();
        let rc = unsafe { ffi::sqlite3_exec(db, csql.as_ptr(), None, ptr::null_mut(), &mut err_ptr) };
        if rc != ffi::SQLITE_OK {
            let msg = if err_ptr.is_null() {
                "unknown".to_string()
            } else {
                let s = unsafe { CStr::from_ptr(err_ptr).to_string_lossy().into_owned() };
                unsafe {
                    ffi::sqlite3_free(err_ptr as *mut _);
                }
                s
            };
            return Err(format!("exec rc={rc} msg={msg}"));
        }
        Ok(())
    })
}

fn column_value(stmt: *mut ffi::sqlite3_stmt, i: i32) -> Value {
    let ctype = unsafe { ffi::sqlite3_column_type(stmt, i) };
    match ctype {
        ffi::SQLITE_INTEGER => Value::from(unsafe { ffi::sqlite3_column_int64(stmt, i) }),
        ffi::SQLITE_FLOAT => Value::from(unsafe { ffi::sqlite3_column_double(stmt, i) }),
        ffi::SQLITE_NULL => Value::Null,
        ffi::SQLITE_TEXT => {
            let p = unsafe { ffi::sqlite3_column_text(stmt, i) };
            if p.is_null() {
                Value::Null
            } else {
                Value::String(unsafe { CStr::from_ptr(p as *const _).to_string_lossy().into_owned() })
            }
        }
        ffi::SQLITE_BLOB => {
            let n = unsafe { ffi::sqlite3_column_bytes(stmt, i) } as usize;
            let p = unsafe { ffi::sqlite3_column_blob(stmt, i) } as *const u8;
            if p.is_null() || n == 0 {
                Value::Null
            } else {
                Value::String(format!("blob:{n}b"))
            }
        }
        _ => Value::Null,
    }
}

fn query_impl(db: *mut ffi::sqlite3, sql: &str, params: &[&str]) -> Result<Value, String> {
    let csql = CString::new(sql).map_err(|e| e.to_string())?;
    let cparams: Vec<CString> = params.iter().map(|p| CString::new(*p).map_err(|e| e.to_string())).collect::<Result<Vec<_>, _>>()?;
    let mut stmt: *mut ffi::sqlite3_stmt = ptr::null_mut();
    let rc = unsafe { ffi::sqlite3_prepare_v2(db, csql.as_ptr(), -1, &mut stmt, ptr::null_mut()) };
    if rc != ffi::SQLITE_OK {
        let msg = unsafe { CStr::from_ptr(ffi::sqlite3_errmsg(db)).to_string_lossy().into_owned() };
        return Err(format!("prepare rc={rc} msg={msg}"));
    }
    for (i, cp) in cparams.iter().enumerate() {
        let rc = unsafe { ffi::sqlite3_bind_text(stmt, (i + 1) as i32, cp.as_ptr(), -1, None) };
        if rc != ffi::SQLITE_OK {
            unsafe {
                ffi::sqlite3_finalize(stmt);
            }
            return Err(format!("bind param {i} rc={rc}"));
        }
    }
    let ncols = unsafe { ffi::sqlite3_column_count(stmt) };
    let mut col_names = Vec::with_capacity(ncols as usize);
    for i in 0..ncols {
        col_names.push(unsafe { CStr::from_ptr(ffi::sqlite3_column_name(stmt, i)).to_string_lossy().into_owned() });
    }
    let mut rows: Vec<Value> = Vec::new();
    loop {
        let step = unsafe { ffi::sqlite3_step(stmt) };
        if step == ffi::SQLITE_DONE {
            break;
        }
        if step != ffi::SQLITE_ROW {
            let msg = unsafe { CStr::from_ptr(ffi::sqlite3_errmsg(db)).to_string_lossy().into_owned() };
            unsafe {
                ffi::sqlite3_finalize(stmt);
            }
            return Err(format!("step rc={step} msg={msg}"));
        }
        let mut row = serde_json::Map::new();
        for i in 0..ncols {
            row.insert(col_names[i as usize].clone(), column_value(stmt, i));
        }
        rows.push(Value::Object(row));
    }
    unsafe {
        ffi::sqlite3_finalize(stmt);
    }
    Ok(Value::Array(rows))
}

fn exec_params(name: &str, sql: &str, params: &[&str]) -> Result<(), String> {
    with_db(name, |db| {
        let csql = CString::new(sql).map_err(|e| e.to_string())?;
        let cparams: Vec<CString> = params.iter().map(|p| CString::new(*p).map_err(|e| e.to_string())).collect::<Result<Vec<_>, _>>()?;
        let mut stmt: *mut ffi::sqlite3_stmt = ptr::null_mut();
        let rc = unsafe { ffi::sqlite3_prepare_v2(db, csql.as_ptr(), -1, &mut stmt, ptr::null_mut()) };
        if rc != ffi::SQLITE_OK {
            let msg = unsafe { CStr::from_ptr(ffi::sqlite3_errmsg(db)).to_string_lossy().into_owned() };
            return Err(format!("prepare rc={rc} msg={msg}"));
        }
        for (i, cp) in cparams.iter().enumerate() {
            let rc = unsafe { ffi::sqlite3_bind_text(stmt, (i + 1) as i32, cp.as_ptr(), -1, None) };
            if rc != ffi::SQLITE_OK {
                unsafe {
                    ffi::sqlite3_finalize(stmt);
                }
                return Err(format!("bind param {i} rc={rc}"));
            }
        }
        let step = unsafe { ffi::sqlite3_step(stmt) };
        unsafe {
            ffi::sqlite3_finalize(stmt);
        }
        if step != ffi::SQLITE_DONE && step != ffi::SQLITE_ROW {
            let msg = unsafe { CStr::from_ptr(ffi::sqlite3_errmsg(db)).to_string_lossy().into_owned() };
            return Err(format!("step rc={step} msg={msg}"));
        }
        Ok(())
    })
}

fn serialize(name: &str) -> Result<Vec<u8>, String> {
    with_db(name, |db| {
        let schema = CString::new("main").unwrap();
        let mut size: i64 = 0;
        let p = unsafe { ffi::sqlite3_serialize(db, schema.as_ptr(), &mut size, 0) };
        if p.is_null() || size <= 0 {
            return Err(format!("serialize null (size={size})"));
        }
        let bytes = unsafe { std::slice::from_raw_parts(p, size as usize).to_vec() };
        unsafe {
            ffi::sqlite3_free(p as *mut _);
        }
        Ok(bytes)
    })
}

fn deserialize(name: &str, bytes: &[u8]) -> Result<(), String> {
    with_db(name, |db| {
        let schema = CString::new("main").unwrap();
        let size = bytes.len() as i64;
        let buf = unsafe { ffi::sqlite3_malloc64(size as u64) } as *mut u8;
        if buf.is_null() {
            return Err("malloc failed".to_string());
        }
        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), buf, bytes.len());
        }
        let flags = (ffi::SQLITE_DESERIALIZE_FREEONCLOSE | ffi::SQLITE_DESERIALIZE_RESIZEABLE) as u32;
        let rc = unsafe { ffi::sqlite3_deserialize(db, schema.as_ptr(), buf, size, size, flags) };
        if rc != ffi::SQLITE_OK {
            return Err(format!("deserialize rc={rc}"));
        }
        Ok(())
    })
}

// Prepared statements can't cross the plugin ABI boundary as raw pointers --
// the caller (gm.wasm) only ever sees an opaque u32 handle id, resolved back
// to the real *mut sqlite3_stmt on THIS side for every execute_bound call.
// This is the one real shape change from rs-plugkit's libsql_wasm.rs (which
// returned an owned PreparedStmt struct directly, valid only within a single
// wasm module's own address space).
struct PreparedEntry {
    db: *mut ffi::sqlite3,
    stmt: *mut ffi::sqlite3_stmt,
}
unsafe impl Send for PreparedEntry {}

impl Drop for PreparedEntry {
    fn drop(&mut self) {
        if !self.stmt.is_null() {
            unsafe {
                ffi::sqlite3_finalize(self.stmt);
            }
        }
    }
}

static PREPARED: Mutex<Option<HashMap<u32, PreparedEntry>>> = Mutex::new(None);
static NEXT_HANDLE: AtomicU32 = AtomicU32::new(1);

fn prepare_stmt(name: &str, sql: &str) -> Result<u32, String> {
    with_db(name, |db| {
        let csql = CString::new(sql).map_err(|e| e.to_string())?;
        let mut stmt: *mut ffi::sqlite3_stmt = ptr::null_mut();
        let rc = unsafe { ffi::sqlite3_prepare_v2(db, csql.as_ptr(), -1, &mut stmt, ptr::null_mut()) };
        if rc != ffi::SQLITE_OK {
            let msg = unsafe { CStr::from_ptr(ffi::sqlite3_errmsg(db)).to_string_lossy().into_owned() };
            return Err(format!("prepare rc={rc} msg={msg}"));
        }
        let handle = NEXT_HANDLE.fetch_add(1, Ordering::SeqCst);
        let mut guard = PREPARED.lock().map_err(|e| e.to_string())?;
        guard.get_or_insert_with(HashMap::new).insert(handle, PreparedEntry { db, stmt });
        Ok(handle)
    })
}

fn execute_bound(handle: u32, params: &[&str]) -> Result<(), String> {
    let guard = PREPARED.lock().map_err(|e| e.to_string())?;
    let map = guard.as_ref().ok_or_else(|| "no prepared statements".to_string())?;
    let entry = map.get(&handle).ok_or_else(|| format!("unknown prepared statement handle {handle}"))?;
    let cparams: Vec<CString> = params.iter().map(|p| CString::new(*p).map_err(|e| e.to_string())).collect::<Result<Vec<_>, _>>()?;
    unsafe {
        ffi::sqlite3_reset(entry.stmt);
        ffi::sqlite3_clear_bindings(entry.stmt);
    }
    for (i, cp) in cparams.iter().enumerate() {
        let rc = unsafe { ffi::sqlite3_bind_text(entry.stmt, (i + 1) as i32, cp.as_ptr(), -1, None) };
        if rc != ffi::SQLITE_OK {
            return Err(format!("bind param {i} rc={rc}"));
        }
    }
    let step = unsafe { ffi::sqlite3_step(entry.stmt) };
    if step != ffi::SQLITE_DONE && step != ffi::SQLITE_ROW {
        let msg = unsafe { CStr::from_ptr(ffi::sqlite3_errmsg(entry.db)).to_string_lossy().into_owned() };
        return Err(format!("step rc={step} msg={msg}"));
    }
    Ok(())
}

fn finalize_stmt(handle: u32) -> Result<(), String> {
    let mut guard = PREPARED.lock().map_err(|e| e.to_string())?;
    if let Some(map) = guard.as_mut() {
        map.remove(&handle);
    }
    Ok(())
}

fn libsql_version() -> String {
    unsafe {
        let p = ffi::sqlite3_libversion();
        if p.is_null() {
            return "unknown".to_string();
        }
        CStr::from_ptr(p).to_string_lossy().into_owned()
    }
}

fn str_params(v: &Value) -> Vec<String> {
    v.get("params")
        .and_then(|p| p.as_array())
        .map(|a| a.iter().map(|x| x.as_str().map(String::from).unwrap_or_else(|| x.to_string())).collect())
        .unwrap_or_default()
}

pub fn handle(verb: &str, body: &Value) -> u64 {
    let name = body.get("db").and_then(|v| v.as_str()).unwrap_or("default");
    match verb {
        "open" => {
            let path = body.get("path").and_then(|v| v.as_str()).unwrap_or(":memory:");
            ok_err(open(name, path))
        }
        "close" => ok_err(close(name)),
        "exec" => {
            let sql = body.get("sql").and_then(|v| v.as_str()).unwrap_or("");
            ok_err(exec(name, sql))
        }
        "query" => {
            let sql = body.get("sql").and_then(|v| v.as_str()).unwrap_or("");
            let params = str_params(body);
            let refs: Vec<&str> = params.iter().map(|s| s.as_str()).collect();
            match query_impl_named(name, sql, &refs) {
                Ok(rows) => return_json(json!({"ok": true, "rows": rows})),
                Err(e) => return_json(json!({"ok": false, "error": e})),
            }
        }
        "exec_params" => {
            let sql = body.get("sql").and_then(|v| v.as_str()).unwrap_or("");
            let params = str_params(body);
            let refs: Vec<&str> = params.iter().map(|s| s.as_str()).collect();
            ok_err(exec_params(name, sql, &refs))
        }
        "begin" => ok_err(exec(name, "BEGIN IMMEDIATE")),
        "commit" => ok_err(exec(name, "COMMIT")),
        "rollback" => ok_err(exec(name, "ROLLBACK")),
        "prepare" => {
            let sql = body.get("sql").and_then(|v| v.as_str()).unwrap_or("");
            match prepare_stmt(name, sql) {
                Ok(h) => return_json(json!({"ok": true, "handle": h})),
                Err(e) => return_json(json!({"ok": false, "error": e})),
            }
        }
        "execute_bound" => {
            let h = body.get("handle").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
            let params = str_params(body);
            let refs: Vec<&str> = params.iter().map(|s| s.as_str()).collect();
            ok_err(execute_bound(h, &refs))
        }
        "finalize" => {
            let h = body.get("handle").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
            ok_err(finalize_stmt(h))
        }
        "serialize" => match serialize(name) {
            Ok(bytes) => return_json(json!({"ok": true, "bytes_b64": base64_encode(&bytes)})),
            Err(e) => return_json(json!({"ok": false, "error": e})),
        },
        "deserialize" => {
            let b64 = body.get("bytes_b64").and_then(|v| v.as_str()).unwrap_or("");
            match base64_decode(b64) {
                Ok(bytes) => ok_err(deserialize(name, &bytes)),
                Err(e) => return_json(json!({"ok": false, "error": e})),
            }
        }
        "version" => return_json(json!({"ok": true, "version": libsql_version()})),
        _ => return_json(json!({"ok": false, "error": "unknown_verb", "verb": verb})),
    }
}

fn query_impl_named(name: &str, sql: &str, params: &[&str]) -> Result<Value, String> {
    with_db(name, |db| query_impl(db, sql, params))
}

// Minimal base64 (no external dependency) for serialize/deserialize's byte
// payload -- these two verbs are rarely-used snapshot/restore paths, not a
// hot loop, so a hand-rolled encoder is fine rather than pulling in the
// `base64` crate for two call sites.
const B64_CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn base64_encode(data: &[u8]) -> String {
    let mut out = String::with_capacity((data.len() + 2) / 3 * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0];
        let b1 = *chunk.get(1).unwrap_or(&0);
        let b2 = *chunk.get(2).unwrap_or(&0);
        out.push(B64_CHARS[(b0 >> 2) as usize] as char);
        out.push(B64_CHARS[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize] as char);
        out.push(if chunk.len() > 1 { B64_CHARS[(((b1 & 0x0f) << 2) | (b2 >> 6)) as usize] as char } else { '=' });
        out.push(if chunk.len() > 2 { B64_CHARS[(b2 & 0x3f) as usize] as char } else { '=' });
    }
    out
}

fn base64_decode(s: &str) -> Result<Vec<u8>, String> {
    let mut rev = [255u8; 256];
    for (i, &c) in B64_CHARS.iter().enumerate() {
        rev[c as usize] = i as u8;
    }
    let clean: Vec<u8> = s.bytes().filter(|&b| b != b'=' && !b.is_ascii_whitespace()).collect();
    let mut out = Vec::with_capacity(clean.len() * 3 / 4);
    for chunk in clean.chunks(4) {
        let vals: Vec<u8> = chunk.iter().map(|&b| rev[b as usize]).collect();
        if vals.iter().any(|&v| v == 255) {
            return Err("invalid base64 input".to_string());
        }
        let n = vals.len();
        let b0 = (vals[0] << 2) | (vals.get(1).copied().unwrap_or(0) >> 4);
        out.push(b0);
        if n > 2 {
            out.push((vals[1] << 4) | (vals[2] >> 2));
        }
        if n > 3 {
            out.push((vals[2] << 6) | vals[3]);
        }
    }
    Ok(out)
}
