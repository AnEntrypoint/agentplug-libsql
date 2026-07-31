use libsql_ffi as ffi;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::ptr;
use std::sync::Mutex;

const BUSY_TIMEOUT_MS: i32 = 30_000;

use crate::abi::{ok_err, return_json};

/// Every verb below opens the db fresh, does its one operation, and closes
/// it before returning -- no connection, prepared statement, or transaction
/// state survives past a single dispatch call. This makes the plugin safe
/// to share as ONE process-wide instance across any number of concurrently
/// active projects/agents (no per-project connection map to collide on),
/// per the explicit design directive: "opening the db in the current
/// folder under .gm, processing the instruction, and then closing it,
/// making it stateless and safe to process any number of agents at the
/// same time, in any folder, at the same time." The real cost is fresh
/// sqlite3_open_v2/close on every call instead of an amortized-once
/// connection -- accepted deliberately in exchange for zero shared-state
/// correctness risk. This is the ONLY model for a real file `path` (the
/// file itself IS the durable identity across calls -- see MEMORY_REGISTRY's
/// doc comment for why `:memory:` needs a genuinely different mechanism).
struct RawDb(*mut ffi::sqlite3);
unsafe impl Send for RawDb {}

impl Drop for RawDb {
    fn drop(&mut self) {
        if !self.0.is_null() {
            // sqlite3_close (strict) returns SQLITE_BUSY and leaves the
            // connection ALIVE if any prepared statement/blob handle on it
            // hasn't been finalized -- and its return value was previously
            // discarded here, so a failed close silently orphaned a live
            // connection that still holds SQLite's own internal lock on the
            // db file for the rest of the process's life. Every later
            // open_db() on the same path then races that orphan and gets
            // `database is locked`, with no OS-level file lock and no
            // second process visible to explain it (the exact symptom
            // reproduced live: sqlite3 CLI opens the file fine from
            // outside, yet every in-process call after the first still
            // sees rc=5). sqlite3_close_v2 never fails this way -- it always
            // detaches the connection immediately and, if statements are
            // still pending, defers the actual deallocation until they
            // finish on their own, so Drop can never leave a phantom
            // lock-holder behind.
            unsafe {
                ffi::sqlite3_close_v2(self.0);
            }
        }
    }
}

/// EXCEPTION to RawDb's stateless-per-call contract, added after it was
/// found live-broken (2026-07-30, thebird project). `path == ":memory:"` is
/// the ONLY option available to a host with no real filesystem (a browser --
/// WASI preopens require an actual backing store, which a browser genuinely
/// does not have). Two approaches were tried and only one actually works:
///
/// 1. (TRIED, DOES NOT WORK) SQLite's own named shared-cache in-memory URI
///    (`file:<name>?mode=memory&cache=shared` + `sqlite3_enable_shared_cache`)
///    -- this ONLY keeps a shared in-memory db alive while at least one
///    connection to it is open SIMULTANEOUSLY; the moment the LAST connection
///    closes, SQLite destroys it, identically to bare `:memory:`. Verified
///    live via an isolated repro (open->exec->close, then a SEPARATE
///    open->query): the table was gone. Sequential open-operate-close, which
///    is this whole module's contract, means there is NEVER a second
///    simultaneous connection, so shared-cache mode buys nothing here.
///
/// 2. (THIS FIX) A real, explicit, process-wide connection registry, keyed
///    by `db_name`, that keeps the actual `sqlite3*` handle open across
///    calls for `:memory:`+named databases ONLY. This is a genuine, narrow
///    exception to the stateless-per-call design -- the file-path case
///    (native hosts, the original multi-agent/multi-project safety
///    argument) is completely unaffected, since it never touches this
///    registry. A `:memory:` connection is registered on first use under its
///    `db_name` and reused by every later call with that same name; nothing
///    ever evicts it automatically (a browser page's lifetime bounds it
///    naturally -- the whole wasm instance, and this static with it, is
///    torn down on reload/navigation, which is the correct "session ended"
///    signal). `db_name` collisions across genuinely unrelated logical
///    databases are the CALLER's responsibility to avoid (thebird's
///    sqlite-shim.js already derives distinct names per app-chosen
///    filename); this registry does not (and structurally cannot) enforce
///    isolation beyond honoring whatever name it's given.
static MEMORY_REGISTRY: Mutex<Option<HashMap<String, RawDb>>> = Mutex::new(None);

fn with_memory_db<T>(name: &str, f: impl FnOnce(*mut ffi::sqlite3) -> Result<T, String>) -> Result<T, String> {
    let mut guard = MEMORY_REGISTRY.lock().map_err(|e| format!("memory registry poisoned: {e}"))?;
    let map = guard.get_or_insert_with(HashMap::new);
    if !map.contains_key(name) {
        let db = open_fresh(":memory:", 0)?;
        map.insert(name.to_string(), db);
    }
    // Safe to unwrap: just inserted if absent.
    let handle = map.get(name).unwrap().0;
    f(handle)
}

/// Opens a genuinely fresh connection with no registry involvement -- the
/// original stateless behavior, used for every file path AND for an
/// anonymous (no `db_name`) `:memory:` request that deliberately wants a
/// scratch db with no cross-call visibility.
fn open_fresh(path: &str, extra_flags: i32) -> Result<RawDb, String> {
    let cpath = CString::new(path).map_err(|e| e.to_string())?;
    let mut db: *mut ffi::sqlite3 = ptr::null_mut();
    let rc = unsafe { ffi::sqlite3_open_v2(cpath.as_ptr(), &mut db, ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE | extra_flags, ptr::null()) };
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
        return Err(format!("sqlite3_open_v2 {path} {msg}"));
    }
    // Without a busy handler sqlite returns SQLITE_BUSY(5) the instant it finds
    // the db locked rather than waiting for the holder to finish. This plugin
    // is stateless by design -- every exec/query opens and closes the file
    // again -- so concurrent dispatches genuinely contend, and the symptom was
    // a live `exec rc=5 msg=database is locked` that silently degraded recall's
    // vector half to an empty kv_query fallback. Wait for the lock instead.
    //
    // 5000ms proved insufficient in practice: with 51 projects registered
    // against one shared daemon, a vector-store write on a 119MB gm.db holds
    // the lock past that window and recall still died with rc=5. WAL is the
    // real fix -- readers stop blocking on a writer entirely, which is the
    // whole shape of the contention here (many concurrent readers, occasional
    // writer) -- and the longer timeout covers the writer-vs-writer case WAL
    // does not.
    unsafe {
        ffi::sqlite3_busy_timeout(db, BUSY_TIMEOUT_MS);
        let wal = b"PRAGMA journal_mode=WAL;\0";
        ffi::sqlite3_exec(db, wal.as_ptr() as *const _, None, std::ptr::null_mut(), std::ptr::null_mut());
    }
    Ok(RawDb(db))
}

/// Dispatches to either the persistent MEMORY_REGISTRY (when `path` is
/// `:memory:` AND a `db_name` is given) or a genuinely fresh, scoped-to-this-
/// call connection (every other case) -- the single decision point every SQL
/// operation below routes through, so the registry-vs-fresh choice lives in
/// exactly one place.
fn with_db<T>(path: &str, db_name: Option<&str>, f: impl FnOnce(*mut ffi::sqlite3) -> Result<T, String>) -> Result<T, String> {
    match db_name {
        Some(name) if path == ":memory:" && !name.is_empty() => with_memory_db(name, f),
        _ => {
            let db = open_fresh(path, 0)?;
            f(db.0)
        }
    }
}

fn exec(path: &str, sql: &str, db_name: Option<&str>) -> Result<(), String> {
    with_db(path, db_name, |db| {
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

fn query(path: &str, sql: &str, params: &[&str], db_name: Option<&str>) -> Result<Value, String> {
    with_db(path, db_name, |db| query_impl(db, sql, params))
}

fn exec_params(path: &str, sql: &str, params: &[&str], db_name: Option<&str>) -> Result<(), String> {
    with_db(path, db_name, |db| {
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

fn serialize(path: &str, db_name: Option<&str>) -> Result<Vec<u8>, String> {
    with_db(path, db_name, |db| {
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

fn deserialize(path: &str, bytes: &[u8], db_name: Option<&str>) -> Result<(), String> {
    with_db(path, db_name, |db| {
        let schema = CString::new("main").unwrap();
        let size = bytes.len() as i64;
        let buf = unsafe { ffi::sqlite3_malloc64(size as u64) } as *mut u8;
        if buf.is_null() {
            return Err("malloc failed".to_string());
        }
        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), buf, bytes.len());
        }
        // NOT SQLITE_DESERIALIZE_FREEONCLOSE: for a registry-held :memory:
        // connection (see MEMORY_REGISTRY), "on close" may be arbitrarily far
        // in the future (the connection stays open across many later calls)
        // -- FREEONCLOSE would be correct memory-management but is confusing
        // to reason about for a long-lived handle, and SQLite takes ownership
        // of buf via sqlite3_deserialize either way (RESIZEABLE lets it
        // realloc/grow the buffer as more data is written later, which a
        // registry connection expects since MORE writes happen after this
        // deserialize call, unlike the old stateless one-shot-then-close use).
        // Freeing is instead handled by SQLite itself on the NEXT deserialize
        // (which replaces the schema) or whenever the connection eventually
        // does close (via sqlite3_close_v2's normal cleanup of the current
        // in-memory page image, which SQLite owns regardless of the
        // FREEONCLOSE flag once deserialize has attached it -- FREEONCLOSE
        // only controls whether SQLite frees the SPECIFIC buffer pointer we
        // passed in on close, vs. potentially having copied/replaced it via
        // RESIZEABLE growth in the meantime; either way this doesn't leak).
        let flags = ffi::SQLITE_DESERIALIZE_RESIZEABLE as u32;
        let rc = unsafe { ffi::sqlite3_deserialize(db, schema.as_ptr(), buf, size, size, flags) };
        if rc != ffi::SQLITE_OK {
            unsafe { ffi::sqlite3_free(buf as *mut _); }
            return Err(format!("deserialize rc={rc}"));
        }
        Ok(())
    })
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

/// Rewrites a host-style absolute path (Windows: `C:\dev\gm/.gm/gm.db`,
/// possibly mixed-separator) into the WASI-guest POSIX path this plugin's
/// own `/`-rooted preopen (set up host-side by
/// agentplug-host::HostState::new_with_fs_root, guest path "/") expects --
/// this wasm module is built with libsql-ffi's `wasm32-wasi-vfs` feature, so
/// sqlite3_open_v2 issues real wasi-libc path_open calls, which prefix-match
/// the request against registered preopen guest paths and have no concept
/// of a Windows drive letter at all. `:memory:` and already-POSIX paths
/// (starting with `/`) pass through unchanged.
fn to_wasi_guest_path(path: &str) -> String {
    if path.is_empty() || path == ":memory:" || path.starts_with('/') {
        return path.to_string();
    }
    let mut p = path.to_string();
    if p.len() > 1 && p.as_bytes()[1] == b':' {
        p = p[2..].to_string();
    }
    p = p.replace('\\', "/");
    if !p.starts_with('/') {
        p = format!("/{p}");
    }
    p
}

/// Every verb requires an explicit `path` (the real db file path, resolved
/// by the CALLER against its own project root) -- there is no persistent
/// `name` -> connection mapping anymore, so `name` alone is no longer a
/// meaningful identifier once one plugin instance serves every project...
/// EXCEPT for the `path == ":memory:"` case, where `db`/`db_name` becomes
/// load-bearing again as the shared-cache identity (see resolve_open_uri's
/// and RawDb's doc comments) -- a browser host has no real file path to be
/// the identity instead, so this is the only identity available to it.
pub fn handle(verb: &str, body: &Value) -> u64 {
    let raw_path = body.get("path").and_then(|v| v.as_str()).unwrap_or(":memory:");
    let owned_path = to_wasi_guest_path(raw_path);
    let path = owned_path.as_str();
    let db_name = body.get("db").or_else(|| body.get("db_name")).and_then(|v| v.as_str());
    match verb {
        // "open"/"close"/"begin"/"commit"/"rollback" are no-ops now -- every
        // exec/query call is already its own open-operate-close cycle, so
        // there is nothing left to open/close/transact ahead of time. Kept
        // as accepted-but-inert verbs rather than "unknown_verb" errors so
        // existing callers that still send them don't need updating first.
        "open" | "close" | "begin" | "commit" | "rollback" => ok_err(Ok(())),
        "list_dbs" => return_json(json!({"ok": true, "dbs": Vec::<String>::new()})),
        "exec" => {
            let sql = body.get("sql").and_then(|v| v.as_str()).unwrap_or("");
            ok_err(exec(path, sql, db_name))
        }
        "query" | "query_params" => {
            let sql = body.get("sql").and_then(|v| v.as_str()).unwrap_or("");
            let params = str_params(body);
            let refs: Vec<&str> = params.iter().map(|s| s.as_str()).collect();
            match query(path, sql, &refs, db_name) {
                Ok(rows) => return_json(json!({"ok": true, "rows": rows})),
                Err(e) => return_json(json!({"ok": false, "error": e})),
            }
        }
        "exec_params" => {
            let sql = body.get("sql").and_then(|v| v.as_str()).unwrap_or("");
            let params = str_params(body);
            let refs: Vec<&str> = params.iter().map(|s| s.as_str()).collect();
            ok_err(exec_params(path, sql, &refs, db_name))
        }
        // "prepare"/"execute_bound"/"finalize" (a cross-call prepared
        // statement handle) is inherently incompatible with per-call
        // statelessness -- collapsed into a single atomic verb that
        // prepares, binds, steps, and finalizes within one open-operate-close
        // cycle, same shape as exec_params. Callers doing a prepare-once/
        // execute-many bulk-insert loop now pay one open+prepare+bind+step
        // per row instead of amortizing prepare across the loop -- a real,
        // deliberate cost accepted in exchange for zero persistent state.
        "prepare_execute" | "execute_bound" => {
            let sql = body.get("sql").and_then(|v| v.as_str()).unwrap_or("");
            let params = str_params(body);
            let refs: Vec<&str> = params.iter().map(|s| s.as_str()).collect();
            ok_err(exec_params(path, sql, &refs, db_name))
        }
        "prepare" => return_json(json!({"ok": false, "error": "prepare/finalize handles removed -- use prepare_execute (or exec_params) which does prepare+bind+step+finalize atomically per call"})),
        "finalize" => ok_err(Ok(())),
        "serialize" => match serialize(path, db_name) {
            Ok(bytes) => {
                let b64 = base64_encode(&bytes);
                return_json(json!({"ok": true, "data": b64.clone(), "bytes_b64": b64}))
            }
            Err(e) => return_json(json!({"ok": false, "error": e})),
        },
        "deserialize" => {
            let b64 = body
                .get("data")
                .and_then(|v| v.as_str())
                .or_else(|| body.get("bytes_b64").and_then(|v| v.as_str()))
                .unwrap_or("");
            match base64_decode(b64) {
                Ok(bytes) => ok_err(deserialize(path, &bytes, db_name)),
                Err(e) => return_json(json!({"ok": false, "error": e})),
            }
        }
        "version" => return_json(json!({"ok": true, "version": libsql_version()})),
        _ => return_json(json!({"ok": false, "error": "unknown_verb", "verb": verb})),
    }
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
