use std::alloc::{alloc, dealloc, Layout};
use std::mem;

#[no_mangle]
pub extern "C" fn plugkit_alloc(len: u32) -> u32 {
    if len == 0 {
        return 0;
    }
    let layout = Layout::from_size_align(len as usize, mem::align_of::<u8>()).unwrap();
    unsafe { alloc(layout) as u32 }
}

#[no_mangle]
pub extern "C" fn plugkit_free(ptr: u32, len: u32) {
    if ptr == 0 || len == 0 {
        return;
    }
    let layout = Layout::from_size_align(len as usize, mem::align_of::<u8>()).unwrap();
    unsafe { dealloc(ptr as *mut u8, layout) };
}

pub fn read_str(ptr: u32, len: u32) -> String {
    if len == 0 {
        return String::new();
    }
    unsafe {
        let slice = std::slice::from_raw_parts(ptr as *const u8, len as usize);
        String::from_utf8_lossy(slice).into_owned()
    }
}

pub fn return_bytes(bytes: Vec<u8>) -> u64 {
    if bytes.is_empty() {
        return 0;
    }
    let len = bytes.len();
    let ptr = plugkit_alloc(len as u32);
    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr as *mut u8, len);
    }
    (ptr as u64 & 0xffff_ffff) | ((len as u64) << 32)
}

pub fn return_json(v: serde_json::Value) -> u64 {
    return_bytes(v.to_string().into_bytes())
}

pub fn ok_err(r: Result<(), String>) -> u64 {
    match r {
        Ok(()) => return_json(serde_json::json!({"ok": true})),
        Err(e) => return_json(serde_json::json!({"ok": false, "error": e})),
    }
}

#[no_mangle]
pub extern "C" fn plugin_call(verb_ptr: u32, verb_len: u32, body_ptr: u32, body_len: u32) -> u64 {
    let verb = read_str(verb_ptr, verb_len);
    let body_str = read_str(body_ptr, body_len);
    let body: serde_json::Value = serde_json::from_str(&body_str).unwrap_or(serde_json::json!({}));
    let packed = crate::db::handle(&verb, &body);
    attach_diagnostics(packed)
}

// Merges any diagnostics db.rs collected during this call (e.g. the WAL-
// unavailable notice, which cannot use eprintln! -- see PENDING_DIAGNOSTICS'
// own doc comment for why) into the response's own JSON body as a
// "diagnostics" array, so the JS host can dedupe/print them itself instead
// of writing raw wasm-side stdio the host cannot intercept. A no-op
// (returns `packed` unchanged) whenever nothing was pushed this call, and
// best-effort: a response this function cannot parse as JSON (should not
// happen -- every db.rs verb arm returns return_json()'s own output) is
// passed through unchanged rather than dropped.
fn attach_diagnostics(packed: u64) -> u64 {
    // Drained UNCONDITIONALLY, before any other check -- PENDING_DIAGNOSTICS
    // is a thread_local Vec that outlives a single plugin_call, so leaving it
    // undrained on any path would not discard a diagnostic, it would silently
    // carry it forward and attach it to a LATER, unrelated call's response.
    let diagnostics = crate::db::take_pending_diagnostics();
    if diagnostics.is_empty() {
        return packed;
    }
    let diagnostics_value = serde_json::Value::Array(diagnostics.into_iter().map(serde_json::Value::String).collect());
    // packed == 0 means db::handle() returned an EMPTY response (return_bytes'
    // own contract for an empty Vec) -- no existing verb arm does this today,
    // but nothing guarantees a future one won't. Falling through to `return
    // packed` here (as an earlier version did) would drop already-drained
    // diagnostics on the floor instead of just skipping the merge -- they are
    // real, correctly-triggered warnings, not a caller-visible response body.
    // Synthesize a minimal JSON object to carry them instead of discarding.
    if packed == 0 {
        return return_json(serde_json::json!({ "diagnostics": diagnostics_value }));
    }
    let ptr = (packed & 0xffff_ffff) as u32;
    let len = (packed >> 32) as u32;
    let text = read_str(ptr, len);
    plugkit_free(ptr, len);
    let mut value: serde_json::Value = match serde_json::from_str(&text) {
        Ok(v) => v,
        Err(_) => return return_bytes(text.into_bytes()),
    };
    if let Some(obj) = value.as_object_mut() {
        obj.insert("diagnostics".to_string(), diagnostics_value);
    }
    return_json(value)
}
