//! The C bridge for the iOS `MinymatonModule`. The Swift side calls these
//! exports; the handle is a leaked `Box<MobileCore>`, freed by
//! `minymaton_close`. Requests block, so the module calls them from a
//! background queue. Events arrive through the callback as UTF-8 JSON.

use std::ffi::{CStr, CString, c_char, c_void};
use std::path::PathBuf;

use crate::MobileCore;

pub type MinymatonEventCallback = extern "C" fn(*mut c_void, *const c_char);

struct Callback {
    context: *mut c_void,
    callback: MinymatonEventCallback,
}

// SAFETY: the Swift module retains the context for the core's lifetime and
// serializes open/request/close on one queue; the core only invokes the
// callback from its own threads with the retained context.
unsafe impl Send for Callback {}
unsafe impl Sync for Callback {}

impl Callback {
    fn invoke(&self, event: String) {
        if let Ok(value) = CString::new(event) {
            (self.callback)(self.context, value.as_ptr());
        }
    }
}

fn to_c_string(value: String) -> *mut c_char {
    match CString::new(value) {
        Ok(value) => value.into_raw(),
        Err(_) => CString::new("{\"ok\":false,\"error\":{\"code\":\"protocol_error\",\"message\":\"response contains NUL\"}}")
            .expect("static JSON is NUL-free")
            .into_raw(),
    }
}

/// # Safety
/// `data_dir` must be a valid NUL-terminated UTF-8 string. `callback` and
/// `context` must outlive the returned handle (until `minymaton_close`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn minymaton_open(
    data_dir: *const c_char,
    callback: MinymatonEventCallback,
    context: *mut c_void,
) -> *mut c_void {
    if data_dir.is_null() {
        return std::ptr::null_mut();
    }
    let data_dir = match unsafe { CStr::from_ptr(data_dir) }.to_str() {
        Ok(value) => value.to_owned(),
        Err(_) => return std::ptr::null_mut(),
    };
    let callback = Callback { context, callback };
    let listener = move |event: String| callback.invoke(event);
    match MobileCore::open(&PathBuf::from(data_dir), Box::new(listener)) {
        Ok(core) => Box::into_raw(Box::new(core)) as *mut c_void,
        Err(_) => std::ptr::null_mut(),
    }
}

/// # Safety
/// `handle` must come from `minymaton_open` and not be closed. `command` must
/// be a valid NUL-terminated UTF-8 string. The result must be freed with
/// `minymaton_free_string`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn minymaton_request(
    handle: *mut c_void,
    command: *const c_char,
) -> *mut c_char {
    if handle.is_null() || command.is_null() {
        return to_c_string("{\"ok\":false,\"error\":{\"code\":\"protocol_error\",\"message\":\"the core is closed\"}}".to_owned());
    }
    // SAFETY: `handle` came from `minymaton_open` and `minymaton_close` has
    // not run; the module serializes close after every outstanding request.
    let core = unsafe { &*(handle as *const MobileCore) };
    let command = match unsafe { CStr::from_ptr(command) }.to_str() {
        Ok(value) => value,
        Err(_) => {
            return to_c_string("{\"ok\":false,\"error\":{\"code\":\"protocol_error\",\"message\":\"invalid command string\"}}".to_owned());
        }
    };
    to_c_string(core.request(command))
}

/// # Safety
/// `value` must come from `minymaton_request` and be freed exactly once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn minymaton_free_string(value: *mut c_char) {
    if !value.is_null() {
        // SAFETY: `value` came from `to_c_string` in this module.
        drop(unsafe { CString::from_raw(value) });
    }
}

/// # Safety
/// `handle` must come from `minymaton_open` and be closed exactly once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn minymaton_close(handle: *mut c_void) {
    if !handle.is_null() {
        // SAFETY: `handle` came from `minymaton_open` and is closed once.
        let core = unsafe { Box::from_raw(handle as *mut MobileCore) };
        core.close();
        drop(core);
    }
}
