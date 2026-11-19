//! The JNI bridge for `com.minymaton.MinymatonModule`. Its companion object
//! declares `@JvmStatic external` functions, so each is a static native method
//! of `MinymatonModule`. The handle is a leaked `Box<MobileCore>`, freed by
//! `nativeClose`. Requests block, so the module calls them from an executor.

use std::path::PathBuf;

use jni::JNIEnv;
use jni::objects::{GlobalRef, JClass, JObject, JString, JValue};
use jni::sys::{jlong, jstring};

use crate::MobileCore;

fn throw(env: &mut JNIEnv, message: &str) {
    let _ = env.throw_new("java/lang/IllegalStateException", message);
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_minymaton_MinymatonModule_nativeOpen(
    mut env: JNIEnv,
    _class: JClass,
    data_dir: JString,
    listener: JObject,
) -> jlong {
    let data_dir: String = match env.get_string(&data_dir) {
        Ok(value) => value.into(),
        Err(error) => {
            throw(&mut env, &format!("invalid data dir: {error}"));
            return 0;
        }
    };
    let (vm, listener): (jni::JavaVM, GlobalRef) =
        match (env.get_java_vm(), env.new_global_ref(listener)) {
            (Ok(vm), Ok(listener)) => (vm, listener),
            _ => {
                throw(&mut env, "cannot retain the event listener");
                return 0;
            }
        };
    let callback = move |event: String| {
        let Ok(mut env) = vm.attach_current_thread_as_daemon() else {
            return;
        };
        let Ok(event) = env.new_string(event) else {
            return;
        };
        let result = env.call_method(
            listener.as_obj(),
            "onEvent",
            "(Ljava/lang/String;)V",
            &[JValue::Object(&event)],
        );
        if result.is_err() {
            let _ = env.exception_clear();
        }
        let _ = env.delete_local_ref(event);
    };
    match MobileCore::open(&PathBuf::from(data_dir), Box::new(callback)) {
        Ok(core) => Box::into_raw(Box::new(core)) as jlong,
        Err(error) => {
            throw(&mut env, &error.to_string());
            0
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_minymaton_MinymatonModule_nativeRequest(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    command: JString,
) -> jstring {
    if handle == 0 {
        throw(&mut env, "the core is closed");
        return std::ptr::null_mut();
    }
    // SAFETY: `handle` came from `nativeOpen` and `nativeClose` has not run;
    // the module serializes close after every outstanding request.
    let core = unsafe { &*(handle as *const MobileCore) };
    let command: String = match env.get_string(&command) {
        Ok(value) => value.into(),
        Err(error) => {
            throw(&mut env, &format!("invalid command string: {error}"));
            return std::ptr::null_mut();
        }
    };
    let response = core.request(&command);
    match env.new_string(response) {
        Ok(response) => response.into_raw(),
        Err(error) => {
            throw(&mut env, &format!("cannot return the response: {error}"));
            std::ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_minymaton_MinymatonModule_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    handle: jlong,
) {
    if handle != 0 {
        // SAFETY: `handle` came from `nativeOpen` and is closed exactly once.
        let core = unsafe { Box::from_raw(handle as *mut MobileCore) };
        core.close();
        drop(core);
    }
}
