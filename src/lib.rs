//! purple-matrix-rs: the Rust half of a libpurple protocol plugin for Matrix.
//!
//! This library owns the matrix-rust-sdk client; `c/prpl.c` owns libpurple. The two meet at the
//! `extern "C"` functions below, and nothing else crosses: strings go over as C strings that the
//! Rust side allocates and frees, and every call that touches the SDK runs on the client's own
//! Tokio runtime.
//!
//! What is here today is the skeleton that spec 001 builds on: the library reports itself and can
//! build an SDK client with its SQLite stores, without touching the network.

use std::ffi::{CStr, CString};
use std::os::raw::c_char;

use matrix_sdk::Client;

/// The matrix-sdk line this library is written against; the exact version is in `Cargo.lock`.
const SDK_VERSION: &str = "0.19";

/// A string for the C side; `purple_matrix_string_free` gives it back.
fn into_c(s: String) -> *mut c_char {
    CString::new(s).unwrap_or_default().into_raw()
}

/// # Safety
/// `s` must be a NUL-terminated string that outlives the call, or null.
unsafe fn from_c(s: *const c_char) -> String {
    if s.is_null() {
        return String::new();
    }
    unsafe { CStr::from_ptr(s) }.to_string_lossy().into_owned()
}

/// What this library is: name, version and the SDK it was built with.
#[unsafe(no_mangle)]
pub extern "C" fn purple_matrix_version() -> *mut c_char {
    into_c(format!(
        "purple-matrix-rs {} on matrix-sdk {SDK_VERSION}",
        env!("CARGO_PKG_VERSION")
    ))
}

/// Build an SDK client for `homeserver` with its SQLite stores under `store_dir`, without
/// touching the network, and say what happened. The client is built and dropped on a runtime of
/// its own: its SQLite pool needs a runtime context to go away, which a client that outlives a
/// `block_on` no longer has.
///
/// # Safety
/// Both arguments must be NUL-terminated strings that outlive the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn purple_matrix_client_probe(
    homeserver: *const c_char,
    store_dir: *const c_char,
) -> *mut c_char {
    let (homeserver, store_dir) = unsafe { (from_c(homeserver), from_c(store_dir)) };
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(r) => r,
        Err(e) => return into_c(format!("no runtime: {e}")),
    };
    let answer = runtime.block_on(async {
        match Client::builder()
            .homeserver_url(&homeserver)
            .sqlite_store(&store_dir, None)
            .build()
            .await
        {
            Ok(client) => {
                let line = format!(
                    "client built for {} with an SQLite store in {store_dir}",
                    client.homeserver()
                );
                drop(client);
                line
            }
            Err(e) => format!("client build failed: {e}"),
        }
    });
    into_c(answer)
}

/// Give back a string this library handed out.
///
/// # Safety
/// `s` must come from this library and not be used afterwards; null is fine.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn purple_matrix_string_free(s: *mut c_char) {
    if !s.is_null() {
        drop(unsafe { CString::from_raw(s) });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn take(s: *mut c_char) -> String {
        let out = unsafe { CStr::from_ptr(s) }.to_string_lossy().into_owned();
        unsafe { purple_matrix_string_free(s) };
        out
    }

    #[test]
    fn version_names_the_library_and_the_sdk() {
        let v = take(purple_matrix_version());
        assert!(v.starts_with("purple-matrix-rs "));
        assert!(v.contains("matrix-sdk"));
    }

    #[test]
    fn a_client_builds_with_its_stores_and_without_network() {
        let dir = std::env::temp_dir().join(format!("purple-matrix-rs-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let homeserver = CString::new("https://matrix.example.org").expect("cstring");
        let store = CString::new(dir.to_string_lossy().as_bytes()).expect("cstring");
        let answer =
            take(unsafe { purple_matrix_client_probe(homeserver.as_ptr(), store.as_ptr()) });
        assert!(
            answer.starts_with("client built for https://matrix.example.org/"),
            "{answer}"
        );
        assert!(dir.join("matrix-sdk-crypto.sqlite3").exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn freeing_null_is_fine() {
        unsafe { purple_matrix_string_free(std::ptr::null_mut()) };
    }
}
