//! purple-matrix-rs: the Rust half of a libpurple protocol plugin for Matrix.
//!
//! This library owns the matrix-rust-sdk client; `c/prpl.c` owns libpurple. The two meet at the
//! `extern "C"` functions below, and nothing else crosses: strings go over as C strings that the
//! Rust side allocates and frees, an account is an opaque handle, and every call that touches the
//! SDK runs on the account's own runtime thread ([`runtime`]).
//!
//! The C side calls in — `purple_matrix_account_new`, `_login`, `_close` — and is woken by the
//! callback it registered when events wait; it collects them on its main thread with
//! `purple_matrix_account_next_event`. No libpurple call is ever made from the runtime thread.

mod login;
mod runtime;

use std::ffi::{CStr, CString, c_void};
use std::os::raw::{c_char, c_int};

pub use login::{ErrorReason, LoginError};
pub use runtime::{Account, Event};

/// The matrix-sdk line this library is written against; the exact version is in `Cargo.lock`.
const SDK_VERSION: &str = "0.19";

/// `purple_matrix_account_next_event`'s answer when nothing waits.
pub const EVENT_NONE: c_int = 0;
/// `purple_matrix_account_next_event`'s answer for [`Event::Connected`]: the text is the user id.
pub const EVENT_CONNECTED: c_int = 1;
/// `purple_matrix_account_next_event`'s answer for [`Event::Error`]: the reason is set, the text
/// is the message.
pub const EVENT_ERROR: c_int = 2;

/// The wake callback the C side registers: called on the runtime thread with its context.
pub type WakeFn = unsafe extern "C" fn(ctx: *mut c_void);

/// The callback's context, handed back as it came. Raw pointers are not `Send`; the C side owns
/// what it points at and is the only one to read it, so carrying it across threads is sound.
struct WakeContext(*mut c_void);
unsafe impl Send for WakeContext {}
unsafe impl Sync for WakeContext {}

impl WakeContext {
    /// Through a method, so a closure captures the wrapper and not the bare pointer in it.
    fn ptr(&self) -> *mut c_void {
        self.0
    }
}

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

/// Start an account's runtime thread. `wake` is called with `ctx` on that thread whenever events
/// wait; it must schedule `purple_matrix_account_next_event` on the main thread and nothing more.
/// Null when the thread could not be started.
///
/// # Safety
/// `wake` must stay callable, and `ctx` valid, until `purple_matrix_account_close`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn purple_matrix_account_new(
    wake: Option<WakeFn>,
    ctx: *mut c_void,
) -> *mut Account {
    let ctx = WakeContext(ctx);
    let account = Account::new(move || {
        if let Some(wake) = wake {
            unsafe { wake(ctx.ptr()) }
        }
    });
    match account {
        Ok(account) => Box::into_raw(Box::new(account)),
        Err(_) => std::ptr::null_mut(),
    }
}

/// Sign in with `token` against `homeserver`, the SDK's stores under `store_dir`; the outcome
/// arrives as an event.
///
/// # Safety
/// `account` must come from `purple_matrix_account_new` and not be closed; the strings must be
/// NUL-terminated and outlive the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn purple_matrix_account_login(
    account: *mut Account,
    homeserver: *const c_char,
    store_dir: *const c_char,
    token: *const c_char,
) {
    if account.is_null() {
        return;
    }
    let (homeserver, store_dir, token) =
        unsafe { (from_c(homeserver), from_c(store_dir), from_c(token)) };
    unsafe { &*account }.login(&homeserver, &store_dir, &token);
}

/// The next event for libpurple, on the main thread: `EVENT_NONE` when the batch is collected,
/// `EVENT_CONNECTED` with the user id in `text`, or `EVENT_ERROR` with the `PurpleConnectionError`
/// value in `reason` and the message in `text`. `text` goes back with `purple_matrix_string_free`.
///
/// # Safety
/// `account` must come from `purple_matrix_account_new` and not be closed; `reason` and `text`
/// must be writable or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn purple_matrix_account_next_event(
    account: *mut Account,
    reason: *mut c_int,
    text: *mut *mut c_char,
) -> c_int {
    if account.is_null() {
        return EVENT_NONE;
    }
    let (kind, event_reason, event_text) = match unsafe { &*account }.next_event() {
        None => return EVENT_NONE,
        Some(Event::Connected { user_id }) => (EVENT_CONNECTED, 0, user_id),
        Some(Event::Error { reason, message }) => (EVENT_ERROR, reason as c_int, message),
    };
    if !reason.is_null() {
        unsafe { *reason = event_reason };
    }
    if !text.is_null() {
        unsafe { *text = into_c(event_text) };
    }
    kind
}

/// Shut the account's runtime thread down, wait for it, and free the handle: the client is dropped
/// on its runtime. No wake follows this call. Null is fine.
///
/// # Safety
/// `account` must come from `purple_matrix_account_new` and not be used afterwards.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn purple_matrix_account_close(account: *mut Account) {
    if !account.is_null() {
        unsafe { Box::from_raw(account) }.close();
    }
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
    use std::sync::mpsc;

    use super::*;

    fn take(s: *mut c_char) -> String {
        let out = unsafe { CStr::from_ptr(s) }.to_string_lossy().into_owned();
        unsafe { purple_matrix_string_free(s) };
        out
    }

    #[test]
    fn version_names_the_library_and_the_sdk() {
        let v = take(purple_matrix_version());
        assert_eq!(
            v,
            format!(
                "purple-matrix-rs {} on matrix-sdk 0.19",
                env!("CARGO_PKG_VERSION")
            )
        );
    }

    #[test]
    fn freeing_null_is_fine() {
        unsafe { purple_matrix_string_free(std::ptr::null_mut()) };
    }

    #[test]
    fn closing_null_is_fine() {
        unsafe { purple_matrix_account_close(std::ptr::null_mut()) };
    }

    /// The C wake callback a client would register: here it signals a channel whose sender is
    /// the context.
    unsafe extern "C" fn wake(ctx: *mut c_void) {
        let sender = unsafe { &*(ctx as *const mpsc::Sender<()>) };
        let _ = sender.send(());
    }

    #[test]
    fn the_c_surface_wakes_and_hands_an_error_out_as_reason_and_text() {
        let (tx, rx) = mpsc::channel::<()>();
        let ctx = Box::into_raw(Box::new(tx));
        let account = unsafe { purple_matrix_account_new(Some(wake), ctx as *mut c_void) };
        assert!(!account.is_null());

        let dir = std::env::temp_dir().join(format!("purple-matrix-rs-ffi-{}", std::process::id()));
        // Port 1 on the loopback: nothing listens there, so the connection is refused.
        let homeserver = CString::new("http://127.0.0.1:1").expect("cstring");
        let store = CString::new(dir.to_string_lossy().as_bytes()).expect("cstring");
        let token = CString::new("syt_ffi_probe_token").expect("cstring");
        unsafe {
            purple_matrix_account_login(
                account,
                homeserver.as_ptr(),
                store.as_ptr(),
                token.as_ptr(),
            )
        };
        rx.recv_timeout(std::time::Duration::from_secs(60))
            .expect("the wake callback fires once the event waits");

        let mut reason: c_int = -1;
        let mut text: *mut c_char = std::ptr::null_mut();
        let kind = unsafe { purple_matrix_account_next_event(account, &mut reason, &mut text) };
        assert_eq!(kind, EVENT_ERROR);
        assert_eq!(reason, ErrorReason::Network as c_int);
        // The rest of the message is the OS's text for the refused connection, which varies.
        let message = take(text);
        assert!(
            message.starts_with("the homeserver was not reached: "),
            "{message}"
        );
        let kind = unsafe { purple_matrix_account_next_event(account, &mut reason, &mut text) };
        assert_eq!(kind, EVENT_NONE);

        unsafe { purple_matrix_account_close(account) };
        drop(unsafe { Box::from_raw(ctx) });
        let _ = std::fs::remove_dir_all(&dir);
    }
}
