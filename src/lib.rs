//! purple-matrix-rs: the Rust half of a libpurple protocol plugin for Matrix.
//!
//! This library owns the matrix-rust-sdk client; `c/prpl.c` owns libpurple. The two meet at the
//! `extern "C"` functions below, and nothing else crosses: strings go over as C strings that the
//! Rust side allocates and frees, an account is an opaque handle, and every call that touches the
//! SDK runs on the account's own runtime thread ([`runtime`]).
//!
//! The C side calls in — `purple_matrix_account_new`, `_login`, `_join`, `_reject`, `_close` —
//! and is woken by the callback it registered when events wait; it collects them on its main
//! thread with `purple_matrix_account_next_event`, one [`CEvent`] at a time. No libpurple call is
//! ever made from the runtime thread.

mod login;
mod rooms;
mod runtime;

use std::ffi::{CStr, CString, c_void};
use std::os::raw::{c_char, c_int};

pub use login::{ErrorReason, LoginError};
pub use runtime::{Account, Event, Member};

/// The matrix-sdk line this library is written against; the exact version is in `Cargo.lock`.
const SDK_VERSION: &str = "0.19";

/// The kinds of [`CEvent`], as `c/prpl.c` reads them; which fields a kind fills is documented at
/// [`CEvent`].
pub const EVENT_CONNECTED: c_int = 1;
pub const EVENT_ERROR: c_int = 2;
pub const EVENT_ROOM_JOINED: c_int = 3;
pub const EVENT_ROOM_LEFT: c_int = 4;
pub const EVENT_MEMBER_JOINED: c_int = 5;
pub const EVENT_MEMBER_RENAMED: c_int = 6;
pub const EVENT_MEMBER_LEFT: c_int = 7;
pub const EVENT_ROOM_NAMED: c_int = 8;
pub const EVENT_ROOM_TOPIC: c_int = 9;
pub const EVENT_MESSAGE: c_int = 10;
pub const EVENT_NOTICE: c_int = 11;
pub const EVENT_INVITED: c_int = 12;
pub const EVENT_JOIN_FAILED: c_int = 13;
pub const EVENT_LEAVE_FAILED: c_int = 14;

/// An [`Event`] for the C side: one struct for every kind, so a new kind is a new `kind` value
/// and the fields it fills, not a new function. Strings are null where the kind has no value for
/// them; `purple_matrix_event_free` gives the whole struct back.
///
/// | kind | fields |
/// |---|---|
/// | `CONNECTED` | `text` the user id |
/// | `ERROR` | `reason` the `PurpleConnectionError`, `text` the message |
/// | `ROOM_JOINED` | `room_id`, `text` the name, `topic` (null for none), `members` |
/// | `ROOM_LEFT` | `room_id` |
/// | `MEMBER_JOINED` | `room_id`, `user_id`, `name` |
/// | `MEMBER_RENAMED` | `room_id`, `old_name`, `name` |
/// | `MEMBER_LEFT` | `room_id`, `name` |
/// | `ROOM_NAMED` | `room_id`, `text` the name |
/// | `ROOM_TOPIC` | `room_id`, `name` who set it, `topic` |
/// | `MESSAGE` | `room_id`, `event_id`, `name` the sender, `text` the body, `timestamp` |
/// | `NOTICE` | `room_id`, `text`, `timestamp` |
/// | `INVITED` | `room_id`, `text` the room's name, `name` the inviter |
/// | `JOIN_FAILED`, `LEAVE_FAILED` | `room_id`, `text` the message |
#[repr(C)]
pub struct CEvent {
    pub kind: c_int,
    pub reason: c_int,
    /// Seconds since the epoch, the event's own.
    pub timestamp: i64,
    pub room_id: *mut c_char,
    pub event_id: *mut c_char,
    pub user_id: *mut c_char,
    pub name: *mut c_char,
    pub old_name: *mut c_char,
    pub text: *mut c_char,
    pub topic: *mut c_char,
    pub member_count: c_int,
    /// `member_count` user ids, and the names they go by, in the same order.
    pub member_ids: *mut *mut c_char,
    pub member_names: *mut *mut c_char,
}

impl CEvent {
    /// Every field null or zero: the base of the struct updates in `From<Event>`, which is left
    /// to drop with nothing to free.
    fn empty(kind: c_int) -> Self {
        Self {
            kind,
            reason: 0,
            timestamp: 0,
            room_id: std::ptr::null_mut(),
            event_id: std::ptr::null_mut(),
            user_id: std::ptr::null_mut(),
            name: std::ptr::null_mut(),
            old_name: std::ptr::null_mut(),
            text: std::ptr::null_mut(),
            topic: std::ptr::null_mut(),
            member_count: 0,
            member_ids: std::ptr::null_mut(),
            member_names: std::ptr::null_mut(),
        }
    }
}

impl From<Event> for CEvent {
    fn from(event: Event) -> Self {
        match event {
            Event::Connected { user_id } => Self {
                text: into_c(user_id),
                ..Self::empty(EVENT_CONNECTED)
            },
            Event::Error { reason, message } => Self {
                reason: reason as c_int,
                text: into_c(message),
                ..Self::empty(EVENT_ERROR)
            },
            Event::RoomJoined {
                room_id,
                name,
                topic,
                members,
            } => {
                let (ids, names): (Vec<_>, Vec<_>) = members
                    .into_iter()
                    .map(|m| (into_c(m.user_id), into_c(m.name)))
                    .unzip();
                Self {
                    room_id: into_c(room_id),
                    text: into_c(name),
                    topic: topic.map(into_c).unwrap_or(std::ptr::null_mut()),
                    member_count: c_int::try_from(ids.len()).unwrap_or(c_int::MAX),
                    member_ids: Box::into_raw(ids.into_boxed_slice()).cast(),
                    member_names: Box::into_raw(names.into_boxed_slice()).cast(),
                    ..Self::empty(EVENT_ROOM_JOINED)
                }
            }
            Event::RoomLeft { room_id } => Self {
                room_id: into_c(room_id),
                ..Self::empty(EVENT_ROOM_LEFT)
            },
            Event::MemberJoined { room_id, member } => Self {
                room_id: into_c(room_id),
                user_id: into_c(member.user_id),
                name: into_c(member.name),
                ..Self::empty(EVENT_MEMBER_JOINED)
            },
            Event::MemberRenamed {
                room_id,
                old_name,
                name,
            } => Self {
                room_id: into_c(room_id),
                old_name: into_c(old_name),
                name: into_c(name),
                ..Self::empty(EVENT_MEMBER_RENAMED)
            },
            Event::MemberLeft { room_id, name } => Self {
                room_id: into_c(room_id),
                name: into_c(name),
                ..Self::empty(EVENT_MEMBER_LEFT)
            },
            Event::RoomNamed { room_id, name } => Self {
                room_id: into_c(room_id),
                text: into_c(name),
                ..Self::empty(EVENT_ROOM_NAMED)
            },
            Event::RoomTopic {
                room_id,
                who,
                topic,
            } => Self {
                room_id: into_c(room_id),
                name: into_c(who),
                topic: into_c(topic),
                ..Self::empty(EVENT_ROOM_TOPIC)
            },
            Event::Message {
                room_id,
                event_id,
                sender,
                body,
                timestamp,
            } => Self {
                room_id: into_c(room_id),
                event_id: into_c(event_id),
                name: into_c(sender),
                text: into_c(body),
                timestamp,
                ..Self::empty(EVENT_MESSAGE)
            },
            Event::Notice {
                room_id,
                body,
                timestamp,
            } => Self {
                room_id: into_c(room_id),
                text: into_c(body),
                timestamp,
                ..Self::empty(EVENT_NOTICE)
            },
            Event::Invited {
                room_id,
                name,
                inviter,
            } => Self {
                room_id: into_c(room_id),
                text: into_c(name),
                name: into_c(inviter),
                ..Self::empty(EVENT_INVITED)
            },
            Event::JoinFailed { room_id, message } => Self {
                room_id: into_c(room_id),
                text: into_c(message),
                ..Self::empty(EVENT_JOIN_FAILED)
            },
            Event::LeaveFailed { room_id, message } => Self {
                room_id: into_c(room_id),
                text: into_c(message),
                ..Self::empty(EVENT_LEAVE_FAILED)
            },
        }
    }
}

impl Drop for CEvent {
    fn drop(&mut self) {
        for s in [
            self.room_id,
            self.event_id,
            self.user_id,
            self.name,
            self.old_name,
            self.text,
            self.topic,
        ] {
            free_c(s);
        }
        let count = usize::try_from(self.member_count).unwrap_or_default();
        for list in [self.member_ids, self.member_names] {
            if list.is_null() {
                continue;
            }
            // SAFETY: the list was made by `From<Event>` above from a boxed slice of `count`
            // strings this library allocated, and is freed once, here.
            let strings = unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(list, count)) };
            for s in strings {
                free_c(s);
            }
        }
    }
}

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

/// Give back a string from `into_c`; null is fine.
fn free_c(s: *mut c_char) {
    if !s.is_null() {
        // SAFETY: every string the C side holds came from `into_c`, and is given back once.
        drop(unsafe { CString::from_raw(s) });
    }
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

/// Join the room with `room_id`, or accept the invitation to it; the outcome is an event.
///
/// # Safety
/// `account` must come from `purple_matrix_account_new` and not be closed; `room_id` must be
/// NUL-terminated and outlive the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn purple_matrix_account_join(account: *mut Account, room_id: *const c_char) {
    if account.is_null() {
        return;
    }
    let room_id = unsafe { from_c(room_id) };
    unsafe { &*account }.join(&room_id);
}

/// Decline the invitation to the room with `room_id`.
///
/// # Safety
/// `account` must come from `purple_matrix_account_new` and not be closed; `room_id` must be
/// NUL-terminated and outlive the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn purple_matrix_account_reject(
    account: *mut Account,
    room_id: *const c_char,
) {
    if account.is_null() {
        return;
    }
    let room_id = unsafe { from_c(room_id) };
    unsafe { &*account }.reject(&room_id);
}

/// The next event for libpurple, on the main thread: a [`CEvent`] to give back with
/// `purple_matrix_event_free`, or null when the batch is collected.
///
/// # Safety
/// `account` must come from `purple_matrix_account_new` and not be closed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn purple_matrix_account_next_event(account: *mut Account) -> *mut CEvent {
    if account.is_null() {
        return std::ptr::null_mut();
    }
    match unsafe { &*account }.next_event() {
        None => std::ptr::null_mut(),
        Some(event) => Box::into_raw(Box::new(CEvent::from(event))),
    }
}

/// Give back an event from `purple_matrix_account_next_event`, strings and all; null is fine.
///
/// # Safety
/// `event` must come from `purple_matrix_account_next_event` and not be used afterwards.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn purple_matrix_event_free(event: *mut CEvent) {
    if !event.is_null() {
        drop(unsafe { Box::from_raw(event) });
    }
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
    free_c(s);
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
        unsafe { purple_matrix_event_free(std::ptr::null_mut()) };
        assert!(unsafe { purple_matrix_account_next_event(std::ptr::null_mut()) }.is_null());
    }

    /// Read a C string field without taking it.
    fn c_str(s: *mut c_char) -> Option<String> {
        (!s.is_null()).then(|| unsafe { CStr::from_ptr(s) }.to_string_lossy().into_owned())
    }

    #[test]
    fn a_joined_room_crosses_with_its_members_in_order_and_an_absent_topic_as_null() {
        let event = CEvent::from(Event::RoomJoined {
            room_id: "!r:localhost".to_owned(),
            name: "General".to_owned(),
            topic: None,
            members: vec![
                Member {
                    user_id: "@alice:localhost".to_owned(),
                    name: "Alice".to_owned(),
                },
                Member {
                    user_id: "@bob:localhost".to_owned(),
                    name: "Bob".to_owned(),
                },
            ],
        });
        assert_eq!(event.kind, EVENT_ROOM_JOINED);
        assert_eq!(c_str(event.room_id), Some("!r:localhost".to_owned()));
        assert_eq!(c_str(event.text), Some("General".to_owned()));
        assert_eq!(c_str(event.topic), None);
        assert_eq!(event.member_count, 2);
        let members: Vec<(Option<String>, Option<String>)> = (0..2)
            .map(|i| unsafe {
                (
                    c_str(*event.member_ids.add(i)),
                    c_str(*event.member_names.add(i)),
                )
            })
            .collect();
        assert_eq!(
            members,
            vec![
                (
                    Some("@alice:localhost".to_owned()),
                    Some("Alice".to_owned())
                ),
                (Some("@bob:localhost".to_owned()), Some("Bob".to_owned())),
            ]
        );
        drop(event);
    }

    #[test]
    fn a_message_crosses_with_its_timestamp_sender_body_and_event_id() {
        let event = CEvent::from(Event::Message {
            room_id: "!r:localhost".to_owned(),
            event_id: "$m1:localhost".to_owned(),
            sender: "Alice".to_owned(),
            body: "hello".to_owned(),
            timestamp: 1_700_000_000,
        });
        assert_eq!(
            (
                event.kind,
                event.timestamp,
                c_str(event.room_id),
                c_str(event.event_id),
                c_str(event.name),
                c_str(event.text),
                event.member_count,
            ),
            (
                EVENT_MESSAGE,
                1_700_000_000,
                Some("!r:localhost".to_owned()),
                Some("$m1:localhost".to_owned()),
                Some("Alice".to_owned()),
                Some("hello".to_owned()),
                0,
            )
        );
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

        let event = unsafe { purple_matrix_account_next_event(account) };
        assert!(!event.is_null());
        let (kind, reason, text) = unsafe { ((*event).kind, (*event).reason, (*event).text) };
        assert_eq!(kind, EVENT_ERROR);
        assert_eq!(reason, ErrorReason::Network as c_int);
        // The rest of the message is the OS's text for the refused connection, which varies.
        let message = unsafe { CStr::from_ptr(text) }
            .to_string_lossy()
            .into_owned();
        assert!(
            message.starts_with("the homeserver was not reached: "),
            "{message}"
        );
        unsafe { purple_matrix_event_free(event) };
        assert!(unsafe { purple_matrix_account_next_event(account) }.is_null());

        unsafe { purple_matrix_account_close(account) };
        drop(unsafe { Box::from_raw(ctx) });
        let _ = std::fs::remove_dir_all(&dir);
    }
}
