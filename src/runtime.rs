//! One runtime thread per account: a std thread that owns a Tokio runtime and the SDK client.
//!
//! libpurple's calls become [`Command`]s on a channel into the thread. Whatever the thread has to
//! tell libpurple becomes an [`Event`] in the account's outbox, and a wake callback says there is
//! something to collect — the callback runs on the runtime thread, so all it may do is schedule
//! the collection on the main thread (`c/prpl.c` does so with `purple_timeout_add`); the events
//! themselves are popped there with [`Account::next_event`].
//!
//! The client is built and dropped on the runtime, inside its `block_on`: dropped outside, its
//! SQLite pool aborts in teardown (the spike of 2026-10-13, spec 001 decision 5).

use std::collections::VecDeque;
use std::pin::pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::JoinHandle;

use matrix_sdk::Client;
use tokio::sync::mpsc;

use crate::login::{ErrorReason, sign_in};
use crate::rooms;

/// What libpurple asks of the account.
pub(crate) enum Command {
    Login {
        homeserver: String,
        store_dir: String,
        token: String,
    },
    /// Join the room, or accept the invitation to it; the chat opens with the next sync.
    Join {
        room_id: String,
    },
    /// Decline the invitation to the room.
    Reject {
        room_id: String,
    },
    /// Send `body` as a text message into the room; the line appears when the sync delivers the
    /// event, never at send time.
    Send {
        room_id: String,
        body: String,
    },
    Shutdown,
}

/// A room member as the chat lists them: the user id, and the name the member goes by.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Member {
    pub user_id: String,
    pub name: String,
}

/// What the account tells libpurple. Timestamps are seconds since the epoch, the event's own.
#[derive(Debug, PartialEq, Eq)]
pub enum Event {
    /// The token is good; the account is signed in as `user_id`.
    Connected {
        user_id: String,
    },
    /// The connection failed, with libpurple's reason and the message to show.
    Error {
        reason: ErrorReason,
        message: String,
    },
    /// A room the account is in: the chat opens with these members, this name and topic.
    RoomJoined {
        room_id: String,
        name: String,
        topic: Option<String>,
        members: Vec<Member>,
    },
    /// The account is no longer in the room; the chat is over.
    RoomLeft {
        room_id: String,
    },
    MemberJoined {
        room_id: String,
        member: Member,
    },
    /// A member changed display name: `old_name` in the chat becomes `name`.
    MemberRenamed {
        room_id: String,
        old_name: String,
        name: String,
    },
    MemberLeft {
        room_id: String,
        name: String,
    },
    RoomNamed {
        room_id: String,
        name: String,
    },
    RoomTopic {
        room_id: String,
        who: String,
        topic: String,
    },
    /// A line in the chat, history or live; `event_id` tells the server's echo of an own message
    /// from a new one.
    Message {
        room_id: String,
        event_id: String,
        sender: String,
        body: String,
        timestamp: i64,
    },
    /// A line from the plugin itself in the chat, such as a message that could not be decrypted.
    Notice {
        room_id: String,
        body: String,
        timestamp: i64,
    },
    /// An invitation to the room, from `inviter`.
    Invited {
        room_id: String,
        name: String,
        inviter: String,
    },
    JoinFailed {
        room_id: String,
        message: String,
    },
    LeaveFailed {
        room_id: String,
        message: String,
    },
    /// A message could not be sent: `text` is what was typed, `message` the reason.
    SendFailed {
        room_id: String,
        text: String,
        message: String,
    },
}

/// The main-thread path out: events queue here, and the wake callback is called once per batch —
/// when an event lands in an empty queue — until [`Outbox::pop`] has emptied it again.
pub(crate) struct Outbox {
    queue: Mutex<Queue>,
    wake: Box<dyn Fn() + Send + Sync>,
}

struct Queue {
    events: VecDeque<Event>,
    wake_pending: bool,
}

impl Outbox {
    pub(crate) fn new(wake: Box<dyn Fn() + Send + Sync>) -> Self {
        Self {
            queue: Mutex::new(Queue {
                events: VecDeque::new(),
                wake_pending: false,
            }),
            wake,
        }
    }

    pub(crate) fn push(&self, event: Event) {
        let mut queue = self.queue.lock().unwrap_or_else(PoisonError::into_inner);
        queue.events.push_back(event);
        let wake = !queue.wake_pending;
        queue.wake_pending = true;
        drop(queue);
        if wake {
            (self.wake)();
        }
    }

    pub(crate) fn pop(&self) -> Option<Event> {
        let mut queue = self.queue.lock().unwrap_or_else(PoisonError::into_inner);
        let event = queue.events.pop_front();
        if event.is_none() {
            queue.wake_pending = false;
        }
        event
    }
}

/// An account's runtime thread, from its start to [`Account::close`].
pub struct Account {
    commands: mpsc::UnboundedSender<Command>,
    thread: Option<JoinHandle<()>>,
    outbox: Arc<Outbox>,
}

impl Account {
    /// Start the runtime thread. `wake` is called, on that thread, whenever the first event of a
    /// batch is ready; it must schedule [`Account::next_event`] on the main thread and nothing more.
    pub fn new(wake: impl Fn() + Send + Sync + 'static) -> std::io::Result<Self> {
        let (commands, inbox) = mpsc::unbounded_channel();
        let outbox = Arc::new(Outbox::new(Box::new(wake)));
        let thread_outbox = Arc::clone(&outbox);
        let thread = std::thread::Builder::new()
            .name("matrix-account".into())
            .spawn(move || serve(inbox, thread_outbox))?;
        Ok(Self {
            commands,
            thread: Some(thread),
            outbox,
        })
    }

    /// Sign in with `token` against `homeserver`, the stores under `store_dir`; the outcome is an
    /// [`Event::Connected`] or an [`Event::Error`].
    pub fn login(&self, homeserver: &str, store_dir: &str, token: &str) {
        // A send fails only once the thread is gone, and then there is nobody to tell.
        let _ = self.commands.send(Command::Login {
            homeserver: homeserver.to_owned(),
            store_dir: store_dir.to_owned(),
            token: token.to_owned(),
        });
    }

    /// Join the room, or accept the invitation to it; the chat opens as an [`Event::RoomJoined`]
    /// with the next sync, or an [`Event::JoinFailed`] says why not.
    pub fn join(&self, room_id: &str) {
        let _ = self.commands.send(Command::Join {
            room_id: room_id.to_owned(),
        });
    }

    /// Decline the invitation to the room; an [`Event::LeaveFailed`] says if that did not work.
    pub fn reject(&self, room_id: &str) {
        let _ = self.commands.send(Command::Reject {
            room_id: room_id.to_owned(),
        });
    }

    /// Send `body` as a text message into the room. Nothing comes back on success but the
    /// server's echo, an [`Event::Message`] with the next sync; an [`Event::SendFailed`] says why not.
    pub fn send(&self, room_id: &str, body: &str) {
        let _ = self.commands.send(Command::Send {
            room_id: room_id.to_owned(),
            body: body.to_owned(),
        });
    }

    /// The next event for libpurple, on the main thread; `None` once the batch is collected.
    pub fn next_event(&self) -> Option<Event> {
        self.outbox.pop()
    }

    /// Shut the runtime thread down and wait for it: the client is dropped on its runtime, a
    /// sign-in still under way is abandoned.
    pub fn close(mut self) {
        let _ = self.commands.send(Command::Shutdown);
        if let Some(thread) = self.thread.take() {
            // A thread that panicked has nothing left to join on; the account is gone either way.
            let _ = thread.join();
        }
    }
}

/// The runtime thread's body.
fn serve(inbox: mpsc::UnboundedReceiver<Command>, outbox: Arc<Outbox>) {
    match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime.block_on(run(inbox, outbox)),
        Err(e) => refuse(inbox, outbox, e),
    }
}

/// Without a runtime there is no client: every sign-in is answered with the reason.
fn refuse(mut inbox: mpsc::UnboundedReceiver<Command>, outbox: Arc<Outbox>, e: std::io::Error) {
    while let Some(command) = inbox.blocking_recv() {
        match command {
            Command::Login { .. } => outbox.push(Event::Error {
                reason: ErrorReason::Other,
                message: format!("no runtime for the account: {e}"),
            }),
            Command::Join { .. } | Command::Reject { .. } | Command::Send { .. } => {}
            Command::Shutdown => return,
        }
    }
}

/// The command loop; the client lives here, and is dropped here when the loop ends. A signed-in
/// client hands the loop to [`rooms::serve`], which syncs and carries out the commands until the
/// shutdown or until the sync fails for good.
async fn run(mut inbox: mpsc::UnboundedReceiver<Command>, outbox: Arc<Outbox>) {
    let mut client: Option<Client> = None;
    'commands: while let Some(command) = inbox.recv().await {
        match command {
            Command::Login {
                homeserver,
                store_dir,
                token,
            } => {
                // A shutdown during the sign-in drops the sign-in future, client and all, inside
                // the runtime — which is where the client has to go.
                let mut signing_in = pin!(sign_in(&homeserver, &store_dir, &token));
                loop {
                    tokio::select! {
                        outcome = &mut signing_in => {
                            match outcome {
                                Ok((signed_in, user_id)) => {
                                    client = Some(signed_in);
                                    outbox.push(Event::Connected { user_id });
                                }
                                Err(e) => outbox.push(Event::Error {
                                    reason: e.reason(),
                                    message: e.to_string(),
                                }),
                            }
                            break;
                        }
                        next = inbox.recv() => match next {
                            Some(Command::Shutdown) | None => break 'commands,
                            Some(Command::Login { .. }) => outbox.push(Event::Error {
                                reason: ErrorReason::Other,
                                message: "a sign-in is already under way".to_owned(),
                            }),
                            // Nothing to join before the account is signed in.
                            Some(Command::Join { .. } | Command::Reject { .. } | Command::Send { .. }) => {}
                        },
                    }
                }
                if let Some(signed_in) = &client
                    && let rooms::Stopped::Shutdown =
                        rooms::serve(signed_in, &mut inbox, &outbox).await
                {
                    break 'commands;
                }
            }
            // After a sync failed for good there is no room to join; libpurple's close follows.
            Command::Join { .. } | Command::Reject { .. } | Command::Send { .. } => {}
            Command::Shutdown => break,
        }
    }
    drop(client);
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::path::Path;
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    use super::*;

    const TOKEN: &str = "syt_purplematrixrs_probe_token_0123456789";

    /// A homeserver of one answer: a loopback socket that reads one request's headers, writes
    /// `status` with the JSON `body`, and closes. `whoami` is all a sign-in sends.
    fn homeserver_answering(status: &'static str, body: &'static str) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("a loopback port");
        let port = listener.local_addr().expect("the bound address").port();
        std::thread::spawn(move || {
            let Ok((mut stream, _)) = listener.accept() else {
                return;
            };
            let mut request = Vec::new();
            let mut byte = [0u8; 1];
            while !request.ends_with(b"\r\n\r\n") && stream.read(&mut byte).is_ok_and(|n| n == 1) {
                request.push(byte[0]);
            }
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes());
        });
        format!("http://127.0.0.1:{port}")
    }

    /// A homeserver that accepts the connection and never answers.
    fn homeserver_silent() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("a loopback port");
        let port = listener.local_addr().expect("the bound address").port();
        std::thread::spawn(move || {
            let Ok((mut stream, _)) = listener.accept() else {
                return;
            };
            let mut sink = Vec::new();
            let _ = stream.read_to_end(&mut sink);
        });
        format!("http://127.0.0.1:{port}")
    }

    fn store_dir(name: &str) -> std::path::PathBuf {
        let dir =
            std::env::temp_dir().join(format!("purple-matrix-rs-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    /// An account whose wake signals the returned channel.
    fn account() -> (Account, mpsc::Receiver<()>) {
        let (tx, rx) = mpsc::channel();
        let account = Account::new(move || {
            let _ = tx.send(());
        })
        .expect("the runtime thread starts");
        (account, rx)
    }

    fn sign_in_outcome(homeserver: &str, dir: &Path, token: &str) -> (Account, Event) {
        let (account, woken) = account();
        account.login(homeserver, &dir.to_string_lossy(), token);
        woken
            .recv_timeout(Duration::from_secs(60))
            .expect("the wake fires once the outcome waits");
        let event = account.next_event().expect("the outcome is the event");
        assert_eq!(account.next_event(), None);
        (account, event)
    }

    /// Every file under `dir`, recursively, with its bytes.
    fn files_under(dir: &Path) -> Vec<(std::path::PathBuf, Vec<u8>)> {
        let mut files = Vec::new();
        for entry in std::fs::read_dir(dir).expect("the directory is readable") {
            let path = entry.expect("a directory entry").path();
            if path.is_dir() {
                files.extend(files_under(&path));
            } else {
                files.push((
                    path.clone(),
                    std::fs::read(&path).expect("the file is readable"),
                ));
            }
        }
        files
    }

    #[test]
    fn a_good_token_connects_as_its_user_and_is_written_to_no_file() {
        let dir = store_dir("connected");
        let homeserver = homeserver_answering(
            "200 OK",
            r#"{"user_id":"@probe:localhost","device_id":"PROBEDEVICE","is_guest":false}"#,
        );
        let (account, event) = sign_in_outcome(&homeserver, &dir, TOKEN);
        assert_eq!(
            event,
            Event::Connected {
                user_id: "@probe:localhost".to_owned()
            }
        );
        account.close();

        // The session was restored, so the SDK wrote its stores — and the token is in none of them.
        let files = files_under(&dir);
        assert!(
            files
                .iter()
                .any(|(path, _)| path.ends_with("matrix-sdk-state.sqlite3")),
            "the state store exists"
        );
        let holding_the_token: Vec<_> = files
            .iter()
            .filter(|(_, bytes)| bytes.windows(TOKEN.len()).any(|w| w == TOKEN.as_bytes()))
            .map(|(path, _)| path.display().to_string())
            .collect();
        assert_eq!(holding_the_token, Vec::<String>::new());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_rejected_token_is_an_authentication_failure_with_the_servers_message() {
        let dir = store_dir("rejected");
        let homeserver = homeserver_answering(
            "401 Unauthorized",
            r#"{"errcode":"M_UNKNOWN_TOKEN","error":"Invalid access token passed."}"#,
        );
        let (account, event) = sign_in_outcome(&homeserver, &dir, TOKEN);
        assert_eq!(
            event,
            Event::Error {
                reason: ErrorReason::AuthenticationFailed,
                message: "the homeserver rejected the access token: [401 / M_UNKNOWN_TOKEN] Invalid access token passed.".to_owned(),
            }
        );
        account.close();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_token_without_a_device_is_refused() {
        let dir = store_dir("no-device");
        let homeserver = homeserver_answering(
            "200 OK",
            r#"{"user_id":"@probe:localhost","is_guest":false}"#,
        );
        let (account, event) = sign_in_outcome(&homeserver, &dir, TOKEN);
        assert_eq!(
            event,
            Event::Error {
                reason: ErrorReason::AuthenticationFailed,
                message:
                    "the access token belongs to no device, and end-to-end encryption needs one"
                        .to_owned(),
            }
        );
        account.close();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn an_unreachable_homeserver_is_a_network_error_and_the_thread_still_ends() {
        let dir = store_dir("unreachable");
        // Port 1 on the loopback: nothing listens there, so the connection is refused.
        let (account, event) = sign_in_outcome("http://127.0.0.1:1", &dir, TOKEN);
        let Event::Error { reason, message } = event else {
            panic!("{event:?}");
        };
        assert_eq!(reason, ErrorReason::Network);
        // The rest of the message is the OS's text for the refused connection, which varies.
        assert!(
            message.starts_with("the homeserver was not reached: "),
            "{message}"
        );
        account.close();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn no_token_is_refused_before_anything_is_sent() {
        let dir = store_dir("no-token");
        let (account, event) = sign_in_outcome("http://127.0.0.1:1", &dir, "");
        assert_eq!(
            event,
            Event::Error {
                reason: ErrorReason::AuthenticationFailed,
                message: "no access token: put it in the password field".to_owned(),
            }
        );
        account.close();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn closing_during_a_sign_in_abandons_it() {
        let dir = store_dir("abandoned");
        let (account, _woken) = account();
        account.login(&homeserver_silent(), &dir.to_string_lossy(), TOKEN);
        let started = Instant::now();
        account.close();
        // Well under the 30 s the silent homeserver would otherwise cost.
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "{:?}",
            started.elapsed()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn the_outbox_wakes_once_per_batch() {
        let (tx, rx) = mpsc::channel();
        let outbox = Outbox::new(Box::new(move || {
            let _ = tx.send(());
        }));
        let event = || Event::Connected {
            user_id: "@a:b".to_owned(),
        };
        outbox.push(event());
        outbox.push(event());
        assert_eq!(rx.try_iter().count(), 1);
        assert_eq!(outbox.pop(), Some(event()));
        assert_eq!(outbox.pop(), Some(event()));
        assert_eq!(outbox.pop(), None);
        outbox.push(event());
        assert_eq!(rx.try_iter().count(), 1);
    }
}
