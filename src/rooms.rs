//! Sync, once the account is signed in: joined rooms as chats with their members, names and
//! topics, history when a room opens, live messages, membership and topic changes, invitations.
//!
//! The loop calls the SDK's `sync_once` and reads its response itself, instead of registering
//! event handlers: the response says which rooms changed, and the loop owns the one piece of
//! state the SDK does not keep — what libpurple has already been told. A room is "open" once
//! [`Event::RoomJoined`] went out for it; from then on a sync's timeline is shown live, and the
//! event ids shown are remembered so history and a later sync never show a line twice.
//!
//! Commands from libpurple arrive on the same inbox the runtime reads, so a join or a decline
//! runs here while a sync is parked in its long poll.

use std::collections::{HashMap, HashSet, VecDeque};
use std::pin::pin;
use std::time::Duration;

use matrix_sdk::config::SyncSettings;
use matrix_sdk::deserialized_responses::{TimelineEvent, TimelineEventKind};
use matrix_sdk::room::MessagesOptions;
use matrix_sdk::ruma::events::room::member::MembershipState;
use matrix_sdk::ruma::events::room::message::{MessageType, RoomMessageEventContent};
use matrix_sdk::ruma::events::{
    AnySyncMessageLikeEvent, AnySyncStateEvent, AnySyncTimelineEvent, SyncMessageLikeEvent,
    SyncStateEvent,
};
use matrix_sdk::ruma::{OwnedEventId, OwnedRoomId, OwnedUserId, RoomId, UInt, UserId};
use matrix_sdk::sync::SyncResponse;
use matrix_sdk::{Client, Error, HttpError, Room, RoomMemberships, RoomState};
use tokio::sync::mpsc;

use crate::login::ErrorReason;
use crate::runtime::{Command, Event, Member, Outbox};

/// The long poll: how long the homeserver may hold a sync before answering with nothing.
const SYNC_TIMEOUT: Duration = Duration::from_secs(30);
/// How many lines a room shows as history when it opens.
const HISTORY_LIMIT: u32 = 50;
/// How many shown event ids a chat remembers; older ones can no longer come back as live events.
const SHOWN_BOUND: usize = 512;
/// The waits before a sync that failed on the network is tried again; after the last, the
/// account reports the error and libpurple's own reconnect takes over.
const RETRY_DELAYS: [Duration; 3] = [
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(4),
];

/// Why [`serve`] returned.
pub enum Stopped {
    /// libpurple closed the account; the runtime ends.
    Shutdown,
    /// The sync failed for good and the error went out; the runtime waits for the close.
    SyncFailed,
}

/// The command inbox, with a way to wait on a request and still hear the shutdown. Commands
/// that arrive while a request is awaited are held and handed out first by the next `recv`.
struct Inbox<'a> {
    rx: &'a mut mpsc::UnboundedReceiver<Command>,
    held: VecDeque<Command>,
}

impl Inbox<'_> {
    /// The next command; `None` when the sender is gone. Cancel-safe, so it can sit in a `select!`.
    async fn recv(&mut self) -> Option<Command> {
        match self.held.pop_front() {
            Some(command) => Some(command),
            None => self.rx.recv().await,
        }
    }

    /// Await `request` unless the shutdown comes first: every SDK request in the loop goes
    /// through here, since one can take minutes (the SDK retries a members request for as long
    /// as 60 s a try) and libpurple's close waits for the thread on its main thread. `None` is
    /// the shutdown; the request is dropped inside the runtime.
    async fn until_shutdown<T>(&mut self, request: impl Future<Output = T>) -> Option<T> {
        let mut request = pin!(request);
        loop {
            tokio::select! {
                done = &mut request => return Some(done),
                next = self.rx.recv() => match next {
                    Some(Command::Shutdown) | None => return None,
                    Some(other) => self.held.push_back(other),
                },
            }
        }
    }
}

/// Sync until libpurple closes the account or the sync fails for good. Commands on `inbox` are
/// carried out between and during syncs.
pub async fn serve(
    client: &Client,
    inbox: &mut mpsc::UnboundedReceiver<Command>,
    outbox: &Outbox,
) -> Stopped {
    let mut inbox = Inbox {
        rx: inbox,
        held: VecDeque::new(),
    };
    let mut rooms = Rooms::default();
    // The rooms the store already knows open before the first sync answers: with a store that is
    // not new, that sync is incremental and waits out its long poll in a quiet account.
    let from_store = rooms.open_joined(client, outbox);
    if inbox.until_shutdown(from_store).await.is_none() {
        return Stopped::Shutdown;
    }
    let mut failures = 0usize;
    loop {
        let delay = failures.checked_sub(1).map(|i| RETRY_DELAYS[i]);
        let mut syncing = pin!(async move {
            if let Some(delay) = delay {
                tokio::time::sleep(delay).await;
            }
            client
                .sync_once(SyncSettings::default().timeout(SYNC_TIMEOUT))
                .await
        });
        loop {
            tokio::select! {
                outcome = &mut syncing => {
                    match outcome {
                        Ok(response) => {
                            failures = 0;
                            let work = rooms.after_sync(client, &response, outbox);
                            if inbox.until_shutdown(work).await.is_none() {
                                return Stopped::Shutdown;
                            }
                        }
                        Err(e) if is_network(&e) && failures < RETRY_DELAYS.len() => failures += 1,
                        Err(e) => {
                            outbox.push(Event::Error {
                                reason: reason_of(&e),
                                message: format!("the sync failed: {e}"),
                            });
                            return Stopped::SyncFailed;
                        }
                    }
                    break;
                }
                next = inbox.recv() => match next {
                    Some(Command::Shutdown) | None => return Stopped::Shutdown,
                    Some(Command::Join { room_id }) => {
                        let work = rooms.join(client, &room_id, outbox);
                        if inbox.until_shutdown(work).await.is_none() {
                            return Stopped::Shutdown;
                        }
                    }
                    Some(Command::Close { room_id }) => rooms.close(&room_id),
                    Some(Command::Leave { room_id }) => {
                        let work = rooms.leave(client, &room_id, outbox);
                        if inbox.until_shutdown(work).await.is_none() {
                            return Stopped::Shutdown;
                        }
                    }
                    Some(Command::Reject { room_id }) => {
                        let work = rooms.reject(client, &room_id, outbox);
                        if inbox.until_shutdown(work).await.is_none() {
                            return Stopped::Shutdown;
                        }
                    }
                    Some(Command::Send { room_id, body }) => {
                        let work = rooms.send(client, &room_id, body, outbox);
                        if inbox.until_shutdown(work).await.is_none() {
                            return Stopped::Shutdown;
                        }
                    }
                    Some(Command::Login { .. }) => outbox.push(Event::Error {
                        reason: ErrorReason::Other,
                        message: "the account is already signed in".to_owned(),
                    }),
                },
            }
        }
    }
}

/// A failure at the network layer, where a retry can help; the SDK retries a transient server
/// error (a 5xx, a rate limit) inside `sync_once` on its own.
fn is_network(e: &Error) -> bool {
    matches!(e, Error::Http(http) if matches!(**http, HttpError::Reqwest(_)))
}

/// libpurple's reason for a sync that failed for good.
fn reason_of(e: &Error) -> ErrorReason {
    use matrix_sdk::ruma::api::error::ErrorKind;
    let Error::Http(http) = e else {
        return ErrorReason::Other;
    };
    if matches!(**http, HttpError::Reqwest(_)) {
        return ErrorReason::Network;
    }
    match http.client_api_error_kind() {
        Some(ErrorKind::UnknownToken { .. } | ErrorKind::Forbidden) => {
            ErrorReason::AuthenticationFailed
        }
        _ => ErrorReason::Other,
    }
}

/// What the loop knows about the rooms libpurple has been told about.
#[derive(Default)]
struct Rooms {
    /// The chats open on the libpurple side.
    open: HashMap<OwnedRoomId, Chat>,
    /// The joined rooms whose window was closed: the account stays in them, a sync does not
    /// bring them back, and a join opens them again.
    closed: HashSet<OwnedRoomId>,
    /// The invitations announced, so a sync that carries one again does not announce it twice.
    invited: HashSet<OwnedRoomId>,
}

impl Rooms {
    async fn after_sync(&mut self, client: &Client, response: &SyncResponse, outbox: &Outbox) {
        let mut fresh = HashSet::new();
        for room in sorted(client.joined_rooms()) {
            if !self.open.contains_key(room.room_id()) && !self.closed.contains(room.room_id()) {
                self.open(&room, outbox).await;
                fresh.insert(room.room_id().to_owned());
            }
        }
        for (room_id, update) in &response.rooms.joined {
            let (Some(room), Some(chat)) = (client.get_room(room_id), self.open.get_mut(room_id))
            else {
                continue;
            };
            // A room opened in this round was announced with its state as it is now, so the
            // membership and topic changes in its timeline are already in what went out.
            let is_fresh = fresh.contains(room_id);
            for event in &update.timeline.events {
                let Some(happening) = interpret(event) else {
                    continue;
                };
                if is_fresh && !happening.is_line() {
                    continue;
                }
                chat.show(&room, happening, outbox).await;
            }
            self.invited.remove(room_id);
        }
        for room_id in response.rooms.left.keys() {
            if self.open.remove(room_id).is_some() {
                outbox.push(Event::RoomLeft {
                    room_id: room_id.to_string(),
                });
            }
            self.invited.remove(room_id);
            self.closed.remove(room_id);
        }
        for room in sorted(client.invited_rooms()) {
            if self.invited.insert(room.room_id().to_owned()) {
                announce(&room, outbox).await;
            }
        }
    }

    /// Open every joined room the client already knows, from its store.
    async fn open_joined(&mut self, client: &Client, outbox: &Outbox) {
        for room in sorted(client.joined_rooms()) {
            self.open(&room, outbox).await;
        }
    }

    /// Tell libpurple about a joined room: the chat with its members, name and topic, then the
    /// history the homeserver holds, oldest line first.
    async fn open(&mut self, room: &Room, outbox: &Outbox) {
        let room_id = room.room_id();
        let mut chat = Chat::default();
        let members = match room.members(RoomMemberships::JOIN).await {
            Ok(members) => members,
            // The request failed; the store's view is what there is.
            Err(_) => room
                .members_no_sync(RoomMemberships::JOIN)
                .await
                .unwrap_or_default(),
        };
        let mut list: Vec<Member> = members
            .iter()
            .map(|m| Member {
                user_id: m.user_id().to_string(),
                name: member_name(m),
            })
            .collect();
        list.sort_by(|a, b| a.user_id.cmp(&b.user_id));
        for member in &members {
            chat.names
                .insert(member.user_id().to_owned(), member_name(member));
        }
        outbox.push(Event::RoomJoined {
            room_id: room_id.to_string(),
            name: display_name(room).await,
            topic: room.topic(),
            members: list,
        });

        let mut options = MessagesOptions::backward();
        options.limit = UInt::from(HISTORY_LIMIT);
        match room.messages(options).await {
            Ok(messages) => {
                for event in messages.chunk.iter().rev() {
                    if let Some(happening) = interpret(event).filter(Happening::is_line) {
                        chat.show(room, happening, outbox).await;
                    }
                }
            }
            Err(e) => outbox.push(Event::Notice {
                room_id: room_id.to_string(),
                body: format!("The room's history could not be fetched: {e}"),
                timestamp: now(),
            }),
        }
        self.open.insert(room_id.to_owned(), chat);
    }

    /// Open a room by its id. A room the account is in opens from the state right away, with the
    /// members, name, topic and history it has on connect (a reopened window, a bookmark); an
    /// invitation is accepted and any other room joined, and the chat opens with the next sync.
    async fn join(&mut self, client: &Client, room_id: &str, outbox: &Outbox) {
        let failed = |message: String| Event::JoinFailed {
            room_id: room_id.to_owned(),
            message,
        };
        let Ok(id) = RoomId::parse(room_id) else {
            outbox.push(failed(format!("{room_id} is not a room id")));
            return;
        };
        if let Some(room) = client
            .get_room(&id)
            .filter(|room| room.state() == RoomState::Joined)
        {
            // A window that is open already has nothing to be told again.
            if !self.open.contains_key(&id) {
                self.closed.remove(&id);
                self.open(&room, outbox).await;
            }
            return;
        }
        if let Err(e) = client.join_room_by_id(&id).await {
            outbox.push(failed(e.to_string()));
        }
    }

    /// Send a text message into a joined room. The SDK encrypts it where the room is encrypted
    /// and shares the room key with the members' devices first. Nothing is shown here: the
    /// line is the server's echo, which the next sync delivers through [`Chat::show`].
    async fn send(&self, client: &Client, room_id: &str, body: String, outbox: &Outbox) {
        let failed = |message: String, text: String| Event::SendFailed {
            room_id: room_id.to_owned(),
            text,
            message,
        };
        let room = RoomId::parse(room_id)
            .ok()
            .and_then(|id| client.get_room(&id))
            .filter(|room| room.state() == RoomState::Joined);
        let Some(room) = room else {
            outbox.push(failed("the room is not joined".to_owned(), body));
            return;
        };
        if let Err(e) = room
            .send(RoomMessageEventContent::text_plain(body.clone()))
            .await
        {
            outbox.push(failed(e.to_string(), body));
        }
    }

    /// The window of a room was closed: forget the chat and what was shown in it. The account
    /// stays in the room; nothing goes to the server.
    fn close(&mut self, room_id: &str) {
        let Ok(id) = RoomId::parse(room_id) else {
            return;
        };
        if self.open.remove(&id).is_some() {
            self.closed.insert(id);
        }
    }

    /// Leave a joined room on the server. The chat closes when the sync reports the room as left.
    async fn leave(&mut self, client: &Client, room_id: &str, outbox: &Outbox) {
        let failed = |message: String| Event::LeaveFailed {
            room_id: room_id.to_owned(),
            message,
        };
        let room = RoomId::parse(room_id)
            .ok()
            .and_then(|id| client.get_room(&id))
            .filter(|room| room.state() == RoomState::Joined);
        let Some(room) = room else {
            outbox.push(failed("the room is not joined".to_owned()));
            return;
        };
        if let Err(e) = room.leave().await {
            outbox.push(failed(e.to_string()));
        }
    }

    /// Decline an invitation: leave the room the account is invited to.
    async fn reject(&mut self, client: &Client, room_id: &str, outbox: &Outbox) {
        let failed = |message: String| Event::LeaveFailed {
            room_id: room_id.to_owned(),
            message,
        };
        let room = RoomId::parse(room_id)
            .ok()
            .and_then(|id| client.get_room(&id))
            .filter(|room| room.state() == RoomState::Invited);
        let Some(room) = room else {
            outbox.push(failed("there is no invitation to this room".to_owned()));
            return;
        };
        match room.leave().await {
            Ok(()) => {
                self.invited.remove(room.room_id());
            }
            Err(e) => outbox.push(failed(e.to_string())),
        }
    }
}

/// The store hands rooms out in no fixed order; chats open, and invitations arrive, by room id.
fn sorted(mut rooms: Vec<Room>) -> Vec<Room> {
    rooms.sort_by(|a, b| a.room_id().cmp(b.room_id()));
    rooms
}

/// An invitation, as libpurple shows it: the room's name and who invited.
async fn announce(room: &Room, outbox: &Outbox) {
    let inviter = match room.invite_details().await {
        Ok(invite) => invite
            .inviter
            .map(|m| member_name(&m))
            .unwrap_or_else(|| invite.inviter_id.to_string()),
        Err(_) => String::new(),
    };
    outbox.push(Event::Invited {
        room_id: room.room_id().to_string(),
        name: display_name(room).await,
        inviter,
    });
}

/// The room's name as the SDK computes it: the name set, else the alias, else from the members.
async fn display_name(room: &Room) -> String {
    match room.display_name().await {
        Ok(name) => name.to_string(),
        Err(_) => room.room_id().to_string(),
    }
}

/// The name a member goes by in the chat: the display name, the user id's local part without
/// one, and the user id appended where two members share a display name.
fn member_name(member: &matrix_sdk::room::RoomMember) -> String {
    if member.name_ambiguous() {
        format!("{} ({})", member.name(), member.user_id())
    } else {
        member.name().to_owned()
    }
}

fn now() -> i64 {
    i64::from(matrix_sdk::ruma::MilliSecondsSinceUnixEpoch::now().as_secs())
}

/// One open chat on the libpurple side: the names its members were announced under, and the
/// events shown in it.
#[derive(Default)]
struct Chat {
    names: HashMap<OwnedUserId, String>,
    shown: Shown,
}

impl Chat {
    /// Turn what happened in the room into events for libpurple, each line once.
    async fn show(&mut self, room: &Room, happening: Happening, outbox: &Outbox) {
        let room_id = room.room_id().to_string();
        match happening {
            Happening::Message {
                event_id,
                sender,
                body,
                timestamp,
            } => {
                if self.shown.insert(event_id.clone()) {
                    outbox.push(Event::Message {
                        room_id,
                        event_id: event_id.to_string(),
                        sender: self.sender_name(room, &sender).await,
                        body,
                        timestamp,
                    });
                }
            }
            Happening::Undecryptable {
                event_id,
                sender,
                timestamp,
            } => {
                if self.shown.insert(event_id) {
                    outbox.push(Event::Notice {
                        room_id,
                        body: format!(
                            "An encrypted message from {} could not be decrypted.",
                            self.sender_name(room, &sender).await
                        ),
                        timestamp,
                    });
                }
            }
            Happening::Membership {
                user_id,
                membership,
                previous,
                display_name,
            } => self.membership(room_id, user_id, membership, previous, display_name, outbox),
            Happening::Name(name) => outbox.push(Event::RoomNamed { room_id, name }),
            Happening::Topic { sender, topic } => outbox.push(Event::RoomTopic {
                room_id,
                who: self.sender_name(room, &sender).await,
                topic,
            }),
        }
    }

    fn membership(
        &mut self,
        room_id: String,
        user_id: OwnedUserId,
        membership: MembershipState,
        previous: Option<MembershipState>,
        display_name: Option<String>,
        outbox: &Outbox,
    ) {
        let was_in = previous == Some(MembershipState::Join);
        match membership {
            MembershipState::Join => {
                let name = display_name.unwrap_or_else(|| user_id.localpart().to_owned());
                let old = self.names.insert(user_id.clone(), name.clone());
                match old {
                    Some(old) if was_in && old != name => outbox.push(Event::MemberRenamed {
                        room_id,
                        old_name: old,
                        name,
                    }),
                    Some(_) if was_in => {}
                    _ => outbox.push(Event::MemberJoined {
                        room_id,
                        member: Member {
                            user_id: user_id.to_string(),
                            name,
                        },
                    }),
                }
            }
            MembershipState::Leave | MembershipState::Ban => {
                if let Some(name) = self.names.remove(&user_id) {
                    outbox.push(Event::MemberLeft { room_id, name });
                }
            }
            _ => {}
        }
    }

    /// The name a sender goes by in the chat; a sender who is not a member any more goes by the
    /// name the store last saw, else by the user id.
    async fn sender_name(&self, room: &Room, sender: &UserId) -> String {
        if let Some(name) = self.names.get(sender) {
            return name.clone();
        }
        match room.get_member_no_sync(sender).await {
            Ok(Some(member)) => member_name(&member),
            _ => sender.to_string(),
        }
    }
}

/// The event ids shown in a chat, the newest [`SHOWN_BOUND`] of them.
#[derive(Default)]
struct Shown {
    ids: HashSet<OwnedEventId>,
    order: VecDeque<OwnedEventId>,
}

impl Shown {
    /// Remember `id`; `true` when it was not shown before.
    fn insert(&mut self, id: OwnedEventId) -> bool {
        if !self.ids.insert(id.clone()) {
            return false;
        }
        self.order.push_back(id);
        if self.order.len() > SHOWN_BOUND
            && let Some(oldest) = self.order.pop_front()
        {
            self.ids.remove(&oldest);
        }
        true
    }
}

/// What a timeline event means for the chat.
#[derive(Debug, PartialEq, Eq)]
enum Happening {
    Message {
        event_id: OwnedEventId,
        sender: OwnedUserId,
        body: String,
        timestamp: i64,
    },
    Undecryptable {
        event_id: OwnedEventId,
        sender: OwnedUserId,
        timestamp: i64,
    },
    Membership {
        user_id: OwnedUserId,
        membership: MembershipState,
        previous: Option<MembershipState>,
        display_name: Option<String>,
    },
    Name(String),
    Topic {
        sender: OwnedUserId,
        topic: String,
    },
}

impl Happening {
    /// A line in the chat, as opposed to a change of its state.
    fn is_line(&self) -> bool {
        matches!(self, Self::Message { .. } | Self::Undecryptable { .. })
    }
}

/// The text of a message as a chat line: an emote reads as "/me …", which libpurple renders as
/// an action; text and notice are their body.
fn line_of(msgtype: &MessageType) -> String {
    match msgtype {
        MessageType::Emote(emote) => format!("/me {}", emote.body),
        other => other.body().to_owned(),
    }
}

/// Read a timeline event, decrypted by the SDK where it could; `None` for an event the chat
/// does not show.
fn interpret(event: &TimelineEvent) -> Option<Happening> {
    let Ok(parsed) = event.raw().deserialize() else {
        return None;
    };
    let timestamp = i64::from(parsed.origin_server_ts().as_secs());
    if let TimelineEventKind::UnableToDecrypt { .. } = &event.kind {
        return Some(Happening::Undecryptable {
            event_id: parsed.event_id().to_owned(),
            sender: parsed.sender().to_owned(),
            timestamp,
        });
    }
    match parsed {
        AnySyncTimelineEvent::MessageLike(AnySyncMessageLikeEvent::RoomMessage(
            SyncMessageLikeEvent::Original(message),
        )) => Some(Happening::Message {
            event_id: message.event_id,
            sender: message.sender,
            body: line_of(&message.content.msgtype),
            timestamp,
        }),
        AnySyncTimelineEvent::State(AnySyncStateEvent::RoomMember(SyncStateEvent::Original(
            member,
        ))) => Some(Happening::Membership {
            user_id: member.state_key,
            membership: member.content.membership,
            previous: member.unsigned.prev_content.map(|c| c.membership),
            display_name: member.content.displayname,
        }),
        AnySyncTimelineEvent::State(AnySyncStateEvent::RoomName(SyncStateEvent::Original(
            name,
        ))) => Some(Happening::Name(name.content.name)),
        AnySyncTimelineEvent::State(AnySyncStateEvent::RoomTopic(SyncStateEvent::Original(
            topic,
        ))) => Some(Happening::Topic {
            sender: topic.sender,
            topic: topic.content.topic,
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use matrix_sdk::deserialized_responses::{UnableToDecryptInfo, UnableToDecryptReason};
    use matrix_sdk::ruma::serde::Raw;
    use matrix_sdk::ruma::{EventId, owned_event_id, owned_user_id};
    use serde_json::json;

    use super::*;

    fn plaintext(json: serde_json::Value) -> TimelineEvent {
        TimelineEvent::from_plaintext(Raw::from_json_string(json.to_string()).expect("valid json"))
    }

    #[test]
    fn an_emote_reads_as_me_and_a_notice_as_its_body() {
        let line = |msgtype: &str, body: &str| {
            interpret(&plaintext(json!({
                "type": "m.room.message",
                "event_id": "$m1:localhost",
                "sender": "@alice:localhost",
                "origin_server_ts": 1_700_000_000_000u64,
                "content": {"msgtype": msgtype, "body": body}
            })))
            .map(|happening| match happening {
                Happening::Message { body, .. } => body,
                other => panic!("{other:?}"),
            })
        };
        assert_eq!(line("m.emote", "waves"), Some("/me waves".to_owned()));
        assert_eq!(line("m.notice", "deployed"), Some("deployed".to_owned()));
    }

    #[test]
    fn a_message_is_a_line_with_its_sender_body_and_second_timestamp() {
        let event = plaintext(json!({
            "type": "m.room.message",
            "event_id": "$m1:localhost",
            "sender": "@alice:localhost",
            "origin_server_ts": 1_700_000_000_123u64,
            "content": {"msgtype": "m.text", "body": "hello"}
        }));
        assert_eq!(
            interpret(&event),
            Some(Happening::Message {
                event_id: owned_event_id!("$m1:localhost"),
                sender: owned_user_id!("@alice:localhost"),
                body: "hello".to_owned(),
                timestamp: 1_700_000_000,
            })
        );
        assert!(interpret(&event).is_some_and(|h| h.is_line()));
    }

    #[test]
    fn an_undecryptable_event_is_a_notice_not_a_drop() {
        let raw = Raw::from_json_string(
            json!({
                "type": "m.room.encrypted",
                "event_id": "$e1:localhost",
                "sender": "@alice:localhost",
                "origin_server_ts": 1_700_000_000_000u64,
                "content": {"algorithm": "m.megolm.v1.aes-sha2", "ciphertext": "x", "sender_key": "k", "session_id": "s", "device_id": "d"}
            })
            .to_string(),
        )
        .expect("valid json");
        let event = TimelineEvent::from_utd(
            raw,
            UnableToDecryptInfo {
                session_id: Some("s".to_owned()),
                reason: UnableToDecryptReason::MissingMegolmSession {
                    withheld_code: None,
                },
            },
        );
        assert_eq!(
            interpret(&event),
            Some(Happening::Undecryptable {
                event_id: owned_event_id!("$e1:localhost"),
                sender: owned_user_id!("@alice:localhost"),
                timestamp: 1_700_000_000,
            })
        );
    }

    #[test]
    fn membership_name_and_topic_are_state_changes() {
        let joined = plaintext(json!({
            "type": "m.room.member",
            "event_id": "$j1:localhost",
            "sender": "@carol:localhost",
            "state_key": "@carol:localhost",
            "origin_server_ts": 1_700_000_000_000u64,
            "content": {"membership": "join", "displayname": "Carol"},
            "unsigned": {"prev_content": {"membership": "invite"}}
        }));
        assert_eq!(
            interpret(&joined),
            Some(Happening::Membership {
                user_id: owned_user_id!("@carol:localhost"),
                membership: MembershipState::Join,
                previous: Some(MembershipState::Invite),
                display_name: Some("Carol".to_owned()),
            })
        );
        let named = plaintext(json!({
            "type": "m.room.name",
            "event_id": "$n1:localhost",
            "sender": "@alice:localhost",
            "state_key": "",
            "origin_server_ts": 1_700_000_000_000u64,
            "content": {"name": "General"}
        }));
        assert_eq!(
            interpret(&named),
            Some(Happening::Name("General".to_owned()))
        );
        let topic = plaintext(json!({
            "type": "m.room.topic",
            "event_id": "$t1:localhost",
            "sender": "@alice:localhost",
            "state_key": "",
            "origin_server_ts": 1_700_000_000_000u64,
            "content": {"topic": "Chatter"}
        }));
        assert_eq!(
            interpret(&topic),
            Some(Happening::Topic {
                sender: owned_user_id!("@alice:localhost"),
                topic: "Chatter".to_owned(),
            })
        );
        assert!(interpret(&topic).is_some_and(|h| !h.is_line()));
    }

    #[test]
    fn a_reaction_is_not_shown() {
        let event = plaintext(json!({
            "type": "m.reaction",
            "event_id": "$r1:localhost",
            "sender": "@alice:localhost",
            "origin_server_ts": 1_700_000_000_000u64,
            "content": {"m.relates_to": {"rel_type": "m.annotation", "event_id": "$m1:localhost", "key": "👍"}}
        }));
        assert_eq!(interpret(&event), None);
    }

    #[test]
    fn shown_ids_are_remembered_once_and_the_oldest_forgotten_past_the_bound() {
        let mut shown = Shown::default();
        let id = |n: usize| EventId::parse(format!("$e{n}:localhost")).expect("an event id");
        assert!(shown.insert(id(0)));
        assert!(!shown.insert(id(0)));
        for n in 1..=SHOWN_BOUND {
            assert!(shown.insert(id(n)));
        }
        // The bound is SHOWN_BOUND ids: the first went out when the (SHOWN_BOUND + 1)th came in,
        // and is new again; its return pushes the second out.
        assert!(shown.insert(id(0)));
        assert!(!shown.insert(id(2)));
        assert_eq!(shown.ids.len(), SHOWN_BOUND);
        assert_eq!(shown.order.len(), SHOWN_BOUND);
        assert_eq!(shown.order.front(), Some(&id(2)));
        assert_eq!(shown.order.back(), Some(&id(0)));
    }

    /// An outbox whose wake does nothing; the test reads the events back with `pop`.
    fn quiet_outbox() -> Outbox {
        Outbox::new(Box::new(|| {}))
    }

    fn drain(outbox: &Outbox) -> Vec<Event> {
        std::iter::from_fn(|| outbox.pop()).collect()
    }

    #[test]
    fn a_member_joins_once_is_renamed_on_a_new_display_name_and_leaves_under_that_name() {
        let outbox = quiet_outbox();
        let mut chat = Chat::default();
        let carol = owned_user_id!("@carol:localhost");
        let room = "!r:localhost".to_owned();
        chat.membership(
            room.clone(),
            carol.clone(),
            MembershipState::Join,
            Some(MembershipState::Invite),
            Some("Carol".to_owned()),
            &outbox,
        );
        // A join on top of a join with the same name is a profile change the chat does not show.
        chat.membership(
            room.clone(),
            carol.clone(),
            MembershipState::Join,
            Some(MembershipState::Join),
            Some("Carol".to_owned()),
            &outbox,
        );
        chat.membership(
            room.clone(),
            carol.clone(),
            MembershipState::Join,
            Some(MembershipState::Join),
            Some("Caroline".to_owned()),
            &outbox,
        );
        chat.membership(
            room.clone(),
            carol.clone(),
            MembershipState::Leave,
            Some(MembershipState::Join),
            None,
            &outbox,
        );
        // A leave of someone the chat never listed is nothing to show.
        chat.membership(
            room.clone(),
            owned_user_id!("@dave:localhost"),
            MembershipState::Leave,
            Some(MembershipState::Join),
            None,
            &outbox,
        );
        assert_eq!(
            drain(&outbox),
            vec![
                Event::MemberJoined {
                    room_id: room.clone(),
                    member: Member {
                        user_id: carol.to_string(),
                        name: "Carol".to_owned(),
                    },
                },
                Event::MemberRenamed {
                    room_id: room.clone(),
                    old_name: "Carol".to_owned(),
                    name: "Caroline".to_owned(),
                },
                Event::MemberLeft {
                    room_id: room,
                    name: "Caroline".to_owned(),
                },
            ]
        );
        assert!(chat.names.is_empty());
    }

    #[test]
    fn a_member_without_a_display_name_goes_by_the_local_part() {
        let outbox = quiet_outbox();
        let mut chat = Chat::default();
        chat.membership(
            "!r:localhost".to_owned(),
            owned_user_id!("@erin:localhost"),
            MembershipState::Join,
            None,
            None,
            &outbox,
        );
        assert_eq!(
            drain(&outbox),
            vec![Event::MemberJoined {
                room_id: "!r:localhost".to_owned(),
                member: Member {
                    user_id: "@erin:localhost".to_owned(),
                    name: "erin".to_owned(),
                },
            }]
        );
    }

    mod loopback {
        //! The sync loop against a homeserver of this test's making: a loopback socket that
        //! answers whoami, the versions, members, history, join and leave, and serves a script
        //! of sync responses in order — one gated behind a request it has to see first — then
        //! holds the next sync open like a long poll until the client goes.

        use std::io::{Read, Write};
        use std::net::{TcpListener, TcpStream};
        use std::sync::{Arc, Mutex, PoisonError, mpsc};
        use std::time::{Duration, Instant};

        use serde_json::{Value, json};

        use crate::runtime::{Account, Event, Member};

        const GENERAL: &str = "!general:localhost";
        const SECRET: &str = "!secret:localhost";
        const SPAM: &str = "!spam:localhost";
        /// A room whose members request the homeserver never answers.
        const HANG: &str = "!hang:localhost";
        /// An invitation whose join request the homeserver never answers.
        const STUCK: &str = "!stuck:localhost";

        /// One sync response, served once the request line holding `gate` has been seen.
        struct Scripted {
            gate: Option<&'static str>,
            body: Value,
        }

        struct State {
            seen: Vec<String>,
            /// The sends, as the homeserver got them: the request line and the JSON body.
            sent: Vec<(String, Value)>,
            script: Vec<Scripted>,
            next: usize,
        }

        struct Homeserver {
            url: String,
            state: Arc<Mutex<State>>,
        }

        impl Homeserver {
            fn start(script: Vec<Scripted>) -> Self {
                let listener = TcpListener::bind("127.0.0.1:0").expect("a loopback port");
                let port = listener.local_addr().expect("the bound address").port();
                let state = Arc::new(Mutex::new(State {
                    seen: Vec::new(),
                    sent: Vec::new(),
                    script,
                    next: 0,
                }));
                let shared = Arc::clone(&state);
                std::thread::spawn(move || {
                    for stream in listener.incoming().flatten() {
                        let state = Arc::clone(&shared);
                        std::thread::spawn(move || answer(stream, &state));
                    }
                });
                Self {
                    url: format!("http://127.0.0.1:{port}"),
                    state,
                }
            }

            fn seen(&self) -> Vec<String> {
                self.state
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .seen
                    .clone()
            }

            fn sent(&self) -> Vec<(String, Value)> {
                self.state
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .sent
                    .clone()
            }

            /// Wait until `n` sync requests came in: the client asks for the next sync only
            /// once it has processed the one before.
            fn await_syncs(&self, n: usize) {
                let started = Instant::now();
                while self.seen().iter().filter(|l| l.contains("/sync")).count() < n {
                    assert!(
                        started.elapsed() < Duration::from_secs(60),
                        "{:?}",
                        self.seen()
                    );
                    std::thread::sleep(Duration::from_millis(20));
                }
            }
        }

        /// Read one request, answer it, close: every request is a connection of its own.
        fn answer(mut stream: TcpStream, state: &Mutex<State>) {
            let mut head = Vec::new();
            let mut byte = [0u8; 1];
            while !head.ends_with(b"\r\n\r\n") && stream.read(&mut byte).is_ok_and(|n| n == 1) {
                head.push(byte[0]);
            }
            let head = String::from_utf8_lossy(&head).into_owned();
            let line = head.lines().next().unwrap_or_default().to_owned();
            let (method, target) = line.split_once(' ').unwrap_or_default();
            let target = target.trim_end_matches(" HTTP/1.1");
            let content_length: usize = head
                .lines()
                .find_map(|l| {
                    l.strip_prefix("content-length: ")
                        .or(l.strip_prefix("Content-Length: "))
                })
                .and_then(|n| n.parse().ok())
                .unwrap_or_default();
            let mut body = vec![0u8; content_length];
            let _ = stream.read_exact(&mut body);
            let sent_body: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
            {
                let mut state = state.lock().unwrap_or_else(PoisonError::into_inner);
                state.seen.push(format!("{method} {target}"));
                if method == "PUT" {
                    state
                        .sent
                        .push((format!("{method} {target}"), sent_body.clone()));
                }
            }

            let (status, body) = match (method, target) {
                // A send first asks whether the room is encrypted; this room has no such state.
                ("GET", t) if t.contains("/state/m.room.encryption") => (
                    "404 Not Found",
                    json!({"errcode": "M_NOT_FOUND", "error": "Event not found."}),
                ),
                ("PUT", t) if t.contains("/send/m.room.message/") => {
                    if sent_body["body"] == "forbidden" {
                        (
                            "403 Forbidden",
                            json!({"errcode": "M_FORBIDDEN", "error": "You are not allowed to post here"}),
                        )
                    } else {
                        ("200 OK", json!({"event_id": "$echo:localhost"}))
                    }
                }
                ("GET", "/_matrix/client/v3/account/whoami") => (
                    "200 OK",
                    json!({"user_id": "@probe:localhost", "device_id": "PROBEDEVICE", "is_guest": false}),
                ),
                ("GET", "/_matrix/client/versions") => {
                    ("200 OK", json!({"versions": ["v1.1", "v1.5", "v1.11"]}))
                }
                ("GET", t) if t.starts_with("/_matrix/client/v3/sync") => {
                    match next_sync(state) {
                        Some(body) => ("200 OK", body),
                        None => {
                            // The script is spent: hold the long poll until the client goes.
                            let mut sink = Vec::new();
                            let _ = stream.read_to_end(&mut sink);
                            return;
                        }
                    }
                }
                ("GET" | "POST", t)
                    if t.contains("!hang:localhost/members")
                        || t.contains("!stuck:localhost/join") =>
                {
                    // Never answered: hold the request until the client goes.
                    let mut sink = Vec::new();
                    let _ = stream.read_to_end(&mut sink);
                    return;
                }
                ("GET", t) if t.contains("!general:localhost/messages") => (
                    "200 OK",
                    json!({"start": "h0", "end": "h1", "chunk": [
                        message("$m1:localhost", "@alice:localhost", "hello", 1_700_000_000_000u64),
                        message("$m0:localhost", "@probe:localhost", "welcome", 1_699_999_000_000u64),
                    ]}),
                ),
                ("GET", t) if t.contains("/messages") => {
                    ("200 OK", json!({"start": "h0", "end": "h1", "chunk": []}))
                }
                ("GET", t) if t.contains("!general:localhost/members") => (
                    "200 OK",
                    json!({"chunk": [
                        member(GENERAL, "@probe:localhost", "join", Some("Probe")),
                        member(GENERAL, "@alice:localhost", "join", Some("Alice")),
                    ]}),
                ),
                ("GET", t) if t.contains("!secret:localhost/members") => (
                    "200 OK",
                    json!({"chunk": [
                        member(SECRET, "@bob:localhost", "join", Some("Bob")),
                        member(SECRET, "@probe:localhost", "join", Some("Probe")),
                    ]}),
                ),
                ("POST", t) if t.contains("!secret:localhost/join") => {
                    ("200 OK", json!({"room_id": SECRET}))
                }
                ("POST", t)
                    if t.contains("!spam:localhost/leave")
                        || t.contains("!general:localhost/leave") =>
                {
                    ("200 OK", json!({}))
                }
                _ => (
                    "404 Not Found",
                    json!({"errcode": "M_UNRECOGNIZED", "error": "Unrecognized request"}),
                ),
            };
            let body = body.to_string();
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes());
        }

        /// The next scripted sync, once its gate has been seen; `None` when the script is spent.
        fn next_sync(state: &Mutex<State>) -> Option<Value> {
            let started = Instant::now();
            loop {
                let mut state = state.lock().unwrap_or_else(PoisonError::into_inner);
                let scripted = state.script.get(state.next)?;
                let open = scripted
                    .gate
                    .is_none_or(|gate| state.seen.iter().any(|l| l.contains(gate)));
                if open {
                    let body = scripted.body.clone();
                    state.next += 1;
                    return Some(body);
                }
                drop(state);
                assert!(
                    started.elapsed() < Duration::from_secs(60),
                    "the gate never opened"
                );
                std::thread::sleep(Duration::from_millis(20));
            }
        }

        fn message(event_id: &str, sender: &str, body: &str, ts: u64) -> Value {
            json!({
                "type": "m.room.message",
                "event_id": event_id,
                "sender": sender,
                "origin_server_ts": ts,
                "content": {"msgtype": "m.text", "body": body}
            })
        }

        fn member(room_id: &str, user_id: &str, membership: &str, name: Option<&str>) -> Value {
            let mut content = json!({"membership": membership});
            if let Some(name) = name {
                content["displayname"] = json!(name);
            }
            json!({
                "type": "m.room.member",
                "room_id": room_id,
                "event_id": format!("$member-{membership}-{}", user_id.trim_start_matches('@')),
                "sender": user_id,
                "state_key": user_id,
                "origin_server_ts": 1_600_000_000_000u64,
                "content": content
            })
        }

        fn state_event(kind: &str, sender: &str, content: Value) -> Value {
            json!({
                "type": kind,
                "event_id": format!("${kind}:localhost"),
                "sender": sender,
                "state_key": "",
                "origin_server_ts": 1_600_000_000_000u64,
                "content": content
            })
        }

        fn stripped(kind: &str, sender: &str, state_key: &str, content: Value) -> Value {
            json!({"type": kind, "sender": sender, "state_key": state_key, "content": content})
        }

        fn script() -> Vec<Scripted> {
            vec![
                Scripted {
                    gate: None,
                    body: json!({"next_batch": "s1", "rooms": {
                        "join": {GENERAL: {
                            "state": {"events": [
                                state_event("m.room.create", "@alice:localhost", json!({"creator": "@alice:localhost", "room_version": "10"})),
                                state_event("m.room.name", "@alice:localhost", json!({"name": "General"})),
                                state_event("m.room.topic", "@alice:localhost", json!({"topic": "Chatter"})),
                                member(GENERAL, "@alice:localhost", "join", Some("Alice")),
                                member(GENERAL, "@probe:localhost", "join", Some("Probe")),
                            ]},
                            "timeline": {"events": [
                                message("$m1:localhost", "@alice:localhost", "hello", 1_700_000_000_000u64),
                            ], "limited": false, "prev_batch": "p1"}
                        }},
                        "invite": {
                            SECRET: {"invite_state": {"events": [
                                stripped("m.room.name", "@bob:localhost", "", json!({"name": "Secret"})),
                                stripped("m.room.member", "@bob:localhost", "@bob:localhost", json!({"membership": "join", "displayname": "Bob"})),
                                stripped("m.room.member", "@bob:localhost", "@probe:localhost", json!({"membership": "invite"})),
                            ]}},
                            SPAM: {"invite_state": {"events": [
                                stripped("m.room.name", "@mallory:localhost", "", json!({"name": "Spam"})),
                                stripped("m.room.member", "@mallory:localhost", "@probe:localhost", json!({"membership": "invite"})),
                            ]}}
                        }
                    }}),
                },
                Scripted {
                    gate: None,
                    body: json!({"next_batch": "s2", "rooms": {"join": {GENERAL: {
                        "timeline": {"events": [
                            message("$m2:localhost", "@alice:localhost", "live", 1_700_000_100_000u64),
                            json!({
                                "type": "m.room.member",
                                "event_id": "$carol-joins:localhost",
                                "sender": "@carol:localhost",
                                "state_key": "@carol:localhost",
                                "origin_server_ts": 1_700_000_200_000u64,
                                "content": {"membership": "join", "displayname": "Carol"}
                            }),
                            json!({
                                "type": "m.room.topic",
                                "event_id": "$new-topic:localhost",
                                "sender": "@alice:localhost",
                                "state_key": "",
                                "origin_server_ts": 1_700_000_300_000u64,
                                "content": {"topic": "New topic"}
                            }),
                        ], "limited": false}
                    }}}}),
                },
                Scripted {
                    gate: Some("!secret:localhost/join"),
                    body: json!({"next_batch": "s3", "rooms": {"join": {SECRET: {
                        "state": {"events": [
                            state_event("m.room.name", "@bob:localhost", json!({"name": "Secret"})),
                            member(SECRET, "@bob:localhost", "join", Some("Bob")),
                            member(SECRET, "@probe:localhost", "join", Some("Probe")),
                        ]},
                        "timeline": {"events": [], "limited": false}
                    }}}}),
                },
                Scripted {
                    gate: Some("!spam:localhost/leave"),
                    body: json!({"next_batch": "s4", "rooms": {"leave": {SPAM: {}}}}),
                },
            ]
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

        /// Collect events until `n` have arrived, batch by batch as the wake announces them.
        fn collect(
            homeserver: &Homeserver,
            account: &Account,
            woken: &mpsc::Receiver<()>,
            n: usize,
        ) -> Vec<Event> {
            let mut events = Vec::new();
            while events.len() < n {
                woken
                    .recv_timeout(Duration::from_secs(60))
                    .unwrap_or_else(|_| {
                        panic!(
                            "{} of {n} events arrived: {events:?}; the homeserver saw {:?}",
                            events.len(),
                            homeserver.seen()
                        )
                    });
                while let Some(event) = account.next_event() {
                    events.push(event);
                }
            }
            events
        }

        fn room(room_id: &str) -> String {
            room_id.to_owned()
        }

        /// Wait until the homeserver has seen a request line holding `part`.
        fn await_request(homeserver: &Homeserver, part: &str) {
            let started = Instant::now();
            while !homeserver.seen().iter().any(|l| l.contains(part)) {
                assert!(
                    started.elapsed() < Duration::from_secs(30),
                    "no {part} request: {:?}",
                    homeserver.seen()
                );
                std::thread::sleep(Duration::from_millis(20));
            }
        }

        /// The close of an account whose runtime thread waits on a request the homeserver never
        /// answers returns at once; the SDK itself would wait 60 s a try, and libpurple closes on
        /// its main thread.
        fn closes_quickly(account: Account) {
            let started = Instant::now();
            account.close();
            let took = started.elapsed();
            assert!(took < Duration::from_secs(10), "the close took {took:?}");
        }

        #[test]
        fn a_close_while_the_members_of_a_room_are_awaited_returns_at_once() {
            let homeserver = Homeserver::start(vec![Scripted {
                gate: None,
                body: json!({"next_batch": "s1", "rooms": {"join": {HANG: {"state": {"events": [
                    state_event("m.room.create", "@alice:localhost", json!({"creator": "@alice:localhost", "room_version": "10"})),
                    member(HANG, "@probe:localhost", "join", Some("Probe")),
                ]}}}}}),
            }]);
            let dir = std::env::temp_dir().join(format!(
                "purple-matrix-rs-loopback-hang-{}",
                std::process::id()
            ));
            let _ = std::fs::remove_dir_all(&dir);
            let (account, _woken) = account();
            account.login(
                &homeserver.url,
                &dir.to_string_lossy(),
                "syt_loopback_token",
            );
            await_request(&homeserver, "!hang:localhost/members");
            closes_quickly(account);
            let _ = std::fs::remove_dir_all(&dir);
        }

        #[test]
        fn a_close_while_a_join_is_awaited_returns_at_once() {
            let homeserver = Homeserver::start(vec![Scripted {
                gate: None,
                body: json!({"next_batch": "s1", "rooms": {"invite": {STUCK: {"invite_state": {"events": [
                    stripped("m.room.name", "@bob:localhost", "", json!({"name": "Stuck"})),
                    stripped("m.room.member", "@bob:localhost", "@probe:localhost", json!({"membership": "invite"})),
                ]}}}}}),
            }]);
            let dir = std::env::temp_dir().join(format!(
                "purple-matrix-rs-loopback-stuck-{}",
                std::process::id()
            ));
            let _ = std::fs::remove_dir_all(&dir);
            let (account, woken) = account();
            account.login(
                &homeserver.url,
                &dir.to_string_lossy(),
                "syt_loopback_token",
            );
            let events = collect(&homeserver, &account, &woken, 2);
            assert_eq!(events.len(), 2);
            account.join(STUCK);
            await_request(&homeserver, "!stuck:localhost/join");
            closes_quickly(account);
            let _ = std::fs::remove_dir_all(&dir);
        }

        #[test]
        fn a_sent_message_reaches_the_server_as_plain_text_and_shows_once_as_the_servers_echo() {
            let homeserver = Homeserver::start(vec![
                Scripted {
                    gate: None,
                    body: json!({"next_batch": "s1", "rooms": {"join": {GENERAL: {
                        "state": {"events": [
                            state_event("m.room.create", "@alice:localhost", json!({"creator": "@alice:localhost", "room_version": "10"})),
                            member(GENERAL, "@alice:localhost", "join", Some("Alice")),
                            member(GENERAL, "@probe:localhost", "join", Some("Probe")),
                        ]},
                        "timeline": {"events": [], "limited": false, "prev_batch": "p1"}
                    }}}}),
                },
                // The echo comes only once the send has reached the homeserver.
                Scripted {
                    gate: Some("/send/m.room.message/"),
                    body: json!({"next_batch": "s2", "rooms": {"join": {GENERAL: {
                        "timeline": {"events": [
                            message("$echo:localhost", "@probe:localhost", "hello & <b>", 1_700_000_400_000u64),
                        ], "limited": false}
                    }}}}),
                },
            ]);
            let dir = std::env::temp_dir().join(format!(
                "purple-matrix-rs-loopback-send-{}",
                std::process::id()
            ));
            let _ = std::fs::remove_dir_all(&dir);
            let (account, woken) = account();
            account.login(
                &homeserver.url,
                &dir.to_string_lossy(),
                "syt_loopback_token",
            );
            // Connected, the room, and the two history lines.
            let opened = collect(&homeserver, &account, &woken, 4);
            assert_eq!(opened.len(), 4, "{opened:?}");

            account.send(GENERAL, "hello & <b>");
            let echo = collect(&homeserver, &account, &woken, 1);
            assert_eq!(
                echo,
                vec![Event::Message {
                    room_id: room(GENERAL),
                    event_id: "$echo:localhost".to_owned(),
                    sender: "Probe".to_owned(),
                    body: "hello & <b>".to_owned(),
                    timestamp: 1_700_000_400,
                }]
            );
            let sent = homeserver.sent();
            assert_eq!(sent.len(), 1);
            // The transaction id varies from run to run; the rest of the request line is fixed.
            assert!(
                sent[0].0.starts_with(
                    "PUT /_matrix/client/v3/rooms/!general:localhost/send/m.room.message/"
                ),
                "{}",
                sent[0].0
            );
            assert_eq!(
                sent[0].1,
                json!({"msgtype": "m.text", "body": "hello & <b>"})
            );

            // A refusal by the homeserver, and a room the account is not in, are send failures.
            account.send(GENERAL, "forbidden");
            account.send("!elsewhere:localhost", "lost");
            let failures = collect(&homeserver, &account, &woken, 2);
            assert_eq!(
                failures,
                vec![
                    Event::SendFailed {
                        room_id: room(GENERAL),
                        text: "forbidden".to_owned(),
                        message: "the server returned an error: [403 / M_FORBIDDEN] You are not allowed to post here".to_owned(),
                    },
                    Event::SendFailed {
                        room_id: "!elsewhere:localhost".to_owned(),
                        text: "lost".to_owned(),
                        message: "the room is not joined".to_owned(),
                    },
                ]
            );
            account.close();
            let _ = std::fs::remove_dir_all(&dir);
        }

        /// The joined room of the tests below, as its first sync brings it.
        fn general_joined() -> Scripted {
            Scripted {
                gate: None,
                body: json!({"next_batch": "s1", "rooms": {"join": {GENERAL: {
                    "state": {"events": [
                        state_event("m.room.create", "@alice:localhost", json!({"creator": "@alice:localhost", "room_version": "10"})),
                        state_event("m.room.name", "@alice:localhost", json!({"name": "General"})),
                        state_event("m.room.topic", "@alice:localhost", json!({"topic": "Chatter"})),
                        member(GENERAL, "@alice:localhost", "join", Some("Alice")),
                        member(GENERAL, "@probe:localhost", "join", Some("Probe")),
                    ]},
                    "timeline": {"events": [], "limited": false, "prev_batch": "p1"}
                }}}}),
            }
        }

        /// What opening `GENERAL` tells libpurple: the chat, then the history the mock serves.
        fn general_opened() -> Vec<Event> {
            vec![
                Event::RoomJoined {
                    room_id: room(GENERAL),
                    name: "General".to_owned(),
                    topic: Some("Chatter".to_owned()),
                    members: vec![
                        Member {
                            user_id: "@alice:localhost".to_owned(),
                            name: "Alice".to_owned(),
                        },
                        Member {
                            user_id: "@probe:localhost".to_owned(),
                            name: "Probe".to_owned(),
                        },
                    ],
                },
                Event::Message {
                    room_id: room(GENERAL),
                    event_id: "$m0:localhost".to_owned(),
                    sender: "Probe".to_owned(),
                    body: "welcome".to_owned(),
                    timestamp: 1_699_999_000,
                },
                Event::Message {
                    room_id: room(GENERAL),
                    event_id: "$m1:localhost".to_owned(),
                    sender: "Alice".to_owned(),
                    body: "hello".to_owned(),
                    timestamp: 1_700_000_000,
                },
            ]
        }

        #[test]
        fn a_closed_window_keeps_the_room_a_join_reopens_it_from_state_and_leave_reaches_the_server()
         {
            let homeserver = Homeserver::start(vec![
                general_joined(),
                // A line arrives while the window is closed; it is shown by nobody.
                Scripted {
                    gate: Some("/send/m.room.message/"),
                    body: json!({"next_batch": "s2", "rooms": {"join": {GENERAL: {
                        "timeline": {"events": [
                            message("$closed:localhost", "@alice:localhost", "while closed", 1_700_000_500_000u64),
                        ], "limited": false}
                    }}}}),
                },
                Scripted {
                    gate: Some("!general:localhost/leave"),
                    body: json!({"next_batch": "s3", "rooms": {"leave": {GENERAL: {}}}}),
                },
            ]);
            let dir = std::env::temp_dir().join(format!(
                "purple-matrix-rs-loopback-reopen-{}",
                std::process::id()
            ));
            let _ = std::fs::remove_dir_all(&dir);
            let (account, woken) = account();
            account.login(
                &homeserver.url,
                &dir.to_string_lossy(),
                "syt_loopback_token",
            );
            let opened = collect(&homeserver, &account, &woken, 4);
            assert_eq!(opened[1..], general_opened());

            // Closing the window sends nothing; the sync that follows does not bring it back.
            account.close_chat(GENERAL);
            account.send(GENERAL, "poke");
            homeserver.await_syncs(3);
            assert_eq!(account.next_event(), None);

            // A bookmark on the room opens it again, from state, with no join request.
            account.join(GENERAL);
            let reopened = collect(&homeserver, &account, &woken, 3);
            assert_eq!(reopened, general_opened());
            assert!(
                !homeserver.seen().iter().any(|l| l.contains("/join")),
                "{:?}",
                homeserver.seen()
            );

            // A join on a room already open tells nobody again.
            account.join(GENERAL);
            // A room never joined is joined on the server; the mock refuses it.
            account.join("!fresh:localhost");
            let refused = collect(&homeserver, &account, &woken, 1);
            assert_eq!(
                refused,
                vec![Event::JoinFailed {
                    room_id: "!fresh:localhost".to_owned(),
                    message:
                        "the server returned an error: [404 / M_UNRECOGNIZED] Unrecognized request"
                            .to_owned(),
                }]
            );
            assert_eq!(
                homeserver
                    .seen()
                    .iter()
                    .filter(|l| l.contains("/join"))
                    .collect::<Vec<_>>(),
                vec!["POST /_matrix/client/v3/rooms/!fresh:localhost/join"]
            );

            // /leave goes to the server, and the sync that reports the room as left ends the chat.
            account.leave(GENERAL);
            assert_eq!(
                collect(&homeserver, &account, &woken, 1),
                vec![Event::RoomLeft {
                    room_id: room(GENERAL)
                }]
            );
            assert!(
                homeserver
                    .seen()
                    .contains(&"POST /_matrix/client/v3/rooms/!general:localhost/leave".to_owned())
            );

            // Leaving a room the account is not in says so without a request.
            account.leave(GENERAL);
            assert_eq!(
                collect(&homeserver, &account, &woken, 1),
                vec![Event::LeaveFailed {
                    room_id: room(GENERAL),
                    message: "the room is not joined".to_owned(),
                }]
            );
            account.close();
            let _ = std::fs::remove_dir_all(&dir);
        }

        #[test]
        fn a_second_sign_in_on_the_same_store_opens_the_rooms_before_the_first_sync_answers() {
            let dir = std::env::temp_dir().join(format!(
                "purple-matrix-rs-loopback-store-{}",
                std::process::id()
            ));
            let _ = std::fs::remove_dir_all(&dir);

            let first = Homeserver::start(vec![general_joined()]);
            let (once, woken) = account();
            once.login(&first.url, &dir.to_string_lossy(), "syt_loopback_token");
            assert_eq!(collect(&first, &once, &woken, 4)[1..], general_opened());
            once.close();

            // This homeserver answers no sync at all: whatever arrives came from the store.
            let second = Homeserver::start(Vec::new());
            let (again, woken) = account();
            again.login(&second.url, &dir.to_string_lossy(), "syt_loopback_token");
            let events = collect(&second, &again, &woken, 4);
            assert_eq!(
                events[0],
                Event::Connected {
                    user_id: "@probe:localhost".to_owned()
                }
            );
            assert_eq!(events[1..], general_opened());
            again.close();
            let _ = std::fs::remove_dir_all(&dir);
        }

        #[test]
        fn joined_rooms_open_with_history_once_invites_arrive_and_join_and_decline_reach_the_server()
         {
            let homeserver = Homeserver::start(script());
            let dir = std::env::temp_dir().join(format!(
                "purple-matrix-rs-loopback-sync-{}",
                std::process::id()
            ));
            let _ = std::fs::remove_dir_all(&dir);
            let (account, woken) = account();
            account.login(
                &homeserver.url,
                &dir.to_string_lossy(),
                "syt_loopback_token",
            );

            // Sign-in, the first sync with its room, history and invitations, the second sync live.
            let first = collect(&homeserver, &account, &woken, 9);
            assert_eq!(
                first,
                vec![
                    Event::Connected {
                        user_id: "@probe:localhost".to_owned()
                    },
                    Event::RoomJoined {
                        room_id: room(GENERAL),
                        name: "General".to_owned(),
                        topic: Some("Chatter".to_owned()),
                        members: vec![
                            Member {
                                user_id: "@alice:localhost".to_owned(),
                                name: "Alice".to_owned()
                            },
                            Member {
                                user_id: "@probe:localhost".to_owned(),
                                name: "Probe".to_owned()
                            },
                        ],
                    },
                    Event::Message {
                        room_id: room(GENERAL),
                        event_id: "$m0:localhost".to_owned(),
                        sender: "Probe".to_owned(),
                        body: "welcome".to_owned(),
                        timestamp: 1_699_999_000,
                    },
                    Event::Message {
                        room_id: room(GENERAL),
                        event_id: "$m1:localhost".to_owned(),
                        sender: "Alice".to_owned(),
                        body: "hello".to_owned(),
                        timestamp: 1_700_000_000,
                    },
                    Event::Invited {
                        room_id: room(SECRET),
                        name: "Secret".to_owned(),
                        inviter: "Bob".to_owned(),
                    },
                    Event::Invited {
                        room_id: room(SPAM),
                        name: "Spam".to_owned(),
                        inviter: "@mallory:localhost".to_owned(),
                    },
                    Event::Message {
                        room_id: room(GENERAL),
                        event_id: "$m2:localhost".to_owned(),
                        sender: "Alice".to_owned(),
                        body: "live".to_owned(),
                        timestamp: 1_700_000_100,
                    },
                    Event::MemberJoined {
                        room_id: room(GENERAL),
                        member: Member {
                            user_id: "@carol:localhost".to_owned(),
                            name: "Carol".to_owned()
                        },
                    },
                    Event::RoomTopic {
                        room_id: room(GENERAL),
                        who: "Alice".to_owned(),
                        topic: "New topic".to_owned(),
                    },
                ]
            );

            // Accepting the invitation joins on the server; the chat opens with the next sync.
            account.join(SECRET);
            let joined = collect(&homeserver, &account, &woken, 1);
            assert_eq!(
                joined,
                vec![Event::RoomJoined {
                    room_id: room(SECRET),
                    name: "Secret".to_owned(),
                    topic: None,
                    members: vec![
                        Member {
                            user_id: "@bob:localhost".to_owned(),
                            name: "Bob".to_owned()
                        },
                        Member {
                            user_id: "@probe:localhost".to_owned(),
                            name: "Probe".to_owned()
                        },
                    ],
                }]
            );

            // Declining leaves on the server, and the room's departure in the sync is no event.
            account.reject(SPAM);
            homeserver.await_syncs(5);
            assert_eq!(account.next_event(), None);

            // What cannot be joined or declined says so without a request.
            account.join("nope");
            account.reject(GENERAL);
            let refused = collect(&homeserver, &account, &woken, 2);
            assert_eq!(
                refused,
                vec![
                    Event::JoinFailed {
                        room_id: "nope".to_owned(),
                        message: "nope is not a room id".to_owned(),
                    },
                    Event::LeaveFailed {
                        room_id: room(GENERAL),
                        message: "there is no invitation to this room".to_owned(),
                    },
                ]
            );

            let membership: Vec<String> = homeserver
                .seen()
                .into_iter()
                .filter(|l| l.contains("/join") || l.contains("/leave"))
                .collect();
            assert_eq!(
                membership,
                vec![
                    "POST /_matrix/client/v3/rooms/!secret:localhost/join".to_owned(),
                    "POST /_matrix/client/v3/rooms/!spam:localhost/leave".to_owned(),
                ]
            );

            account.close();
            let _ = std::fs::remove_dir_all(&dir);
        }
    }

    mod dev_server {
        //! The sync loop against a real Synapse, switched on by the environment and skipped with
        //! a message without it (the message shows only under `--nocapture`: the harness
        //! swallows the output of a passing test). The server is the dev server with the user
        //! `bob`, whose rooms `#encrypted:localhost` and `#unencrypted:localhost` (no topic;
        //! bob and admin are members, others may be) the tests assert as they stand. They leave
        //! the rooms as they found them, bar one line the rooms test sends as bob into the
        //! unencrypted room to see it arrive live, and one line per room the send test sends
        //! through the plugin. bob's account may be in use elsewhere while the tests run, so
        //! other rooms' events are let through.
        //!
        //! ```sh
        //! PURPLE_MATRIX_TEST_HOMESERVER=http://localhost:13622 \
        //! PURPLE_MATRIX_TEST_TOKEN=<access token of bob> \
        //! PURPLE_MATRIX_TEST_STORE=/absolute/path/to/a/store \
        //! cargo test dev_server -- --nocapture
        //! ```
        //!
        //! `PURPLE_MATRIX_TEST_HOMESERVER` is the URL of the homeserver,
        //! `PURPLE_MATRIX_TEST_TOKEN` an access token of a test user on it and
        //! `PURPLE_MATRIX_TEST_STORE` an absolute path of the directory for the client's stores.
        //!
        //! The token must belong to a device no other client uses: the SDK's crypto store is
        //! that device's identity, and a second store for the same device overwrites its keys
        //! on the server.
        //!
        //! The store directory is created if missing and never removed, so every run is the
        //! same device with the same keys. Both tests use it, one at a time (a `Mutex`): two
        //! clients on one SQLite crypto store at once would damage it. The token stays in the
        //! environment and the client's memory and is written to no file.

        use std::collections::HashSet;
        use std::sync::mpsc;
        use std::time::Duration;

        use serde_json::{Value, json};

        use crate::runtime::{Account, Event, Member};

        // Room version 12 ids: no server part.
        const ENCRYPTED: &str = "!heVtGY7Bur-1MiZibul_OfNzEnYeHI9uXStT3asayDA";
        const UNENCRYPTED: &str = "!xIo6dn_Zx7l1B8qE9VyhE2QRu9h0QoMpgT7UrsOoERg";

        /// One client at a time on the persistent store; held for the whole test.
        static STORE: std::sync::Mutex<()> = std::sync::Mutex::new(());

        type Settings = (
            String,
            String,
            std::path::PathBuf,
            std::sync::MutexGuard<'static, ()>,
        );

        fn settings() -> Option<Settings> {
            match (
                std::env::var("PURPLE_MATRIX_TEST_HOMESERVER"),
                std::env::var("PURPLE_MATRIX_TEST_TOKEN"),
                std::env::var("PURPLE_MATRIX_TEST_STORE"),
            ) {
                (Ok(homeserver), Ok(token), Ok(store)) => {
                    let store = std::path::PathBuf::from(store);
                    assert!(
                        store.is_absolute(),
                        "PURPLE_MATRIX_TEST_STORE must be absolute"
                    );
                    std::fs::create_dir_all(&store).expect("the store directory");
                    // A failed test poisons the lock; the store is still fine for the next.
                    let guard = STORE.lock().unwrap_or_else(|e| e.into_inner());
                    Some((homeserver, token, store, guard))
                }
                _ => {
                    eprintln!(
                        "skipped: PURPLE_MATRIX_TEST_HOMESERVER, PURPLE_MATRIX_TEST_TOKEN and PURPLE_MATRIX_TEST_STORE are not all set"
                    );
                    None
                }
            }
        }

        fn account() -> (Account, mpsc::Receiver<()>) {
            let (tx, rx) = mpsc::channel();
            let account = Account::new(move || {
                let _ = tx.send(());
            })
            .expect("the runtime thread starts");
            (account, rx)
        }

        /// Collect events, batch by batch, until one satisfies `done`.
        fn collect_until(
            account: &Account,
            woken: &mpsc::Receiver<()>,
            done: impl Fn(&Event) -> bool,
        ) -> Vec<Event> {
            let mut events = Vec::new();
            while !events.iter().any(&done) {
                woken
                    .recv_timeout(Duration::from_secs(60))
                    .unwrap_or_else(|_| panic!("the events so far: {events:?}"));
                while let Some(event) = account.next_event() {
                    events.push(event);
                }
            }
            events
        }

        fn is_line(event: &Event) -> bool {
            matches!(event, Event::Message { .. } | Event::Notice { .. })
        }

        /// Whether the event concerns one of `rooms`; the connection events concern none.
        fn about(event: &Event, rooms: &[&str]) -> bool {
            let room_id = match event {
                Event::Connected { .. } | Event::Error { .. } => return false,
                Event::RoomJoined { room_id, .. }
                | Event::RoomLeft { room_id }
                | Event::MemberJoined { room_id, .. }
                | Event::MemberRenamed { room_id, .. }
                | Event::MemberLeft { room_id, .. }
                | Event::RoomNamed { room_id, .. }
                | Event::RoomTopic { room_id, .. }
                | Event::Message { room_id, .. }
                | Event::Notice { room_id, .. }
                | Event::Invited { room_id, .. }
                | Event::JoinFailed { room_id, .. }
                | Event::LeaveFailed { room_id, .. }
                | Event::SendFailed { room_id, .. } => room_id,
            };
            rooms.contains(&room_id.as_str())
        }

        fn line_ids(events: &[Event]) -> Vec<String> {
            events
                .iter()
                .filter_map(|e| match e {
                    Event::Message { event_id, .. } => Some(event_id.clone()),
                    _ => None,
                })
                .collect()
        }

        /// The members the test names, sorted by user id. Other members are tolerated, unnamed.
        fn members() -> Vec<Member> {
            vec![
                Member {
                    user_id: "@admin:localhost".to_owned(),
                    name: "Admin".to_owned(),
                },
                Member {
                    user_id: "@bob:localhost".to_owned(),
                    name: "Bob".to_owned(),
                },
            ]
        }

        /// A chat's members cut down to the ones `members()` names, in the order of `members()`.
        fn named_members(event: &Event) -> Option<Event> {
            let Event::RoomJoined {
                room_id,
                name,
                topic,
                members: all,
            } = event
            else {
                return None;
            };
            let mut named: Vec<Member> = all
                .iter()
                .filter(|m| members().contains(m))
                .map(|m| Member {
                    user_id: m.user_id.clone(),
                    name: m.name.clone(),
                })
                .collect();
            named.sort_by(|a, b| a.user_id.cmp(&b.user_id));
            Some(Event::RoomJoined {
                room_id: room_id.clone(),
                name: name.clone(),
                topic: topic.clone(),
                members: named,
            })
        }

        /// Send a text line as bob through the client API and return its event id and server
        /// timestamp, in seconds.
        fn send_as_bob(homeserver: &str, token: &str, room_id: &str, body: &str) -> (String, i64) {
            let runtime = tokio::runtime::Runtime::new().expect("a runtime for the API calls");
            runtime.block_on(async {
                let http = matrix_sdk::reqwest::Client::new();
                let txn = format!("purple-matrix-rs-{}", std::process::id());
                let sent = http
                    .put(format!(
                        "{homeserver}/_matrix/client/v3/rooms/{room_id}/send/m.room.message/{txn}"
                    ))
                    .bearer_auth(token)
                    .header("Content-Type", "application/json")
                    .body(json!({"msgtype": "m.text", "body": body}).to_string())
                    .send()
                    .await
                    .expect("the send request")
                    .bytes()
                    .await
                    .expect("the send answer");
                let sent: Value = serde_json::from_slice(&sent).expect("json");
                let event_id = sent["event_id"].as_str().expect("an event id").to_owned();
                let event = http
                    .get(format!(
                        "{homeserver}/_matrix/client/v3/rooms/{room_id}/event/{event_id}"
                    ))
                    .bearer_auth(token)
                    .send()
                    .await
                    .expect("the event request")
                    .bytes()
                    .await
                    .expect("the event");
                let event: Value = serde_json::from_slice(&event).expect("json");
                let ts = event["origin_server_ts"].as_i64().expect("a timestamp") / 1000;
                (event_id, ts)
            })
        }

        /// Create a room as bob, with nobody invited, and return its id.
        fn create_room_as_bob(homeserver: &str, token: &str, name: &str) -> String {
            let runtime = tokio::runtime::Runtime::new().expect("a runtime for the API calls");
            runtime.block_on(async {
                let bytes = matrix_sdk::reqwest::Client::new()
                    .post(format!("{homeserver}/_matrix/client/v3/createRoom"))
                    .bearer_auth(token)
                    .header("Content-Type", "application/json")
                    .body(json!({"name": name}).to_string())
                    .send()
                    .await
                    .expect("the createRoom request")
                    .bytes()
                    .await
                    .expect("the createRoom answer");
                let created: Value = serde_json::from_slice(&bytes).expect("json");
                created["room_id"].as_str().expect("a room id").to_owned()
            })
        }

        /// The event as the homeserver holds it, undecrypted: `GET .../event/<id>`.
        fn raw_event(homeserver: &str, token: &str, room_id: &str, event_id: &str) -> Value {
            let runtime = tokio::runtime::Runtime::new().expect("a runtime for the API calls");
            runtime.block_on(async {
                let bytes = matrix_sdk::reqwest::Client::new()
                    .get(format!(
                        "{homeserver}/_matrix/client/v3/rooms/{room_id}/event/{event_id}"
                    ))
                    .bearer_auth(token)
                    .send()
                    .await
                    .expect("the event request")
                    .bytes()
                    .await
                    .expect("the event");
                serde_json::from_slice(&bytes).expect("json")
            })
        }

        #[test]
        fn a_sent_line_comes_back_once_as_the_echo_and_is_encrypted_on_the_wire_where_the_room_is()
        {
            let Some((homeserver, token, dir, _store)) = settings() else {
                return;
            };
            let (account, woken) = account();
            account.login(&homeserver, &dir.to_string_lossy(), &token);
            let mut seen = collect_until(
                &account,
                &woken,
                |e| matches!(e, Event::RoomJoined { room_id, .. } if room_id == UNENCRYPTED),
            );
            if !seen
                .iter()
                .any(|e| matches!(e, Event::RoomJoined { room_id, .. } if room_id == ENCRYPTED))
            {
                seen.extend(collect_until(
                    &account,
                    &woken,
                    |e| matches!(e, Event::RoomJoined { room_id, .. } if room_id == ENCRYPTED),
                ));
            }

            for (room_id, wire_type) in [
                (UNENCRYPTED, "m.room.message"),
                (ENCRYPTED, "m.room.encrypted"),
            ] {
                let body = format!(
                    "purple-matrix-rs send test {} {wire_type}",
                    std::process::id()
                );
                account.send(room_id, &body);
                let is_echo = |e: &Event| matches!(e, Event::Message { body: b, .. } if *b == body);
                let events = collect_until(&account, &woken, is_echo);
                let echoes: Vec<&Event> = events.iter().filter(|e| is_echo(e)).collect();
                assert_eq!(echoes.len(), 1, "{events:?}");
                let Event::Message {
                    room_id: echoed_in,
                    event_id,
                    sender,
                    ..
                } = echoes[0]
                else {
                    panic!("{:?}", echoes[0]);
                };
                // The echo is bob's own line, in the room it was sent to.
                assert_eq!((echoed_in.as_str(), sender.as_str()), (room_id, "Bob"));

                // On the wire the line is what the room demands: the SDK encrypted it without
                // being asked, and the body is nowhere in the event the server holds.
                let raw = raw_event(&homeserver, &token, room_id, event_id);
                assert_eq!(raw["type"], wire_type, "{raw}");
                assert_eq!(raw["sender"], "@bob:localhost");
                if wire_type == "m.room.encrypted" {
                    assert!(raw["content"].get("body").is_none(), "{raw}");
                    assert_eq!(raw["content"]["algorithm"], "m.megolm.v1.aes-sha2");
                } else {
                    assert_eq!(raw["content"]["body"], body.as_str());
                }
                seen.extend(events);
            }
            let ids = line_ids(&seen);
            let distinct: HashSet<&String> = ids.iter().collect();
            assert_eq!(distinct.len(), ids.len(), "a line shown twice: {ids:?}");

            account.close();
        }

        #[test]
        fn the_rooms_open_with_their_members_and_a_line_sent_live_shows_once() {
            let Some((homeserver, token, dir, _store)) = settings() else {
                return;
            };
            let (account, woken) = account();
            account.login(&homeserver, &dir.to_string_lossy(), &token);

            // The connection and the two chats. History lines, if the rooms hold any, come
            // between them and are whatever the server has: they are checked apart, by shape.
            let opened = collect_until(
                &account,
                &woken,
                |e| matches!(e, Event::RoomJoined { room_id, .. } if room_id == UNENCRYPTED),
            );
            assert_eq!(
                opened.first(),
                Some(&Event::Connected {
                    user_id: "@bob:localhost".to_owned()
                })
            );
            let state: Vec<Event> = opened
                .iter()
                .filter(|e| !is_line(e) && about(e, &[ENCRYPTED, UNENCRYPTED]))
                .filter_map(named_members)
                .collect();
            assert_eq!(
                state,
                vec![
                    Event::RoomJoined {
                        room_id: ENCRYPTED.to_owned(),
                        name: "Encrypted".to_owned(),
                        topic: None,
                        members: members(),
                    },
                    Event::RoomJoined {
                        room_id: UNENCRYPTED.to_owned(),
                        name: "Unencrypted".to_owned(),
                        topic: None,
                        members: members(),
                    },
                ]
            );
            // A line sent as bob arrives live, once, as bob's own; nothing of the history comes
            // with that sync.
            let body = format!("purple-matrix-rs rooms test {}", std::process::id());
            let (event_id, timestamp) = send_as_bob(&homeserver, &token, UNENCRYPTED, &body);
            let live = collect_until(
                &account,
                &woken,
                |e| matches!(e, Event::Message { event_id: id, .. } if *id == event_id),
            );
            assert_eq!(
                live.last(),
                Some(&Event::Message {
                    room_id: UNENCRYPTED.to_owned(),
                    event_id: event_id.clone(),
                    sender: "Bob".to_owned(),
                    body,
                    timestamp,
                })
            );
            // `open` announces a room before it fetches its history, so history lines can arrive
            // after `RoomJoined`, in the same batches as the live line: whatever came, no line
            // is shown twice, and the live one once.
            let mut shown = line_ids(&opened);
            shown.extend(line_ids(&live));
            let distinct: HashSet<&String> = shown.iter().collect();
            assert_eq!(distinct.len(), shown.len(), "a line shown twice: {shown:?}");
            assert_eq!(shown.iter().filter(|id| **id == event_id).count(), 1);

            account.close();
        }

        #[test]
        fn a_closed_window_reopens_from_state_and_leave_leaves_a_room_made_for_it() {
            let Some((homeserver, token, dir, _store)) = settings() else {
                return;
            };
            let (account, woken) = account();
            account.login(&homeserver, &dir.to_string_lossy(), &token);
            collect_until(
                &account,
                &woken,
                |e| matches!(e, Event::RoomJoined { room_id, .. } if room_id == UNENCRYPTED),
            );

            // Closing the window and opening the room again: the chat with its members, and
            // the account is still in the room, so nothing is joined.
            account.close_chat(UNENCRYPTED);
            account.join(UNENCRYPTED);
            let reopened = collect_until(
                &account,
                &woken,
                |e| matches!(e, Event::RoomJoined { room_id, .. } if room_id == UNENCRYPTED),
            );
            let state: Vec<Event> = reopened
                .iter()
                .filter(|e| about(e, &[UNENCRYPTED]))
                .filter_map(named_members)
                .collect();
            assert_eq!(
                state,
                vec![Event::RoomJoined {
                    room_id: UNENCRYPTED.to_owned(),
                    name: "Unencrypted".to_owned(),
                    topic: None,
                    members: members(),
                }]
            );

            // A room made for the test, so bob stays in the two others. It stays behind, left.
            let name = format!("purple-matrix-rs leave test {}", std::process::id());
            let made = create_room_as_bob(&homeserver, &token, &name);
            collect_until(
                &account,
                &woken,
                |e| matches!(e, Event::RoomJoined { room_id, .. } if *room_id == made),
            );
            account.leave(&made);
            let left = collect_until(
                &account,
                &woken,
                |e| matches!(e, Event::RoomLeft { room_id } if *room_id == made),
            );
            assert!(
                left.iter().all(|e| !matches!(e, Event::LeaveFailed { .. })),
                "{left:?}"
            );
            account.close();
        }
    }
}
