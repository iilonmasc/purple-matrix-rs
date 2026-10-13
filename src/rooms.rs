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

/// Sync until libpurple closes the account or the sync fails for good. Commands on `inbox` are
/// carried out between and during syncs.
pub async fn serve(
    client: &Client,
    inbox: &mut mpsc::UnboundedReceiver<Command>,
    outbox: &Outbox,
) -> Stopped {
    let mut rooms = Rooms::default();
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
                            rooms.after_sync(client, &response, outbox).await;
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
                    Some(Command::Join { room_id }) => rooms.join(client, &room_id, outbox).await,
                    Some(Command::Reject { room_id }) => rooms.reject(client, &room_id, outbox).await,
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
    /// The invitations announced, so a sync that carries one again does not announce it twice.
    invited: HashSet<OwnedRoomId>,
}

impl Rooms {
    async fn after_sync(&mut self, client: &Client, response: &SyncResponse, outbox: &Outbox) {
        let mut fresh = HashSet::new();
        for room in sorted(client.joined_rooms()) {
            if !self.open.contains_key(room.room_id()) {
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
        }
        for room in sorted(client.invited_rooms()) {
            if self.invited.insert(room.room_id().to_owned()) {
                announce(&room, outbox).await;
            }
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

    /// Accept an invitation, or join a room by its id; the chat opens with the next sync.
    async fn join(&self, client: &Client, room_id: &str, outbox: &Outbox) {
        let failed = |message: String| Event::JoinFailed {
            room_id: room_id.to_owned(),
            message,
        };
        let Ok(id) = RoomId::parse(room_id) else {
            outbox.push(failed(format!("{room_id} is not a room id")));
            return;
        };
        if let Err(e) = client.join_room_by_id(&id).await {
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
            body: message.content.body().to_owned(),
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

        /// One sync response, served once the request line holding `gate` has been seen.
        struct Scripted {
            gate: Option<&'static str>,
            body: Value,
        }

        struct State {
            seen: Vec<String>,
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
            state
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .seen
                .push(format!("{method} {target}"));

            let (status, body) = match (method, target) {
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
                ("POST", t) if t.contains("!spam:localhost/leave") => ("200 OK", json!({})),
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
        //! a message without it. The server is Tim's dev server with the user `bob`, whose rooms
        //! `#encrypted:localhost` and `#unencrypted:localhost` (members Admin, Bob, Tim; no
        //! topic) this test asserts as they stand; it leaves them as it found them, bar one line
        //! it sends as bob into the unencrypted room to see it arrive live, once. bob's account
        //! may be in use elsewhere while the test runs, so other rooms' events are let through.
        //!
        //! ```sh
        //! PURPLE_MATRIX_TEST_HOMESERVER=http://localhost:13622 \
        //! PURPLE_MATRIX_TEST_TOKEN="$(op item get 'Matrix dev: bob' --vault Adium --fields 'label=access token' --reveal)" \
        //! cargo test dev_server -- --nocapture
        //! ```
        //!
        //! The token stays in the environment and the client's memory: it is written to no file,
        //! and the SQLite stores go under a temp dir that is removed at the end.

        use std::collections::HashSet;
        use std::sync::mpsc;
        use std::time::Duration;

        use serde_json::{Value, json};

        use crate::runtime::{Account, Event, Member};

        // Room version 12 ids: no server part.
        const ENCRYPTED: &str = "!heVtGY7Bur-1MiZibul_OfNzEnYeHI9uXStT3asayDA";
        const UNENCRYPTED: &str = "!xIo6dn_Zx7l1B8qE9VyhE2QRu9h0QoMpgT7UrsOoERg";

        fn settings() -> Option<(String, String)> {
            match (
                std::env::var("PURPLE_MATRIX_TEST_HOMESERVER"),
                std::env::var("PURPLE_MATRIX_TEST_TOKEN"),
            ) {
                (Ok(homeserver), Ok(token)) => Some((homeserver, token)),
                _ => {
                    eprintln!(
                        "skipped: PURPLE_MATRIX_TEST_HOMESERVER and PURPLE_MATRIX_TEST_TOKEN are not set"
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
                | Event::LeaveFailed { room_id, .. } => room_id,
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
                Member {
                    user_id: "@tim:localhost".to_owned(),
                    name: "Tim".to_owned(),
                },
            ]
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

        #[test]
        fn the_rooms_open_with_their_members_and_a_line_sent_live_shows_once() {
            let Some((homeserver, token)) = settings() else {
                return;
            };
            let dir = std::env::temp_dir().join(format!(
                "purple-matrix-rs-dev-server-{}",
                std::process::id()
            ));
            let _ = std::fs::remove_dir_all(&dir);
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
            let state: Vec<&Event> = opened
                .iter()
                .filter(|e| !is_line(e) && about(e, &[ENCRYPTED, UNENCRYPTED]))
                .collect();
            assert_eq!(
                state,
                vec![
                    &Event::RoomJoined {
                        room_id: ENCRYPTED.to_owned(),
                        name: "Encrypted".to_owned(),
                        topic: None,
                        members: members(),
                    },
                    &Event::RoomJoined {
                        room_id: UNENCRYPTED.to_owned(),
                        name: "Unencrypted".to_owned(),
                        topic: None,
                        members: members(),
                    },
                ]
            );
            let history = line_ids(&opened);
            let distinct: HashSet<&String> = history.iter().collect();
            assert_eq!(
                distinct.len(),
                history.len(),
                "a history line shown twice: {history:?}"
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
            let repeated: Vec<String> = line_ids(&live)
                .into_iter()
                .filter(|id| history.contains(id))
                .collect();
            assert_eq!(repeated, Vec::<String>::new());
            let in_the_room: Vec<&Event> =
                live.iter().filter(|e| about(e, &[UNENCRYPTED])).collect();
            assert_eq!(in_the_room, vec![live.last().expect("the line")]);

            account.close();
            let _ = std::fs::remove_dir_all(&dir);
        }
    }
}
