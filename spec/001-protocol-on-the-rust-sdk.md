# 001 — The protocol on the Rust SDK

Settled with Tim on 2026-10-13, after a day on purple-matrix in Adium's revival fork: the Gitter
rooms work, but the plugin's upstream stopped in 2023, its encryption never got past decrypting,
and the one library that does Matrix encryption right is the matrix-rust-sdk. So the protocol is
written anew on that SDK, as this repository.

## Decisions

1. **One repository, one plugin, one id.** This repository is the plugin. The Rust library owns
   the SDK; `c/prpl.c` owns libpurple; `just plugin` links the two into `libmatrix.so`. The
   protocol id stays `prpl-matrix`, so an account made for purple-matrix carries on, and the
   client's service binding (Adium's descriptor entry) is untouched.

2. **Signing in.** Stage one: the password field holds an access token, a bool option
   `password_is_token` says so, and the homeserver URL is an option; a token is kept by the client
   (Adium: the keychain), never written into a setting. The SDK's `restore_session` takes it. Stage
   two: the SDK's SSO flow through `purple_notify_uri` and `purple_request_input`, the way Teams
   does it in Adium. Password login comes with the SDK and is offered where a server offers it.

3. **Stage one's scope.** Joined rooms as chats; text in both directions; end-to-end encryption in
   both directions, which the SDK does for an encrypted room without being asked; the member list;
   room name and topic; history on opening; invitations, accepted or declined; `/leave`; bookmarks
   and reopening a room the account is in. Not in stage one: typing, read receipts, media,
   creating rooms or direct chats, presence, device verification, key backup.

4. **Encryption, stage one.** The SDK's stores live under `<purple_user_dir>/matrix/<account>/`.
   The device does not verify itself yet, so other clients show it as unverified; verification and
   key backup are stage two, because both need interaction libpurple has no shape for.

5. **Architecture.** One runtime thread per account owns the SDK client. libpurple's calls become
   messages on a channel into that thread; whatever the thread has to say to libpurple comes back
   through `purple_timeout_add` on the main thread, never from the runtime thread directly. The
   client is built and dropped on its runtime, which the spike of 2026-10-13 showed to be
   mandatory: an SDK client dropped outside its runtime aborts in its SQLite pool's teardown.

6. **What was learned stays true.** Four rules from the purple-matrix branch are acceptance
   criteria here, not suggestions: closing a window is not leaving a room, `/leave` is; nothing is
   echoed locally, the plugin writes what the server confirmed; a room reopens from its state when
   the account is still in it; `chat_info_defaults` answers with the room id, so a bookmark can be
   made.

## Acceptance criteria

1. `just check` is green on a machine with Rust, just, clang, cargo-deny and cargo-machete, and
   nothing of the fleet's.
2. `ADIUM=<checkout> just plugin` produces `plugin/libmatrix.so` that Adium's
   `verify-purple-plugins.sh` accepts: `prpl-matrix` registers against the bundle's own libpurple.
3. An account with a token in the password field and `password_is_token` connects to
   `gitter.ems.host`, and the joined rooms open as chats with their members, names and topics.
4. A message typed into an unencrypted room appears once, after the server confirmed it, and in
   another client.
5. A message typed into an encrypted room arrives encrypted in Element and shows no warning there;
   a message from Element in that room arrives readable.
6. Closing a room's window leaves the account in the room; reconnecting, or opening a bookmark on
   it, brings the room back with its members. `/leave` leaves it on the server.
7. An invitation shows up as a libpurple chat invite; accepting joins, declining leaves it.
8. A room's history, as far as the SDK's timeline holds it, is shown when the room opens, and not
   shown again on the next sync.
9. The token is in no file under the purple user directory.

## Bump level

MINOR: the first release that passes the criteria below is 0.2.0, the skeleton being 0.1.0.

## Parts

| Part | Size | Criteria |
|---|---|---|
| runtime | M | 1, 2, 9 |
| rooms | M | 3, 7, 8 |
| messages | M | 4, 5 |
| returning | S | 6 |

- **runtime** is the per-account runtime thread, the channel in, the main-thread path out, login by
  token with `whoami`, connection state and errors, and the gate and the link against Adium that
  prove the shape; the token staying out of every file is settled here, once.
- **rooms** is sync: joined rooms as chats, members, names, topics, history on open, invitations.
- **messages** is text out and in, encryption by the SDK, the server's echo written as the plugin's
  own line and no local copy.
- **returning** is `chat_leave` as local teardown, `/leave`, reopening a room from its state, and
  `chat_info_defaults` answering with the room id.

The Adium side, a `Dependencies/patches/purple-matrix-rs/` in the fork with a pinned commit, a
`build.sh` that clones, checks and runs `just plugin`, the binary in `PurplePlugins/` and the
purple-matrix patch directory removed, is the fork's work, not a part here; it follows the release
that passes criterion 2. Each part lands on `dev` with its changelog fragment.

## Out of scope, for later specs

Typing and read receipts; media in both directions; creating rooms and direct chats; presence;
device verification; key backup; the SSO flow; Pidgin's own quirks, which nobody has looked at.
