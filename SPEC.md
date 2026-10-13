# purple-matrix-rs — Spec

A libpurple protocol plugin for Matrix on the matrix-rust-sdk: the SDK owns the protocol, sync and
end-to-end encryption, a Rust library wraps it behind a small C surface, and `c/prpl.c` is the
libpurple plugin that calls it. Built for Adium's revival fork first, for any libpurple client in
principle.

## Key decisions

- **One runtime thread per account owns the SDK client** ([spec 001](spec/001-protocol-on-the-rust-sdk.md)).
  libpurple's calls become commands on a channel into that thread; what the thread has to say
  comes back as events in an outbox, which wakes the main thread with `purple_timeout_add(0, …)`.
  libpurple is never called from the runtime thread. The client is built and dropped inside the
  runtime's `block_on` (dropped outside it, the SDK aborts), and every SDK await is selected
  against the shutdown, so closing the account never waits on a request.
- **Signing in, stage one: an access token** ([spec 001](spec/001-protocol-on-the-rust-sdk.md)).
  The token sits in the password field and the option `password_is_token` says so; `whoami` runs
  first, because `restore_session` needs the user and device ids; the token lives in the client's
  memory and in no file or setting. There is no password login yet: every one creates a new
  device, which breaks the encryption identity the stores hold.
- **Closing a window is not leaving a room** ([spec 001](spec/001-protocol-on-the-rust-sdk.md),
  decision 6). `chat_leave` only forgets the chat on the Rust side and sends nothing; `/leave`
  (a purple command, registered on load) leaves the room on the server, and the sync reporting the
  room as left ends the chat.
- **A room reopens from its state** ([spec 001](spec/001-protocol-on-the-rust-sdk.md),
  decision 6). Joining a room the account is in opens it at once with members, name, topic and
  history; an invitation or a room never joined is joined on the server first. On sign-in the
  rooms the store knows open before the first sync, so a quiet account does not wait out the long
  poll.
- **Nothing is echoed locally, and a bookmark carries the room id** ([spec 001](spec/001-protocol-on-the-rust-sdk.md),
  decision 6). A sent line is shown when the server's echo arrives; `chat_info_defaults` answers
  with the room id.

## Specs

- [001 — The protocol on the Rust SDK](spec/001-protocol-on-the-rust-sdk.md)

Departures from the coding conventions: [DEVIATIONS.md](DEVIATIONS.md)
