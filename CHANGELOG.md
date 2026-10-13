# Changelog

## 2026-10-14 — purple-matrix-rs 0.2.1

- The plugin reports its version to libpurple from Cargo.toml instead of a literal that stayed at 0.1.0.

## 2026-10-14 — purple-matrix-rs 0.2.0

- Every source file carries the GPL-2.0-or-later licence header, and the README says why a built plugin, which contains the Apache-2.0 SDK, is distributed under GPLv3.
- The release build is stripped, built with fat LTO and one codegen unit, and just plugin strips the linked bundle: libmatrix.so shrinks from 103,143,552 to 27,600,176 bytes (98 MB to 26 MB) with the same exports and behaviour.
- Messages typed into a chat are sent as plain text, encrypted by the SDK in an encrypted room; the line appears once, as the account's own, when the server's echo arrives, and a failed send shows as an error line in the chat.
- Closing a chat window keeps the account in the room, /leave leaves it on the server, and a room reopens from its state with a bookmark or a reconnect, with the stored rooms opening before the first sync answers.
- Joined rooms open as chats with their members, names and topics, with the room's history shown once on opening; messages, membership and topic changes arrive live, and an invitation shows up as a chat invite that can be accepted or declined.
- Signing in with an access token: the password field holds the token, the account option "The password is an access token" says so, and the Rust side proves it with whoami on a runtime thread of its own, restores the SDK session and reports the connection or its error to libpurple on the main thread. The token is kept by the client and written to no file.
- No personal name in the code: the plugin's author field names the project, and the GPL-2.0 text is in LICENSE
