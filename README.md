# purple-matrix-rs

A [libpurple](https://pidgin.im) protocol plugin for [Matrix](https://matrix.org), written on the
[matrix-rust-sdk](https://github.com/matrix-org/matrix-rust-sdk): the SDK brings sync, rooms,
end-to-end encryption and the login flows, and this plugin brings them to any libpurple client,
Pidgin and Adium among them.

It exists because [purple-matrix](https://github.com/matrix-org/purple-matrix) stopped in 2023
without ever finishing encryption, and because the one library that does Matrix encryption right
today is the Rust SDK's.

## Status

Stage one is built. An account signs in with an access token in the password field (the option
"The password is an access token" says so). Joined rooms open as chats with their members, names,
topics and history; text goes both ways, end-to-end encrypted where the room is; invitations are
accepted or declined; closing a window keeps the account in the room, `/leave` leaves it, and a
bookmark or a reconnect brings a room back. Password and SSO sign-in, device verification and key
backup come later. What is decided, and why, is in [SPEC.md](SPEC.md).

## Shape

| Part | What |
|---|---|
| `src/` | the Rust library: owns the SDK client, exposes a small `extern "C"` surface |
| `c/prpl.c` | the libpurple protocol plugin: owns libpurple, calls into the library |
| `plugin/libmatrix.so` | the loadable plugin, `just plugin` links it |

The Rust half is a static library, so the plugin is one file with no runtime dependencies beyond
the client's own libpurple and glib and the system's TLS and SQLite.

## Building

Needs a stable Rust toolchain, [just](https://github.com/casey/just), clang, and the libpurple
headers and libraries of the client to build for.

    cargo fetch                                  # once: the licence check runs offline over the whole lock
    just check                                   # format, lint, licences, tests
    just plugin                                  # against pkg-config's libpurple
    ADIUM=~/path/to/adium just plugin            # against Adium's bundled frameworks

`check` also runs the maintainer's `medic` and `bumper` where they are installed, and keeps a green
run silent with `chronic` where that is; none of the three is needed.

## Running

The SDK's stores under `<purple user dir>/matrix/<account>/` are the device's encryption identity,
bound to the access token's device id. Never delete them while keeping the token: other clients
then still share room keys with the old identity, and the new store cannot read them. A fresh
start is a new login with a new device id, that is, a new token.

## Licence

GPL-2.0-or-later, as libpurple itself; the SDK is Apache-2.0.
