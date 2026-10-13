# purple-matrix-rs

A [libpurple](https://pidgin.im) protocol plugin for [Matrix](https://matrix.org), written on the
[matrix-rust-sdk](https://github.com/matrix-org/matrix-rust-sdk): the SDK brings sync, rooms,
end-to-end encryption and the login flows, and this plugin brings them to any libpurple client,
Pidgin and Adium among them.

It exists because [purple-matrix](https://github.com/matrix-org/purple-matrix) stopped in 2023
without ever finishing encryption, and because the one library that does Matrix encryption right
today is the Rust SDK's.

## Status

Signing in. The protocol registers, an account signs in with an access token in the password
field (the option "The password is an access token" says so), the Rust side proves it with
`whoami` on a runtime thread of its own and restores the SDK session with its SQLite stores — and
that is where it stops: no rooms, no messages yet. What comes next, in which order, is in
[SPEC.md](SPEC.md).

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

`check` also runs the fleet tools `medic` and `bumper` where they are installed, and keeps a green
run silent with `chronic` where that is; none of the three is needed.

## Licence

GPL-2.0-or-later, as libpurple itself; the SDK is Apache-2.0.
