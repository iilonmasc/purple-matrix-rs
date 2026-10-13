# purple-matrix-rs — Spec

A libpurple protocol plugin for Matrix on the matrix-rust-sdk: the SDK owns the protocol, sync and
end-to-end encryption, a Rust library wraps it behind a small C surface, and `c/prpl.c` is the
libpurple plugin that calls it. Built for Adium's revival fork first, for any libpurple client in
principle.

## Key decisions

None built yet: the skeleton registers the protocol and builds an SDK client with its stores, and
every decision about what the plugin does is in spec 001, below, until its parts land.

## Specified, not built

- [001 — The protocol on the Rust SDK](spec/001-protocol-on-the-rust-sdk.md) — rooms, text,
  encryption, members, history, invites, bookmarks; the skeleton is what is built of it

## Specs

- [001 — The protocol on the Rust SDK](spec/001-protocol-on-the-rust-sdk.md)

Departures from the coding conventions: [DEVIATIONS.md](DEVIATIONS.md)
