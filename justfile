# purple-matrix-rs: a libpurple protocol plugin for Matrix on the matrix-rust-sdk.
# Copyright (C) 2026 the purple-matrix-rs authors
#
# This program is free software; you can redistribute it and/or modify it under the terms of the
# GNU General Public License as published by the Free Software Foundation; either version 2 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
# even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# General Public License in the file LICENSE for more details.

# purple-matrix-rs: a libpurple protocol plugin for Matrix on the matrix-rust-sdk.
# The Rust half is a static library under the gate; the C half in c/ is linked
# into the loadable plugin by `just plugin`, against the libpurple of the client
# it is built for.
#
# Needs: a stable Rust toolchain, just, clang, and for `check` cargo-deny and
# cargo-machete. Three more are used when installed and skipped when not, so a
# clone anywhere works as it is: chronic (moreutils) keeps a green
# gate silent, medic and bumper are the maintainer's repository and release checks.

chronic := `command -v chronic >/dev/null 2>&1 && echo chronic || true`
medic := `command -v medic || true`
bumper := `command -v bumper || true`

default:
    @just --list

[doc("Format the Rust code")]
fmt:
    cargo fmt

[doc("Lint; warnings are errors")]
lint:
    cargo clippy --all-targets -- -D warnings

[doc("Run the whole test suite")]
test:
    cargo test

[doc("The gate: medic and bumper where installed, format, lint, licences and sources, unused dependencies, tests — silent when green")]
check:
    @if [ -n "{{medic}}" ]; then {{chronic}} medic check; fi
    @if [ -n "{{bumper}}" ]; then {{chronic}} bumper change check; fi
    @{{chronic}} cargo fmt --check
    @{{chronic}} cargo clippy --all-targets -- -D warnings
    @{{chronic}} cargo deny --offline check bans licenses sources
    @{{chronic}} cargo machete
    @{{chronic}} cargo test

[doc("Release build of the Rust static library")]
build:
    cargo build --release

# Where the libpurple headers and the libraries to link against come from.
# Adium's checkout carries both in Frameworks/; elsewhere pkg-config knows.
adium := env("ADIUM", "")

[doc("Link the loadable plugin into plugin/libmatrix.so — `ADIUM=<checkout> just plugin` against Adium's frameworks, plain `just plugin` against pkg-config's libpurple")]
plugin: build
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p plugin
    rustlib=target/release/libpurple_matrix_rs.a
    if [[ -n "{{adium}}" ]]; then
        fw="{{adium}}/Frameworks"
        cflags=(-I"$fw/libpurple.framework/Headers" -I"$fw/libglib.framework/Headers" -I"$fw/libgobject.framework/Headers")
        libs=("$fw/libpurple.framework/Versions/0/libpurple" "$fw/libglib.framework/Versions/2.0.0/libglib" "$fw/libgobject.framework/Versions/2.0.0/libgobject")
        export MACOSX_DEPLOYMENT_TARGET=13.0
    else
        read -r -a cflags < <(pkg-config --cflags purple glib-2.0)
        read -r -a libs < <(pkg-config --libs purple glib-2.0)
    fi
    clang -O2 -fPIC -Wall -Wno-deprecated-declarations -DPURPLE_PLUGINS "${cflags[@]}" -c c/prpl.c -o plugin/prpl.o
    clang -bundle -o plugin/libmatrix.so plugin/prpl.o "$rustlib" "${libs[@]}" \
        -framework Security -framework CoreFoundation -framework SystemConfiguration \
        -lsqlite3 -liconv -lresolv -lc++
    rm -f plugin/prpl.o
    # Local symbols go, as Adium's purple-presage build does; the exports libpurple looks up stay.
    /usr/bin/strip -x plugin/libmatrix.so
    if [[ -n "{{adium}}" ]]; then
        # The linker's own signature is what Xcode's copy phase strips; an ad-hoc one survives it
        # (Adium's Utilities/verify-purple-plugins.sh checks for exactly that).
        codesign -f -s - plugin/libmatrix.so
    fi
    echo "plugin/libmatrix.so: $(du -h plugin/libmatrix.so | cut -f1)"

[doc("Delete the build output — run before every push")]
clean:
    rm -rf target
    rm -rf plugin

[doc("Point git at the repo's hooks in .githooks/")]
hooks:
    git config core.hooksPath .githooks

[doc("Security advisories: RustSec — needs the network")]
audit:
    cargo deny check advisories

[doc("Test coverage as a summary — a report, no threshold")]
coverage:
    cargo llvm-cov --summary-only
