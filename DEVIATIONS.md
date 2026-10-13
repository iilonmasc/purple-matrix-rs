# Deviations from the coding conventions

- **The repository is public, on GitHub** (`git/origin-forgejo`) — a libpurple plugin is for every
  libpurple client, not for one person's infrastructure; it lives at
  https://github.com/iilonmasc/purple-matrix-rs where Pidgin and Adium users find it.
  (iilonmasc, 2026-10-13)
- **The gate runs the maintainer's own checks only where they are installed**
  (`just/check-runs-medic`, `just/check-runs-change-check`, `just/check-runs-deny`,
  `just/check-runs-machete`, `just/check-silent`) — `just check` runs `medic`, `bumper` and
  `chronic` through variables that are empty where the tool is missing, and `.githooks/pre-commit`
  commits as git would without `medic`; a public repository has to build and check for a stranger
  with Rust, just, clang, cargo-deny and cargo-machete alone. The lines are there, only spelled
  through a variable, which is why medic cannot see them. (iilonmasc, 2026-10-13)
- **`deny.toml` allows the crate index alone** (`rust/deny-toml`) — the maintainer's private
  registry is not a source of this crate's dependencies, and a public repository names no private
  infrastructure; the licence list and the private-crate rule are as the template has them.
  (iilonmasc, 2026-10-14)
