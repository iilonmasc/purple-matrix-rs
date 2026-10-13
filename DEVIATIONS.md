# Deviations from the coding conventions

- **The repository is public, on GitHub** (`git/origin-forgejo`, `ref/GIT.md`) — a libpurple plugin is for every libpurple client, not for the fleet; it lives at
  https://github.com/iilonmasc/purple-matrix-rs where Pidgin and Adium users find it. (Tim,
  2026-10-13)
- **The gate runs the fleet's tools only where they are installed** (`just/check-runs-medic`,
  `just/check-runs-change-check`, `just/check-runs-deny`, `just/check-runs-machete`,
  `just/check-silent`, `ref/JUSTFILE.md`) — `just check` runs `medic`,
  `bumper` and `chronic` through variables that are empty where the tool is missing, and
  `.githooks/pre-commit` commits as git would without `medic`; a public repository has to build and
  check for a stranger with Rust, just, clang, cargo-deny and cargo-machete alone. The lines are
  there, only spelled through a variable, which is why medic cannot see them. (Tim, 2026-10-13)
