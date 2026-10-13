---
purple-matrix-rs: minor
---

Signing in with an access token: the password field holds the token, the account option "The password is an access token" says so, and the Rust side proves it with whoami on a runtime thread of its own, restores the SDK session and reports the connection or its error to libpurple on the main thread. The token is kept by the client and written to no file.
