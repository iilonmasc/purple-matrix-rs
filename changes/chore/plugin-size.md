---
purple-matrix-rs: patch
---

The release build is stripped, built with fat LTO and one codegen unit, and just plugin strips the linked bundle: libmatrix.so shrinks from 103,143,552 to 27,600,176 bytes (98 MB to 26 MB) with the same exports and behaviour.
