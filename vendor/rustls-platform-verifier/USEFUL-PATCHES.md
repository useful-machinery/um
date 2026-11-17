# Useful Machinery patch

Base: crates.io `rustls-platform-verifier` 0.7.0, package checksum
`26d1e2536ce4f35f4846aa13bff16bd0ff40157cdb14cc056c7b14ba41233ba0`.
Upstream MIT and Apache licenses are retained. This local package is excluded
from the workspace and from repository edition-2024 formatting and duplication
checks; Cargo still builds it as the shared dependency of both HTTPS backends.

The Apple verifier checks every certificate in the chain returned by successful
native trust evaluation against the eight public SPKIs from
`rustls-webpki` 0.103.16 (`src/data/distrusted/`). It includes cached issuers and
anchors absent from the peer-supplied array. Missing chain records and parsing
errors fail closed and retain their diagnostics. Native policy, roots, hostname
and time checks, OCSP, and handshake-signature verification are unchanged.

The upstream changes are limited to the Apple dependency entry, the Apple
success path and resolved-chain helper, the private distrust module, and its
public-key data. Remove this patch only after an upstream verifier provides
equivalent complete-chain distrust. The exact dependency pin prevents a routine
resolution from silently selecting an unpatched release.
