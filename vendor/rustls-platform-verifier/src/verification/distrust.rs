use std::sync::Arc;

use rustls::pki_types::CertificateDer;
use rustls::{CertificateError, Error, OtherError};

// Public-key values from rustls-webpki 0.103.16, not certificate fingerprints.
const DISTRUSTED_SPKIS: &[&[u8]] = &[
    include_bytes!("distrusted/EMDVTLSCAG3B.spki.der"),
    include_bytes!("distrusted/EMDVTLSCAG3C.spki.der"),
    include_bytes!("distrusted/EMEVTLSCAG3B.spki.der"),
    include_bytes!("distrusted/EMEVTLSCAG3C.spki.der"),
    include_bytes!("distrusted/EMOVTLSCAG3B.spki.der"),
    include_bytes!("distrusted/EMOVTLSCAG3C.spki.der"),
    include_bytes!("distrusted/EMOVCSCAG3B.spki.der"),
    include_bytes!("distrusted/EMOVCSCAG3C.spki.der"),
];

pub(super) fn check_certificate(certificate: &[u8]) -> Result<(), Error> {
    // Parse only: this does not add the certificate to any trust store.
    let der = CertificateDer::from(certificate);
    let parsed = webpki::anchor_from_trusted_cert(&der).map_err(|error| {
        Error::InvalidCertificate(CertificateError::Other(OtherError(Arc::new(error))))
    })?;
    if DISTRUSTED_SPKIS.contains(&parsed.subject_public_key_info.as_ref()) {
        return Err(Error::InvalidCertificate(CertificateError::Revoked));
    }
    Ok(())
}
