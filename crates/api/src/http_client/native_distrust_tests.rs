use std::error::Error as StdError;
use std::sync::Arc;
use std::time::Duration;

use rustls::client::danger::ServerCertVerifier;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{CertificateError, Error};
use rustls_platform_verifier::Verifier;

// The peer sends only the leaf. The native platform supplies the compromised
// issuer from an anchor/cache path, beyond the peer-supplied certificate array.
#[test]
fn native_resolved_chain_rejects_a_compromised_issuer_absent_from_the_peer_chain()
-> Result<(), Box<dyn StdError>> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let verifier = Verifier::new_with_extra_roots(
        [CertificateDer::from(
            include_bytes!("fixtures/compromised-intermediate.der").as_slice(),
        )],
        provider,
    )?;
    let result = verifier.verify_server_cert(
        &CertificateDer::from(include_bytes!("fixtures/leaf.der").as_slice()),
        &[],
        &ServerName::try_from("dv-g3b-emudhra.we-have-a-problem.com")?,
        &[],
        UnixTime::since_unix_epoch(Duration::from_secs(1_792_627_200)),
    );
    match result {
        Err(Error::InvalidCertificate(CertificateError::Revoked)) => Ok(()),
        other => Err(format!("unexpected native result: {other:?}").into()),
    }
}

#[test]
fn native_trust_for_an_unblocked_key_is_preserved() -> Result<(), Box<dyn StdError>> {
    let leaf = CertificateDer::from(include_bytes!("fixtures/leaf.der").as_slice());
    let verifier = Verifier::new_with_extra_roots(
        [leaf.clone()],
        Arc::new(rustls::crypto::ring::default_provider()),
    )?;
    verifier.verify_server_cert(
        &leaf,
        &[],
        &ServerName::try_from("dv-g3b-emudhra.we-have-a-problem.com")?,
        &[],
        UnixTime::since_unix_epoch(Duration::from_secs(1_792_627_200)),
    )?;
    Ok(())
}
