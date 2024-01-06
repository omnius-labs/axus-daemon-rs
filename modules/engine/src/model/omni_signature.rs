use ed25519_dalek::Signer;
use rand_core::OsRng;

#[derive(Debug, Clone)]
pub enum OmniSignType {
    Ed25519,
}

#[derive(Debug, Clone)]
pub struct OmniSigner {
    typ: OmniSignType,
    key: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct OmniSignature {
    typ: OmniSignType,
    public_key: Vec<u8>,
    value: Vec<u8>,
}

impl OmniSigner {
    pub fn new(typ: OmniSignType) -> Self {
        match typ {
            OmniSignType::Ed25519 => {
                let signing_key = ed25519_dalek::SigningKey::generate(&mut OsRng);

                let key = signing_key.to_keypair_bytes().to_vec();
                Self { typ, key }
            }
        }
    }

    pub fn sign(&self, msg: &[u8]) -> anyhow::Result<OmniSignature> {
        match self.typ {
            OmniSignType::Ed25519 => {
                let signing_key_bytes = self.key.as_slice();
                if signing_key_bytes.len() != ed25519_dalek::KEYPAIR_LENGTH {
                    anyhow::bail!("Invalid signing_key length");
                }
                let signing_key_bytes = <&[u8; ed25519_dalek::KEYPAIR_LENGTH]>::try_from(signing_key_bytes)?;

                let signing_key = ed25519_dalek::SigningKey::from_keypair_bytes(signing_key_bytes)?;

                let typ = self.typ.clone();
                let public_key = signing_key.verifying_key().to_bytes().to_vec();
                let value = signing_key.sign(msg).to_vec();
                Ok(OmniSignature { typ, public_key, value })
            }
        }
    }
}

impl OmniSignature {
    pub fn verify(&self, msg: &[u8]) -> anyhow::Result<()> {
        match self.typ {
            OmniSignType::Ed25519 => {
                let verifying_key_bytes = self.public_key.as_slice();
                if verifying_key_bytes.len() != ed25519_dalek::PUBLIC_KEY_LENGTH {
                    anyhow::bail!("Invalid verifying_key length");
                }
                let verifying_key_bytes = <&[u8; ed25519_dalek::PUBLIC_KEY_LENGTH]>::try_from(verifying_key_bytes)?;

                let signature_bytes = self.value.as_slice();
                if signature_bytes.len() != ed25519_dalek::SIGNATURE_LENGTH {
                    return Err(anyhow::anyhow!("Invalid signature length"));
                }
                let signature_bytes = <&[u8; ed25519_dalek::SIGNATURE_LENGTH]>::try_from(signature_bytes)?;

                let verifying_key = ed25519_dalek::VerifyingKey::from_bytes(verifying_key_bytes)?;
                let signature = ed25519_dalek::Signature::from_bytes(signature_bytes);
                Ok(verifying_key.verify_strict(msg, &signature)?)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{OmniSignType, OmniSigner};

    #[tokio::test]
    #[ignore]
    async fn simple_test() {
        let signer = OmniSigner::new(OmniSignType::Ed25519);
        let signature = signer.sign(b"test").unwrap();
        assert!(signature.verify(b"test").is_ok());
        assert!(signature.verify(b"test_err").is_err());
    }
}
