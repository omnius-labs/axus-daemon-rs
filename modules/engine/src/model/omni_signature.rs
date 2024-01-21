use std::fmt;

use base64::{engine::general_purpose::URL_SAFE_NO_PAD as BASE64, Engine};
use ed25519_dalek::Signer;
use rand_core::OsRng;
use serde::{Deserialize, Serialize};
use sha3::{Digest, Sha3_256};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OmniSignType {
    Ed25519,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OmniSigner {
    typ: OmniSignType,
    name: String,
    key: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OmniSignature {
    typ: OmniSignType,
    name: String,
    public_key: Vec<u8>,
    value: Vec<u8>,
}

impl OmniSigner {
    pub fn new(typ: &OmniSignType, name: &str) -> Self {
        match typ {
            OmniSignType::Ed25519 => {
                let signing_key = ed25519_dalek::SigningKey::generate(&mut OsRng);

                let typ = typ.clone();
                let name = name.to_string();
                let key = signing_key.to_keypair_bytes().to_vec();
                Self { typ, name, key }
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
                let name = self.name.clone();
                let public_key = signing_key.verifying_key().to_bytes().to_vec();
                let value = signing_key.sign(msg).to_vec();
                Ok(OmniSignature {
                    typ,
                    name,
                    public_key,
                    value,
                })
            }
        }
    }
}

impl fmt::Display for OmniSigner {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.typ {
            OmniSignType::Ed25519 => {
                let signing_key_bytes: [u8; ed25519_dalek::KEYPAIR_LENGTH] = self.key.clone().try_into().map_err(|_| fmt::Error)?;

                let signing_key = ed25519_dalek::SigningKey::from_keypair_bytes(&signing_key_bytes).map_err(|_| fmt::Error)?;
                let public_key = signing_key.verifying_key().to_bytes();

                let mut hasher = Sha3_256::new();
                hasher.update(public_key);
                let hash = hasher.finalize();

                write!(f, "{}@{}", self.name, BASE64.encode(hash))
            }
        }
    }
}

impl OmniSignature {
    pub fn verify(&self, msg: &[u8]) -> anyhow::Result<()> {
        match self.typ {
            OmniSignType::Ed25519 => {
                let verifying_key_bytes: [u8; ed25519_dalek::PUBLIC_KEY_LENGTH] = self
                    .public_key
                    .clone()
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("Invalid verifying_key length"))?;
                let signature_bytes: [u8; ed25519_dalek::SIGNATURE_LENGTH] =
                    self.value.clone().try_into().map_err(|_| anyhow::anyhow!("Invalid signature length"))?;

                let verifying_key = ed25519_dalek::VerifyingKey::from_bytes(&verifying_key_bytes)?;
                let signature = ed25519_dalek::Signature::from_bytes(&signature_bytes);
                Ok(verifying_key.verify_strict(msg, &signature)?)
            }
        }
    }
}

impl fmt::Display for OmniSignature {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.typ {
            OmniSignType::Ed25519 => {
                let mut hasher = Sha3_256::new();
                hasher.update(&self.public_key);
                let hash = hasher.finalize();

                write!(f, "{}@{}", self.name, BASE64.encode(hash))
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
        let signer = OmniSigner::new(&OmniSignType::Ed25519, "test_user");
        let signature = signer.sign(b"test").unwrap();

        println!("{}", signer);
        println!("{}", signature);

        assert!(signature.verify(b"test").is_ok());
        assert!(signature.verify(b"test_err").is_err());
    }
}
