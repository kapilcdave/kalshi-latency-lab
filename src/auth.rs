use std::{env, fs};

use anyhow::{Context, Result, bail};
use base64::{Engine, engine::general_purpose::STANDARD};
use rsa::{
    RsaPrivateKey,
    pkcs1::DecodeRsaPrivateKey,
    pkcs8::DecodePrivateKey,
    pss::BlindedSigningKey,
    rand_core::OsRng,
    signature::{RandomizedSigner, SignatureEncoding},
};
use sha2::Sha256;

pub struct Auth {
    key_id: String,
    signing_key: BlindedSigningKey<Sha256>,
}

pub struct Headers {
    pub key_id: String,
    pub timestamp: String,
    pub signature: String,
}

impl Auth {
    pub fn from_env() -> Result<Self> {
        let key_id = env::var("KALSHI_API_KEY_ID").context("KALSHI_API_KEY_ID is not set")?;
        let path =
            env::var("KALSHI_PRIVATE_KEY_PATH").context("KALSHI_PRIVATE_KEY_PATH is not set")?;
        let pem = fs::read_to_string(&path)
            .with_context(|| format!("failed to read private key at {path}"))?;
        let private_key = parse_private_key(&pem)?;
        Ok(Self {
            key_id,
            signing_key: BlindedSigningKey::new_with_salt_len(private_key, 32),
        })
    }

    pub fn websocket_headers(&self) -> Result<Headers> {
        self.headers("GET", "/trade-api/ws/v2")
    }

    fn headers(&self, method: &str, path: &str) -> Result<Headers> {
        let timestamp = unix_ms()?.to_string();
        let clean_path = path.split('?').next().unwrap_or(path);
        let message = format!("{timestamp}{}{clean_path}", method.to_ascii_uppercase());
        let signature = self
            .signing_key
            .sign_with_rng(&mut OsRng, message.as_bytes());
        Ok(Headers {
            key_id: self.key_id.clone(),
            timestamp,
            signature: STANDARD.encode(signature.to_bytes()),
        })
    }
}

fn parse_private_key(pem: &str) -> Result<RsaPrivateKey> {
    if let Ok(key) = RsaPrivateKey::from_pkcs8_pem(pem) {
        return Ok(key);
    }
    if let Ok(key) = RsaPrivateKey::from_pkcs1_pem(pem) {
        return Ok(key);
    }
    bail!("private key is not a valid PKCS#8 or PKCS#1 RSA PEM")
}

fn unix_ms() -> Result<u128> {
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("system clock is before Unix epoch")?
        .as_millis())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsa::{
        RsaPublicKey,
        pss::{Signature, VerifyingKey},
        signature::Verifier,
    };

    #[test]
    fn signs_documented_websocket_message() {
        let private = RsaPrivateKey::new(&mut OsRng, 2048).unwrap();
        let public = RsaPublicKey::from(&private);
        let auth = Auth {
            key_id: "test".into(),
            signing_key: BlindedSigningKey::new_with_salt_len(private, 32),
        };
        let timestamp = "1787345120337";
        let message = format!("{timestamp}GET/trade-api/ws/v2");
        let signature = auth
            .signing_key
            .sign_with_rng(&mut OsRng, message.as_bytes());
        let verifier = VerifyingKey::<Sha256>::new_with_salt_len(public, 32);
        let encoded = STANDARD.encode(signature.to_bytes());
        let decoded = STANDARD.decode(encoded).unwrap();
        verifier
            .verify(
                message.as_bytes(),
                &Signature::try_from(decoded.as_slice()).unwrap(),
            )
            .unwrap();
    }

    #[test]
    fn rejects_non_rsa_pem() {
        assert!(parse_private_key("not a key").is_err());
    }
}
