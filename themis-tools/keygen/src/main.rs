#![allow(unused)]

use anyhow::bail;
use clap::Parser;
use rcgen::{Certificate, CertificateParams, IsCa};
use ring::{
    rand::SystemRandom,
    signature::{
        EcdsaKeyPair, Ed25519KeyPair, KeyPair, ECDSA_P256_SHA256_ASN1_SIGNING,
        ECDSA_P384_SHA384_ASN1_SIGNING,
    },
};
use std::{
    error::Error,
    fs::{self, write},
    path::{Path, PathBuf},
    str::FromStr,
};

struct KeyNames {
    private: String,
    public: String,
    cert: String,
}

fn key_names(base: &str, kind: &str, number: usize) -> KeyNames {
    let make = |vis| format!("{}-{}-{}-{}", base, kind, vis, number);
    KeyNames {
        private: make("private"),
        public: make("public"),
        cert: make("cert"),
    }
}

#[derive(Debug)]
enum KeyKind {
    Ecdsa,
    Ed25519,
    Rsa,
}

impl FromStr for KeyKind {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ECDSA" => Ok(KeyKind::Ecdsa),
            "Ed25519" => Ok(KeyKind::Ed25519),
            "Rsa" => Ok(KeyKind::Rsa),
            _ => bail!("Algorithm not found."),
        }
    }
}

#[derive(Debug)]
enum EcdsaCurve {
    P256,
    P384,
}

#[derive(Debug)]
enum RsaLength {
    L2048,
    L4096,
    L8192,
}

#[derive(Debug)]
enum RsaHash {
    SHA256,
    SHA384,
    SHA512,
}

fn generate_ed25519(private_key_dest: &Path, public_key_dest: &Path) -> anyhow::Result<()> {
    let random = SystemRandom::new();

    let private_key_bin = Ed25519KeyPair::generate_pkcs8(&random)?;
    let private_key = Ed25519KeyPair::from_pkcs8(private_key_bin.as_ref())?;

    let public_key = private_key.public_key();

    write(private_key_dest, private_key_bin)?;
    write(public_key_dest, public_key)?;

    let _verify: Ed25519KeyPair =
        Ed25519KeyPair::from_pkcs8(&fs::read(private_key_dest).unwrap()).expect("verify");

    Ok(())
}

fn generate_ecdsa(
    private_key_dest: &Path,
    public_key_dest: &Path,
    kind: EcdsaCurve,
) -> anyhow::Result<()> {
    let random = SystemRandom::new();

    let algorithm = match kind {
        EcdsaCurve::P256 => &ECDSA_P256_SHA256_ASN1_SIGNING,
        EcdsaCurve::P384 => &ECDSA_P384_SHA384_ASN1_SIGNING,
    };

    let private_key_bin = EcdsaKeyPair::generate_pkcs8(algorithm, &random)?;
    let private_key = EcdsaKeyPair::from_pkcs8(algorithm, private_key_bin.as_ref())?;

    let public_key = private_key.public_key();

    fs::write(private_key_dest, &private_key_bin)?;
    fs::write(public_key_dest, public_key)?;

    let _verify: EcdsaKeyPair =
        EcdsaKeyPair::from_pkcs8(algorithm, &fs::read(private_key_dest).unwrap()).expect("verify");

    Ok(())
}

fn generate_x509(alg: KeyKind, name: String, private_key_path: &Path, cert_path: &Path) {
    let key_pair = std::fs::read(private_key_path).unwrap();
    let key_pair = rcgen::KeyPair::from_der(&key_pair).unwrap();

    let mut params = CertificateParams::new(vec![name]);
    params.key_pair = Some(key_pair);
    params.is_ca = IsCa::SelfSignedOnly;
    match alg {
        KeyKind::Ecdsa => params.alg = &rcgen::PKCS_ECDSA_P256_SHA256,
        KeyKind::Ed25519 => params.alg = &rcgen::PKCS_ED25519,
        KeyKind::Rsa => params.alg = &rcgen::PKCS_RSA_SHA256,
    }

    let cert = Certificate::from_params(params).unwrap();
    let bytes = cert.serialize_der().unwrap();
    std::fs::write(cert_path, &bytes).unwrap();
}

#[derive(Debug, Parser)]
struct Options {
    #[clap(long)]
    out_dir: PathBuf,
    #[clap(long)]
    certs: bool,
    base: KeyKind,
    start: u64,
    end: u64,
}

fn main() {
    let options = Options::parse();

    if !options.out_dir.is_dir() {
        eprintln!(
            "out-dir '{}' is not a directory. Please create it or pass an existing directory.",
            options.out_dir.display()
        );
        return;
    }

    for i in options.start..options.end {
        match options.base {
            KeyKind::Ecdsa => {
                let KeyNames {
                    private: priv_file,
                    public: pub_file,
                    ..
                } = key_names("ecdsa", "256", i as usize);
                generate_ecdsa(
                    &options.out_dir.join(priv_file),
                    &options.out_dir.join(pub_file),
                    EcdsaCurve::P256,
                )
                .expect("generate ec")
            }
            KeyKind::Ed25519 => {
                let KeyNames {
                    private: priv_file,
                    public: pub_file,
                    cert,
                } = key_names("ed", "25519", i as usize);
                generate_ed25519(
                    &options.out_dir.join(&priv_file),
                    &options.out_dir.join(pub_file),
                )
                .expect("generate ed");

                if options.certs {
                    generate_x509(
                        KeyKind::Ed25519,
                        format!("themis{i}"),
                        &options.out_dir.join(&priv_file),
                        &options.out_dir.join(cert),
                    )
                }
            }
            KeyKind::Rsa => {
                let KeyNames {
                    private: priv_file,
                    public: _pub_file,
                    ..
                } = key_names("rsa", "2048", i as usize);
                println!("RSA keygen is not supported by ring");
                println!(
                    "Run e.g. openssl genpkey -algorithm RSA \
                     -pkeyopt rsa_keygen_bits:2048 \
                     -pkeyopt rsa_keygen_pubexp:65537 | \
                     openssl pkcs8 -topk8 -nocrypt -outform der > {}",
                    priv_file
                )
            }
        }
    }
}
