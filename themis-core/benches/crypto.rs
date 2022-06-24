use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ring::{
    hmac,
    rand::{SecureRandom, SystemRandom},
    signature::*,
};
use themis_core::{app::Request, net::Message};

fn verify_signature(pubkey: &UnparsedPublicKey<Vec<u8>>, message: &[u8], signature: &[u8]) {
    pubkey.verify(message, signature).unwrap();
}

fn generate_ecdsa_key(random: &dyn SecureRandom) -> (EcdsaKeyPair, UnparsedPublicKey<Vec<u8>>) {
    let ecdsa_key = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, random).unwrap();
    let ecdsa_key =
        EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, ecdsa_key.as_ref()).unwrap();
    let ecdsa_pubkey: Vec<u8> = ecdsa_key.public_key().as_ref().into();
    let ecdsa_pubkey = UnparsedPublicKey::new(&ECDSA_P256_SHA256_FIXED, ecdsa_pubkey);
    (ecdsa_key, ecdsa_pubkey)
}

fn generate_ed25519_key(random: &dyn SecureRandom) -> (Ed25519KeyPair, UnparsedPublicKey<Vec<u8>>) {
    let ed25519_key = Ed25519KeyPair::generate_pkcs8(random).unwrap();
    let ed25519_key = Ed25519KeyPair::from_pkcs8(ed25519_key.as_ref()).unwrap();
    let ed25519_pubkey: Vec<u8> = ed25519_key.public_key().as_ref().into();
    let ed25519_pubkey = UnparsedPublicKey::new(&ED25519, ed25519_pubkey);
    (ed25519_key, ed25519_pubkey)
}

fn load_rsa_4096() -> (RsaKeyPair, UnparsedPublicKey<Vec<u8>>) {
    let data = include_bytes!("rsa-4096-private.pk8");
    let rsa_key = RsaKeyPair::from_pkcs8(data).unwrap();
    let public_key = rsa_key.public_key().as_ref().into();
    let public_key = UnparsedPublicKey::new(&RSA_PKCS1_2048_8192_SHA256, public_key);

    (rsa_key, public_key)
}

fn load_rsa_2048() -> (RsaKeyPair, UnparsedPublicKey<Vec<u8>>) {
    let data = include_bytes!("rsa-2048-private.pk8");
    let rsa_key = RsaKeyPair::from_pkcs8(data).unwrap();
    let public_key = rsa_key.public_key().as_ref().into();
    let public_key = UnparsedPublicKey::new(&RSA_PKCS1_2048_8192_SHA256, public_key);

    (rsa_key, public_key)
}

fn test_message() -> Vec<u8> {
    let message = Message::new(1, 2, Request::new(3, vec![5; 1000].into()));
    let message = message.pack().unwrap();

    rmp_serde::to_vec(&message).unwrap()
}

fn verify_benchmarks(cr: &mut Criterion) {
    let message = test_message();

    let random = SystemRandom::new();

    let (ecdsa_key, ecdsa_pubkey) = generate_ecdsa_key(&random);
    let ecdsa_signature = ecdsa_key.sign(&random, &message).unwrap();

    let (rsa_key4, rsa_pubkey4) = load_rsa_4096();
    let (rsa_key2, rsa_pubkey2) = load_rsa_2048();

    let symmetric_key = hmac::Key::generate(hmac::HMAC_SHA256, &random).unwrap();
    let hmac_signature = hmac::sign(&symmetric_key, &message);

    cr.bench_function("verify hmac sha 256", |b| {
        b.iter(|| {
            hmac::verify(
                &symmetric_key,
                black_box(&message),
                black_box(hmac_signature.as_ref()),
            )
            .unwrap();
        })
    });

    let mut c = cr.benchmark_group("verify rsa");
    let mut rsa_signature2 = vec![0; rsa_key2.public_modulus_len()];
    rsa_key2
        .sign(&RSA_PKCS1_SHA256, &random, &message, &mut rsa_signature2)
        .unwrap();

    c.bench_function("2048", |b| {
        b.iter(|| {
            rsa_pubkey2
                .verify(black_box(&message), black_box(&rsa_signature2))
                .unwrap()
        })
    });

    let mut rsa_signature4 = vec![0; rsa_key4.public_modulus_len()];
    rsa_key4
        .sign(&RSA_PKCS1_SHA256, &random, &message, &mut rsa_signature4)
        .unwrap();

    c.bench_function("4096", |b| {
        b.iter(|| {
            rsa_pubkey4
                .verify(black_box(&message), black_box(&rsa_signature4))
                .unwrap()
        })
    });
    drop(c);

    let mut c = cr.benchmark_group("verify ec");

    c.bench_function("ecdsa 256", |b| {
        b.iter(|| {
            verify_signature(
                black_box(&ecdsa_pubkey),
                black_box(&message),
                black_box(ecdsa_signature.as_ref()),
            )
        })
    });

    let (ed25519_key, ed25519_pubkey) = generate_ed25519_key(&random);
    let ed25519_signature = ed25519_key.sign(&message);

    c.bench_function("ed25519", |b| {
        b.iter(|| {
            verify_signature(
                black_box(&ed25519_pubkey),
                black_box(&message),
                black_box(ed25519_signature.as_ref()),
            )
        })
    });
}

fn sign_benchmarks(cr: &mut Criterion) {
    let message = test_message();

    let random = SystemRandom::new();

    let (ecdsa_key, _ecdsa_pubkey) = generate_ecdsa_key(&random);
    let (ed25519_key, _ed25519_pubkey) = generate_ed25519_key(&random);

    let (rsa_key4, _) = load_rsa_4096();
    let (rsa_key2, _) = load_rsa_2048();

    let mut rsa_signature2 = vec![0; rsa_key2.public_modulus_len()];
    let mut rsa_signature4 = vec![0; rsa_key4.public_modulus_len()];

    let symmetric_key = hmac::Key::generate(hmac::HMAC_SHA256, &random).unwrap();

    cr.bench_function("sign hmac sha 256", |b| {
        b.iter(|| {
            hmac::sign(&symmetric_key, black_box(&message));
        })
    });

    let mut c = cr.benchmark_group("sign rsa");
    c.bench_function("2048", |b| {
        b.iter(|| {
            rsa_key2
                .sign(
                    &RSA_PKCS1_SHA256,
                    &random,
                    black_box(&message),
                    black_box(&mut rsa_signature2),
                )
                .unwrap();
        })
    });
    c.bench_function("4096", |b| {
        b.iter(|| {
            rsa_key4
                .sign(
                    &RSA_PKCS1_SHA256,
                    &random,
                    black_box(&message),
                    black_box(&mut rsa_signature4),
                )
                .unwrap();
        })
    });

    drop(c);
    let mut c = cr.benchmark_group("sign ec");

    c.bench_function("ecdsa 256", |b| {
        b.iter(|| {
            ecdsa_key.sign(&random, black_box(&message)).unwrap();
        })
    });

    c.bench_function("ed25519", |b| {
        b.iter(|| {
            ed25519_key.sign(black_box(&message));
        })
    });
}

criterion_group!(verify, verify_benchmarks);
criterion_group!(sign, sign_benchmarks);
criterion_main!(verify, sign);
