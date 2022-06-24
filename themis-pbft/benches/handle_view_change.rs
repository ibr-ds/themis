use bytes::Bytes;
use config::Config;
use criterion::*;
use fnv::FnvHashMap;
use ring::{
    digest::{digest, SHA256},
    rand::SystemRandom,
    signature::{Ed25519KeyPair, KeyPair},
};
use themis_core::{
    app::{self, Client, Request},
    authentication::{
        ed25519::{self, Ed25519Auth},
        Authenticator, AuthenticatorExt,
    },
    comms::{unbounded, Receiver},
    config,
    net::{Message, RawMessage},
    protocol::Proposal,
};
use themis_pbft::{
    config::PBFTConfig,
    messages::{self, Checkpoint, PBFTMessage, PrePrepare, Prepare, PrepareProof, ViewChange},
    DefaultHasher, ProposalHasher, PBFT,
};
use tokio::runtime::Runtime;

fn prepare_set(
    primary: u64,
    sequence: u64,
    view: u64,
    digest: Bytes,
    signer: &mut dyn Authenticator<messages::PBFT>,
) -> PrepareProof {
    let mut msgs = Vec::with_capacity(4);

    let mut pre_prepare =
        Message::broadcast(primary, PrePrepare::new(sequence, view, digest.clone()));
    signer.sign_unpacked(&mut pre_prepare).unwrap();

    for i in (0..4).filter(|&i| i != primary) {
        let mut message = Message::broadcast(i, Prepare::new(sequence, view, digest.clone()));
        signer.sign_unpacked(&mut message).unwrap();
        msgs.push(message);
    }

    PrepareProof(pre_prepare, msgs.into_boxed_slice())
}

fn create_checkpoint_set(
    seq: u64,
    digest: Bytes,
    signer: &mut dyn Authenticator<messages::PBFT>,
) -> Vec<Message<Checkpoint>> {
    let mut checkpoints = Vec::new();
    for i in 0..3 {
        let mut checkpoint = Message::broadcast(i, Checkpoint::new(seq, digest.clone()));
        signer.sign_unpacked(&mut checkpoint).unwrap();
        checkpoints.push(checkpoint);
    }

    checkpoints
}

fn create_view_change(
    from: u64,
    prepared_instances: u64,
    signer: &mut dyn Authenticator<messages::PBFT>,
) -> Message<ViewChange> {
    let mut prepare_sets = Vec::new();
    for i in 1..=prepared_instances {
        let request = Message::broadcast(100, Request::new(i, format!("{}", i).into()));
        let digest = DefaultHasher::hash_proposal(&Proposal::Single(request.clone()));
        let set = prepare_set(0, i, 0, digest.clone(), signer);
        prepare_sets.push(set);
    }

    let digest = digest(&SHA256, b"checkpoint");
    let checkpoints = create_checkpoint_set(0, Bytes::copy_from_slice(digest.as_ref()), signer);

    Message::broadcast(
        from,
        ViewChange::new(
            1,
            0,
            checkpoints.into_boxed_slice(),
            prepare_sets.into_boxed_slice(),
        ),
    )
}

fn do_view_change(runtime: &mut Runtime, mut pbft: PBFT, set: &[PBFTMessage]) {
    runtime.block_on(async move {
        for view_change in set {
            pbft.on_message(view_change.clone()).await.unwrap();
        }

        assert_eq!(pbft.view(), 1);
    })
}

fn setup_keys() -> Ed25519Auth {
    let mut public_keys = FnvHashMap::default();
    let random = SystemRandom::new();

    let my_key = Ed25519KeyPair::generate_pkcs8(&random).unwrap();
    let my_public = Ed25519KeyPair::from_pkcs8(my_key.as_ref()).unwrap();
    for i in &[0, 2, 3] {
        let keypair = Ed25519KeyPair::generate_pkcs8(&random).unwrap();
        let keypair = Ed25519KeyPair::from_pkcs8(keypair.as_ref()).unwrap();

        public_keys.insert(*i, Bytes::copy_from_slice(keypair.public_key().as_ref()));
    }
    public_keys.insert(1, Bytes::copy_from_slice(my_public.public_key().as_ref()));
    ed25519::set_keys(public_keys);

    let auth = Ed25519Auth::from_bytes(my_key.as_ref()).unwrap();

    auth
}
#[derive(Debug)]
pub struct PBFTContext {
    pbft: PBFT,
    _r: Receiver<PBFTMessage>,
    _c: Receiver<RawMessage<Client>>,
    _a: Receiver<app::Command>,
}

impl PBFTContext {
    pub fn new(
        pbft: PBFT,
        r: Receiver<PBFTMessage>,
        c: Receiver<RawMessage<Client>>,
        a: Receiver<app::Command>,
    ) -> Self {
        Self {
            pbft,
            _r: r,
            _c: c,
            _a: a,
        }
    }
}

fn add_pthemis_config(config: &mut Config) {
    let pbftc = PBFTConfig::default();
    config.set("pbft", pbftc).expect("set pbft");
}

pub fn setup_pbft_backup(
    auth: Box<dyn Authenticator<messages::PBFT> + Send + Sync>,
) -> PBFTContext {
    let mut config = config::default();
    config.set("id", 1).unwrap();
    add_pthemis_config(&mut config);

    let r = unbounded();
    let c = unbounded();
    let a = unbounded();
    let pbft = PBFT::new(1, r.0, c.0, a.0, auth, &config);
    PBFTContext::new(pbft, r.1, c.1, a.1)
}

fn bench_handle_view_change(c: &mut Criterion) {
    let _ = tracing_subscriber::fmt::try_init();

    let mut runtime = Runtime::new().unwrap();
    let auth = setup_keys();

    let mut signer = auth.clone();

    let mut group = c.benchmark_group("view_change");
    for prepares in [1, 8, 16, 32, 48, 64].iter().cloned() {
        let vcs = [2, 3]
            .iter()
            .cloned()
            .map(|source| {
                create_view_change(source, prepares, &mut signer)
                    .pack()
                    .unwrap()
            })
            .collect::<Vec<_>>();
        group.bench_with_input(format!("{}-prepares", prepares), &vcs, |b, vcs| {
            b.iter(|| {
                let pbft = setup_pbft_backup(Box::new(auth.clone()));
                do_view_change(&mut runtime, pbft.pbft, vcs);
            });
        });
    }
}

criterion_group!(handle_view_change, bench_handle_view_change);
criterion_main!(handle_view_change);
