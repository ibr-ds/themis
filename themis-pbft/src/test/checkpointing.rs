use std::task::Poll;

use bytes::Bytes;
use futures_util::{future::join, poll, StreamExt};
use ring::digest::{digest, SHA256};

use assert_matches::assert_matches;
use themis_core::{
    app::{ApplyCheckpoint, Command, Request},
    net::Message,
    protocol::Proposal,
    utils::PollExt,
};

use crate::{
    messages::{Checkpoint, FullCheckpoint, GetCheckpoint},
    test::{
        client, commit_set,
        regular::{prepare_phase_primary, test_round},
        setup_with_checkpoint, PBFTContext,
    },
    DefaultHasher, ProposalHasher,
};

use super::test_round_leader;

fn make_request(me: u64, seq: u64) -> Proposal {
    let request = Message::new(client(), me, Request::new(seq, Bytes::new()));
    Proposal::Single(request)
}

const OWN_CHECKPOINT: &[u8] = &[1, 1, 1, 1];
const CONFLICT_CHECKPOINT: &[u8] = &[2, 2, 2, 2];

async fn test_mismatch() -> PBFTContext {
    let me = 0;
    let mut ctx = setup_with_checkpoint(true, 10);

    for i in 1..=9 {
        // Some rounds until next checkpoint
        test_round(&mut ctx, me, i, 0, 0).await;
    }

    let request_vec = make_request(me, 10);
    prepare_phase_primary(&mut ctx, me, 10, 0, request_vec.clone()).await;

    commit_with_checkpoint(me, &mut ctx, request_vec).await;

    apply_conflicting_checkpoint(me, &mut ctx).await;
    ctx
}

async fn apply_conflicting_checkpoint(me: u64, ctx: &mut PBFTContext) {
    let conflict_hash = digest(&SHA256, CONFLICT_CHECKPOINT);
    let conflicting = Checkpoint::new(10, Bytes::copy_from_slice(conflict_hash.as_ref()));
    for i in 1..=3 {
        let msg = Message::new(i, me, conflicting.clone()).pack().unwrap();
        ctx.pbft.on_message(msg).await.unwrap();
        assert_eq!(poll!(ctx.c.next()), Poll::Pending);
        assert_eq!(poll!(ctx.a.next()), Poll::Pending);
        if i == 3 {
            // 2f+1 checkpoint messages different from own, should request checkpoint from somewhere
            assert_eq!(
                poll!(ctx.r.next()).map_some(|m| m.unpack().unwrap()),
                Poll::Ready(Some(Message::new(0, 1, GetCheckpoint::new(10))))
            );
        }
        assert_eq!(poll!(ctx.r.next()), Poll::Pending);
    }
}

async fn apply_checkpoint(me: u64, ctx: &mut PBFTContext) {
    let conflict_hash = digest(&SHA256, OWN_CHECKPOINT);
    let conflicting = Checkpoint::new(10, Bytes::copy_from_slice(conflict_hash.as_ref()));
    for i in 1..=2 {
        let msg = Message::new(i, me, conflicting.clone()).pack().unwrap();
        ctx.pbft.on_message(msg).await.unwrap();

        if i == 2 {
            assert_matches!(poll!(ctx.a.next()), Poll::Ready(Some(..)));
        }

        assert_eq!(poll!(ctx.c.next()), Poll::Pending);
        assert_eq!(poll!(ctx.a.next()), Poll::Pending);
        assert_eq!(poll!(ctx.r.next()), Poll::Pending);
    }
}

async fn commit_with_checkpoint(me: u64, ctx: &mut PBFTContext, request_vec: Proposal) {
    let mut commits = commit_set(10, 0, me, DefaultHasher::hash_proposal(&request_vec)).into_iter();
    let commit = commits.next().unwrap();
    ctx.pbft.on_message(commit).await.unwrap();
    assert_eq!(poll!(ctx.r.next()), Poll::Pending);
    assert_eq!(poll!(ctx.c.next()), Poll::Pending);
    assert_eq!(poll!(ctx.a.next()), Poll::Pending);
    let commit = commits.next().unwrap();
    let PBFTContext { pbft, a, .. } = ctx;
    let (pbft_result, ()) = join(pbft.on_message(commit), async {
        // fake an application that returns a checkpoint
        // needs to be concurrent here because pbft waits for the app checkpoint
        assert_eq!(
            a.next().await,
            Some(Command::Request(request_vec.iter().next().unwrap().clone()))
        );

        let app_msg = a.next().await;
        assert_matches!(app_msg, Some(Command::TakeCheckpoint(sender)) => {
            sender.send(Bytes::from(OWN_CHECKPOINT)).unwrap();
        });
    })
    .await;
    pbft_result.unwrap();
    assert_eq!(poll!(ctx.c.next()), Poll::Pending);
    assert_eq!(poll!(ctx.a.next()), Poll::Pending);
    let expected_hash = digest(&SHA256, OWN_CHECKPOINT);
    assert_eq!(
        poll!(ctx.r.next()).map_some(|m| m.unpack().unwrap()),
        Poll::Ready(Some(Message::broadcast(
            me,
            Checkpoint::new(10, Bytes::copy_from_slice(expected_hash.as_ref())),
        )))
    );
    assert_eq!(poll!(ctx.r.next()), Poll::Pending);
}

#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn acquire_checkpoint() {
    let _ = tracing_subscriber::fmt::try_init();
    let me = 0;
    let mut ctx = test_mismatch().await;

    apply_full_checkpoint(me, 10, &mut ctx, 0).await;
    // 11 should pass after checkpoint
    test_round(&mut ctx, me, 11, 0, 0).await;
}

async fn apply_full_checkpoint(me: u64, seq: u64, ctx: &mut PBFTContext, expected_reqs: usize) {
    let full_cp = Message::new(
        1,
        me,
        FullCheckpoint::new(seq, Bytes::from_static(CONFLICT_CHECKPOINT), Bytes::new()),
    );

    let a = &mut ctx.a;
    let app = async {
        assert_matches!(
            poll!(a.next()),
            Poll::Ready(Some(Command::CheckpointStable {
                sequence,
                apply: Some(ApplyCheckpoint {
                    handle,
                    data,
                    tx,
                }),
                ..
            })) => {
                assert_eq!(sequence, seq);
                assert_eq!(handle, Bytes::from_static(CONFLICT_CHECKPOINT));
                assert_eq!(data, Bytes::new());
                tx.send(true).unwrap();
            }
        );
    };

    let results = tokio::join!(ctx.pbft.on_message(full_cp.pack().unwrap()), app);
    results.0.unwrap();

    assert_eq!(poll!(ctx.r.next()), Poll::Pending);
    assert_eq!(poll!(ctx.c.next()), Poll::Pending);

    // Applying a checkpoint may require re-doing some requests
    for _i in 0..expected_reqs {
        assert_matches!(poll!(ctx.a.next()), Poll::Ready(Some(Command::Request(_))))
    }
    assert_eq!(poll!(ctx.a.next()), Poll::Pending);
}

#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn fail_again() {
    let me = 0;
    let mut ctx = test_mismatch().await;

    let full_cp = Message::new(
        1,
        me,
        FullCheckpoint::new(10, Bytes::from_static(&[3, 3, 3, 3]), Bytes::default()),
    );

    ctx.pbft.on_message(full_cp.pack().unwrap()).await.unwrap();

    assert_eq!(
        poll!(ctx.r.next()).map_some(|m| m.unpack().unwrap()),
        Poll::Ready(Some(Message::new(0, 2, GetCheckpoint::new(10))))
    );
    assert_eq!(poll!(ctx.r.next()), Poll::Pending);
    assert_eq!(poll!(ctx.c.next()), Poll::Pending);
    assert_eq!(poll!(ctx.a.next()), Poll::Pending);

    apply_full_checkpoint(me, 10, &mut ctx, 0).await;
    // 11 should pass after checkpoint
    test_round(&mut ctx, me, 11, 0, 0).await;
}

#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn fast_forward() {
    let _ = tracing_subscriber::fmt::try_init();

    let me = 0;
    let mut ctx = setup_with_checkpoint(true, 10);

    // fewer rounds
    for i in 1..=6 {
        // Some rounds until next checkpoint
        test_round_leader(&mut ctx, me, i, 0, 0).await;
    }

    apply_conflicting_checkpoint(me, &mut ctx).await;

    let full_cp = Message::new(
        1,
        me,
        FullCheckpoint::new(10, Bytes::from_static(CONFLICT_CHECKPOINT), Bytes::new()),
    );

    let a = &mut ctx.a;
    let app = async {
        assert_matches!(
            poll!(a.next()),
            Poll::Ready(Some(Command::CheckpointStable {
                apply: Some(ApplyCheckpoint {
                    handle,
                    data,
                    tx,
                }),
                ..
            })) => {
                assert_eq!(handle, Bytes::from_static(CONFLICT_CHECKPOINT));
                assert_eq!(data, Bytes::new());
                tx.send(true).unwrap();
            }
        );
    };

    let results = tokio::join!(ctx.pbft.on_message(full_cp.pack().unwrap()), app);
    results.0.unwrap();

    assert_eq!(poll!(ctx.r.next()), Poll::Pending);
    assert_eq!(poll!(ctx.c.next()), Poll::Pending);
    assert_eq!(poll!(ctx.a.next()), Poll::Pending);

    // 11 should pass after checkpoint
    test_round_leader(&mut ctx, me, 11, 0, 0).await;
}

#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn checkpoint_later() {
    let _ = tracing_subscriber::fmt::try_init();

    let me = 0;
    let mut ctx = setup_with_checkpoint(true, 10);

    for i in 1..=9 {
        // Some rounds until next checkpoint
        test_round(&mut ctx, me, i, 0, 0).await;
    }

    let request_vec = make_request(me, 10);
    prepare_phase_primary(&mut ctx, me, 10, 0, request_vec.clone()).await;
    commit_with_checkpoint(me, &mut ctx, request_vec).await;

    for i in 11..=13 {
        test_round(&mut ctx, me, i, 0, 0).await;
    }

    apply_checkpoint(me, &mut ctx).await;

    for i in 14..=15 {
        test_round(&mut ctx, me, i, 0, 0).await;
    }
}

#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn rollback() {
    let _ = tracing_subscriber::fmt::try_init();

    let me = 0;
    let mut ctx = setup_with_checkpoint(true, 10);

    for i in 1..=9 {
        // Some rounds until next checkpoint
        test_round(&mut ctx, me, i, 0, 0).await;
    }

    let request_vec = make_request(me, 10);
    prepare_phase_primary(&mut ctx, me, 10, 0, request_vec.clone()).await;
    commit_with_checkpoint(me, &mut ctx, request_vec).await;

    for i in 11..=13 {
        test_round(&mut ctx, me, i, 0, 0).await;
    }

    apply_conflicting_checkpoint(me, &mut ctx).await;
    apply_full_checkpoint(me, 10, &mut ctx, 3).await;

    for i in 14..=15 {
        test_round(&mut ctx, me, i, 0, 0).await;
    }
}

#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn get_checkpoint() {
    let _ = tracing_subscriber::fmt::try_init();

    let me = 0;
    let mut ctx = setup_with_checkpoint(true, 10);

    for i in 1..=9 {
        // Some rounds until next checkpoint
        test_round(&mut ctx, me, i, 0, 0).await;
    }
    let request_vec = make_request(me, 10);
    prepare_phase_primary(&mut ctx, me, 10, 0, request_vec.clone()).await;
    commit_with_checkpoint(me, &mut ctx, request_vec).await;

    for i in 2..4 {
        let expected_hash = digest(&SHA256, OWN_CHECKPOINT);
        let checkpoint = Message::new(
            i,
            me,
            Checkpoint::new(10, Bytes::from(expected_hash.as_ref().to_owned())),
        );
        ctx.pbft
            .on_message(checkpoint.pack().unwrap())
            .await
            .unwrap();
    }

    let message = GetCheckpoint::new(10);
    let message = Message::new(1, me, message).pack().unwrap();

    let a = &mut ctx.a;

    assert_matches!(
        poll!(a.next()),
        Poll::Ready(Some(Command::CheckpointStable { apply: None, .. }))
    );

    let resolve_task = async {
        let cmd = a.next().await.unwrap();
        match cmd {
            Command::ResolveCheckpoint {
                handle: _handle,
                tx,
            } => tx.send("resolved".into()).unwrap(),
            _ => panic!("expected resolve command: {:?}", cmd),
        }
    };
    let process_task = ctx.pbft.on_message(message);

    let (p, _) = tokio::join!(process_task, resolve_task);
    p.unwrap();

    let next = ctx.r.next().await.unwrap();
    let next: Message<FullCheckpoint> = next.unpack().unwrap();
    assert!(!next.inner.data.is_empty() && !next.inner.handle.is_empty());
}
