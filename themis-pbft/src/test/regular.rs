use std::{sync::Arc, task::Poll};

use bytes::Bytes;
use futures_util::{poll, FutureExt, StreamExt};
use ring::digest::{digest, SHA256};

use assert_matches::assert_matches;
use themis_core::{
    app::{self, Request},
    net::Message,
    protocol::{Proposal, Protocol2},
};

use crate::{
    messages::{Assign, Checkpoint, Commit, Forward, GetProposal, PrePrepare, Prepare},
    DefaultHasher, ProposalHasher,
};

use super::{
    client, commit_set, prepare_set, setup_pbft, setup_pbft_backup, setup_with_checkpoint,
    PBFTContext,
};
use app::Command;

pub(super) async fn send_single_request(pbft: &mut PBFTContext, request: Proposal) {
    Protocol2::on_request(&mut pbft.pbft, request)
        .await
        .unwrap();
}

pub(super) async fn test_round(
    pbft: &mut PBFTContext,
    me: u64,
    sequence: u64,
    view: u64,
    checkpoint: u64,
) {
    if me == 0 {
        test_round_leader(pbft, 0, sequence, view, checkpoint).await
    } else {
        test_round_backup(pbft, me, sequence, view, checkpoint).await
    }
}

pub(super) async fn prepare_phase_primary(
    pbft: &mut PBFTContext,
    me: u64,
    sequence: u64,
    view: u64,
    requests: Proposal,
) {
    let hash = DefaultHasher::hash_proposal(&requests);
    let pre_prepare = Message::broadcast(me, PrePrepare::new(sequence, view, hash.clone()));
    let prepares = (1..4).map(|i| Message::new(i, me, Prepare::new(sequence, 0, hash.clone())));

    send_single_request(pbft, requests.clone()).await;
    assert_eq!(
        poll!(pbft.r.next()),
        Poll::Ready(Some(
            Message::broadcast(
                pbft.pbft.id,
                Assign {
                    sequence,
                    batch: requests,
                }
            )
            .pack()
            .unwrap()
        ))
    );
    let pre_p = poll!(pbft.r.next()).map(|o| o.map(|p| p.unpack().unwrap()));
    assert_eq!(pre_p, Poll::Ready(Some(pre_prepare)));
    assert_eq!(poll!(pbft.c.next()), Poll::Pending);
    assert_eq!(poll!(pbft.a.next()), Poll::Pending);

    for (i, prepare) in prepares.into_iter().enumerate() {
        Protocol2::on_message(&mut pbft.pbft, prepare.pack().unwrap())
            .await
            .unwrap();

        if i == 1 {
            assert_eq!(
                poll!(pbft.r.next()),
                Poll::Ready(Some(
                    Message::broadcast(me, Commit::new(sequence, view, hash.clone()))
                        .pack()
                        .unwrap()
                ))
            );
            assert_eq!(poll!(pbft.c.next()), Poll::Pending);
            assert_eq!(poll!(pbft.a.next()), Poll::Pending);
        } else {
            assert_eq!(poll!(pbft.r.next()), Poll::Pending);
            assert_eq!(poll!(pbft.c.next()), Poll::Pending);
            assert_eq!(poll!(pbft.a.next()), Poll::Pending);
        }
    }
}

pub async fn test_round_leader(
    pbft: &mut PBFTContext,
    me: u64,
    sequence: u64,
    view: u64,
    checkpoint: u64,
) {
    let request = Message::new(client(), me, Request::new(sequence, Bytes::new()));
    let request_vec = Proposal::Batch(vec![request.clone()]);
    prepare_phase_primary(pbft, me, sequence, view, request_vec.clone()).await;
    commit_phase(pbft, me, sequence, view, checkpoint, request_vec).await;
}

async fn test_round_backup(
    pbft: &mut PBFTContext,
    me: u64,
    sequence: u64,
    view: u64,
    checkpoint: u64,
) {
    let leader = pbft.pbft.primary();
    let request = Message::new(client(), me, Request::new(sequence, Bytes::new()));
    let proposal = Proposal::Single(request.clone());
    let hash = DefaultHasher::hash_proposal(&proposal);
    let pre_prepare = Message::new(leader, me, PrePrepare::new(sequence, view, hash.clone()));
    let prepares = (2..4).map(|i| Message::new(i, me, Prepare::new(sequence, view, hash.clone())));

    // request -> forward
    send_single_request(pbft, proposal.clone()).await;

    assert_eq!(
        pbft.r.next().await,
        Some(
            Message::new(me, leader, Forward(proposal.clone()))
                .pack()
                .unwrap()
        )
    );
    assert_eq!(poll!(pbft.c.next()), Poll::Pending);
    assert_eq!(poll!(pbft.a.next()), Poll::Pending);

    // pre-prepare -> send prepare message
    Protocol2::on_message(&mut pbft.pbft, pre_prepare.pack().unwrap())
        .await
        .unwrap();

    // forward => store request, no output
    Protocol2::on_message(
        &mut pbft.pbft,
        Message::broadcast(
            leader,
            Assign {
                sequence,
                batch: proposal.clone(),
            },
        )
        .pack()
        .unwrap(),
    )
    .await
    .unwrap();

    assert_eq!(
        pbft.r.next().await,
        Some(
            Message::broadcast(me, Prepare::new(sequence, view, hash.clone()))
                .pack()
                .unwrap()
        )
    );
    assert_eq!(poll!(pbft.c.next()), Poll::Pending);
    assert_eq!(poll!(pbft.a.next()), Poll::Pending);

    for (i, prepare) in prepares.into_iter().enumerate() {
        Protocol2::on_message(&mut pbft.pbft, prepare.pack().unwrap())
            .await
            .unwrap();

        if i == 0 {
            assert_eq!(
                pbft.r.next().await,
                Some(
                    Message::broadcast(me, Commit::new(sequence, view, hash.clone()))
                        .pack()
                        .unwrap()
                )
            );
            assert_eq!(poll!(pbft.c.next()), Poll::Pending);
            assert_eq!(poll!(pbft.a.next()), Poll::Pending);
        } else {
            assert_eq!(poll!(pbft.r.next()), Poll::Pending);
            assert_eq!(poll!(pbft.c.next()), Poll::Pending);
            assert_eq!(poll!(pbft.a.next()), Poll::Pending);
        }
    }

    commit_phase(pbft, me, sequence, view, checkpoint, proposal).await;
}

async fn commit_phase<'a>(
    pbft: &mut PBFTContext,
    me: u64,
    sequence: u64,
    view: u64,
    checkpoint: u64,
    request: Proposal,
) {
    let hash = DefaultHasher::hash_proposal(&request);

    let commits = (0..3)
        .map(|i| Message::new(i, me, Commit::new(sequence, view, hash.clone())))
        .filter(|c| c.source != me);

    for (i, commit) in commits.enumerate() {
        let PBFTContext { pbft, a, r, c } = pbft;
        if i == 1 {
            let get = async {
                let execute = a.next().await;
                assert_eq!(
                    execute,
                    Some(app::Command::Request(
                        request.iter().next().unwrap().clone()
                    ))
                );
                if checkpoint > 0 {
                    let get_cp = a.next().await;
                    if let Some(app::Command::TakeCheckpoint(sender)) = get_cp {
                        sender.send(Bytes::new()).unwrap();
                    }
                }
            };
            // awaiting checkpoint would block here, so poll once for maximum progress
            let f = Protocol2::on_message(pbft, commit.pack().unwrap());

            let (result, _unit) = tokio::join!(f, get);
            result.unwrap();

            if checkpoint > 0 {
                let digest = digest(&SHA256, &Bytes::new());
                let digest = Bytes::copy_from_slice(digest.as_ref());
                assert_eq!(
                    r.next().await,
                    Some(
                        Message::broadcast(me, Checkpoint::new(checkpoint, digest.clone()))
                            .pack()
                            .unwrap()
                    )
                );
                for i in (0..4).filter(|i| *i != me).take(2) {
                    let cp = Message::new(i, me, Checkpoint::new(checkpoint, digest.clone()));
                    let poll = poll!(Protocol2::on_message(pbft, cp.pack().unwrap()).boxed());
                    assert_matches!(poll, Poll::Ready(Ok(_)));
                }

                assert_matches!(poll!(a.next()), Poll::Ready(Some(Command::CheckpointStable { sequence, apply: None, ..})) if sequence == checkpoint);
                //this should not try to apply checkpoint
                assert_eq!(poll!(a.next()), Poll::Pending);
            }
        } else {
            Protocol2::on_message(pbft, commit.pack().unwrap())
                .await
                .unwrap();
            assert_eq!(poll!(r.next()), Poll::Pending);
            assert_eq!(poll!(c.next()), Poll::Pending);
            assert_eq!(poll!(a.next()), Poll::Pending);
        }
    }
}

#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn test_single_round() {
    let _ = tracing_subscriber::fmt::try_init();

    let mut pbft = setup_pbft();
    test_round(&mut pbft, 0, 1, 0, 0).await
}

#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn test_many_rounds() {
    let mut pbft = setup_pbft();
    for seq in 1..5 {
        test_round(&mut pbft, 0, seq, 0, 0).await
    }
}

#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn test_checkpoint1() {
    let _ = tracing_subscriber::fmt::try_init();
    let mut pbft = setup_with_checkpoint(true, 10);
    for seq in 1..11 {
        if seq == 10 {
            test_round(&mut pbft, 0, seq, 0, 10).await
        } else {
            test_round(&mut pbft, 0, seq, 0, 0).await
        }
    }
}

#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn test_checkpoint2() {
    let _ = tracing_subscriber::fmt::try_init();

    let mut pbft = setup_with_checkpoint(true, 10);
    for seq in 1..101 {
        if seq % 10 == 0 {
            test_round(&mut pbft, 0, seq, 0, seq).await
        } else {
            test_round(&mut pbft, 0, seq, 0, 0).await
        }
    }
}

#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn test_single_backup() {
    let _ = tracing_subscriber::fmt::try_init();

    let mut pbft = setup_pbft_backup();
    test_round(&mut pbft, 1, 1, 0, 0).await;
}

#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn test_many_backup() {
    let _ = tracing_subscriber::fmt::try_init();

    let mut pbft = setup_with_checkpoint(false, 10);
    for seq in 1..21 {
        if seq % 10 == 0 {
            test_round(&mut pbft, 1, seq, 0, seq).await
        } else {
            test_round(&mut pbft, 1, seq, 0, 0).await
        }
    }
}

#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn prepare_without_request() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();
    let PBFTContext {
        mut pbft,
        mut r,
        mut c,
        mut a,
    } = setup_pbft_backup();
    let me = 1;

    {
        let mut config: crate::PBFTConfig = pbft.config.as_ref().clone();
        config.request_proposals = true;
        pbft.config = Arc::new(config);
    }

    let request = Message::new(100, me, Request::new(0, "0".into()));
    let proposal = Proposal::Single(request.clone());
    let hash = DefaultHasher::hash_proposal(&proposal);
    let prepares = prepare_set(0, 1, 0, me, hash.clone());

    for message in prepares {
        pbft.on_message(message).await?;
    }
    assert_eq!(
        r.next().await.unwrap(),
        Message::new(pbft.id, 0, GetProposal::new(hash.clone()))
            .pack()
            .unwrap()
    );
    assert_eq!(Poll::Pending, poll!(c.next()));
    assert_eq!(Poll::Pending, poll!(a.next()));

    let assign = Message::new(
        0,
        me,
        Assign {
            sequence: 1,
            batch: proposal,
        },
    );
    pbft.on_message(assign.pack()?).await?;

    assert_eq!(Poll::Pending, poll!(c.next()));
    assert_eq!(Poll::Pending, poll!(a.next()));
    assert_eq!(
        Some(Message::broadcast(me, Prepare::new(1, 0, hash.clone())).pack()?),
        r.next().await
    );
    assert_eq!(
        Some(Message::broadcast(me, Commit::new(1, 0, hash.clone())).pack()?),
        r.next().await
    );

    Ok(())
}

#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn prepare_rev_without_request() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();
    let PBFTContext {
        mut pbft,
        mut r,
        mut c,
        mut a,
    } = setup_pbft_backup();
    let me = 1;

    {
        let mut config: crate::PBFTConfig = pbft.config.as_ref().clone();
        config.request_proposals = true;
        pbft.config = Arc::new(config);
    }

    let request = Message::new(100, me, Request::new(0, "0".into()));
    let proposal = Proposal::Single(request.clone());
    let hash = DefaultHasher::hash_proposal(&proposal);
    let prepares = prepare_set(0, 1, 0, me, hash.clone());

    for message in prepares.into_iter().rev() {
        pbft.on_message(message).await?;
    }
    assert_eq!(
        r.next().await.unwrap(),
        Message::new(pbft.id, 0, GetProposal::new(hash.clone()))
            .pack()
            .unwrap()
    );
    assert_eq!(Poll::Pending, poll!(c.next()));
    assert_eq!(Poll::Pending, poll!(a.next()));

    let assign = Message::new(
        0,
        me,
        Assign {
            sequence: 1,
            batch: proposal,
        },
    );
    pbft.on_message(assign.pack()?).await?;

    assert_eq!(Poll::Pending, poll!(c.next()));
    assert_eq!(Poll::Pending, poll!(a.next()));
    assert_eq!(
        Some(Message::broadcast(me, Prepare::new(1, 0, hash.clone())).pack()?),
        r.next().await
    );
    assert_eq!(
        Some(Message::broadcast(me, Commit::new(1, 0, hash.clone())).pack()?),
        r.next().await
    );

    Ok(())
}

#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn commit_without_request() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();
    let PBFTContext {
        mut pbft,
        mut r,
        mut c,
        mut a,
    } = setup_pbft_backup();

    {
        let mut config: crate::PBFTConfig = pbft.config.as_ref().clone();
        config.request_proposals = true;
        pbft.config = Arc::new(config);
    }

    let me = 1;

    let request = Message::new(100, me, Request::new(0, "0".into()));
    let proposal = Proposal::Single(request.clone());
    let hash = DefaultHasher::hash_proposal(&proposal);
    let prepares = prepare_set(0, 1, 0, me, hash.clone());
    let commits = commit_set(1, 0, me, hash.clone());

    for message in prepares {
        pbft.on_message(message).await?;
    }
    assert_eq!(
        r.next().await.unwrap(),
        Message::new(pbft.id, 0, GetProposal::new(hash.clone()))
            .pack()
            .unwrap()
    );
    assert_eq!(Poll::Pending, poll!(c.next()));
    assert_eq!(Poll::Pending, poll!(a.next()));

    for message in commits {
        pbft.on_message(message).await?;
    }
    assert_eq!(Poll::Pending, poll!(r.next()));
    assert_eq!(Poll::Pending, poll!(c.next()));
    assert_eq!(Poll::Pending, poll!(a.next()));

    let assign = Message::new(
        0,
        me,
        Assign {
            sequence: 1,
            batch: proposal,
        },
    );
    pbft.on_message(assign.pack()?).await?;

    assert_eq!(Some(Command::Request(request)), a.next().await);
    assert_eq!(Poll::Pending, poll!(a.next()));
    assert_eq!(
        Some(Message::broadcast(me, Prepare::new(1, 0, hash.clone())).pack()?),
        r.next().await
    );
    assert_eq!(
        Some(Message::broadcast(me, Commit::new(1, 0, hash.clone())).pack()?),
        r.next().await
    );

    Ok(())
}

#[tokio::test]
async fn wrong_primary() -> Result<(), Box<dyn std::error::Error>> {
    let mut ctx = setup_pbft();

    let me = 0;
    let request = Message::new(100, me, Request::new(0, "0".into()));
    let proposal = Proposal::Single(request);
    let hash = DefaultHasher::hash_proposal(&proposal);
    // ctx.pbft.on_request(proposal).await?;

    let pre_prepare = Message::broadcast(1, PrePrepare::new(1, 0, hash)).pack()?;

    ctx.pbft.on_message(pre_prepare).await.expect_err("rejected");
    Ok(())
}
