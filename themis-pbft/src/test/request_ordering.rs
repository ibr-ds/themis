use std::sync::Arc;

use super::{commit_set, prepare_set, setup_pbft, setup_pbft_backup, PBFTContext};
use crate::{
    messages::{
        self, Assign, GetProposal, GetResponse, NewView, PBFTTag, PrePrepare, Prepare, ViewChange,
    },
    timeout::Info,
    DefaultHasher, ProposalHasher,
};
use assert_matches::assert_matches;
use futures_util::{poll, stream::StreamExt};
use themis_core::{
    app::{self, Request},
    authentication::{dummy::Dummy, AuthenticatorExt},
    net::Message,
    protocol::{Error, Event, Proposal, Protocol2, ProtocolTag},
};
use tokio_test::*;

#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn ignore_duplicate() {
    let PBFTContext {
        mut pbft, mut r, ..
    } = setup_pbft();

    let request = Message::new(100, 0, Request::new(0, "test".into()));
    let proposal = Proposal::Single(request.clone());
    let proposal_d = DefaultHasher::hash_proposal(&proposal);

    pbft.on_request(proposal.clone()).await.unwrap();

    let assign = r.next().await.unwrap();
    let pre_prepare = r.next().await.unwrap();

    assert_eq!(
        assign,
        Message::broadcast(0, Assign::new(1, proposal.clone()))
            .pack()
            .unwrap()
    );
    assert_eq!(
        pre_prepare,
        Message::broadcast(0, PrePrepare::new(1, 0, proposal_d.clone()))
            .pack()
            .unwrap()
    );
    assert_matches!(
        pbft.on_request(proposal.clone()).await,
        Err(Error::Reject(themis_core::protocol::Rejection::Duplicate))
    );
    // duplicate, ignored
    assert_pending!(poll!(r.next()));
    assert_pending!(poll!(r.next()));
}

#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn ignore_duplicate_prepared() {
    let PBFTContext {
        mut pbft, mut r, ..
    } = setup_pbft();

    let request = Message::new(100, 0, Request::new(0, "test".into()));
    let proposal = Proposal::Single(request.clone());
    let proposal_d = DefaultHasher::hash_proposal(&proposal);

    pbft.on_request(proposal.clone()).await.unwrap();

    let assign = r.next().await.unwrap();
    let pre_prepare = r.next().await.unwrap();

    assert_eq!(
        assign,
        Message::broadcast(0, Assign::new(1, proposal.clone()))
            .pack()
            .unwrap()
    );
    assert_eq!(
        pre_prepare,
        Message::broadcast(0, PrePrepare::new(1, 0, proposal_d.clone()))
            .pack()
            .unwrap()
    );

    let prepares = prepare_set(0, 1, 0, 0, proposal_d.clone());
    for prepare in prepares {
        pbft.on_message(prepare).await.unwrap();
    }
    let commit = r.next().await.unwrap();
    assert_eq!(commit.tag(), ProtocolTag::Protocol(PBFTTag::COMMIT));

    assert_matches!(
        pbft.on_request(proposal.clone()).await,
        Err(Error::Reject(themis_core::protocol::Rejection::Duplicate))
    );
    // duplicate, ignored
    assert_pending!(poll!(r.next()));
    assert_pending!(poll!(r.next()));
}

#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn ignore_duplicate_committed() {
    let _ = tracing_subscriber::fmt::try_init();

    let PBFTContext {
        mut pbft,
        mut r,
        c: _,
        mut a,
    } = setup_pbft();

    let request = Message::new(100, 0, Request::new(0, "test".into()));
    let proposal = Proposal::Single(request.clone());
    let proposal_d = DefaultHasher::hash_proposal(&proposal);

    pbft.on_request(proposal.clone()).await.unwrap();

    let assign = r.next().await.unwrap();
    let pre_prepare = r.next().await.unwrap();

    assert_eq!(
        assign,
        Message::broadcast(0, Assign::new(1, proposal.clone()))
            .pack()
            .unwrap()
    );
    assert_eq!(
        pre_prepare,
        Message::broadcast(0, PrePrepare::new(1, 0, proposal_d.clone()))
            .pack()
            .unwrap()
    );

    let prepares = prepare_set(0, 1, 0, 0, proposal_d.clone());
    for prepare in prepares {
        pbft.on_message(prepare).await.unwrap();
    }
    let commit = r.next().await.unwrap();
    assert_eq!(commit.tag(), ProtocolTag::Protocol(PBFTTag::COMMIT));

    let commits = commit_set(1, 0, 0, proposal_d.clone());
    for commit in commits {
        pbft.on_message(commit).await.unwrap();
    }
    let execute = a.next().await.unwrap();
    assert_eq!(execute, app::Command::Request(request));

    assert_matches!(
        pbft.on_request(proposal.clone()).await,
        Err(Error::Reject(themis_core::protocol::Rejection::Duplicate))
    );
    // duplicate, ignored
    assert_pending!(poll!(r.next()));
    assert_pending!(poll!(r.next()));
}

#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn pre_prepare_after_view_change() {
    let _ = tracing_subscriber::fmt::try_init();
    let PBFTContext {
        mut pbft, mut r, ..
    } = setup_pbft_backup();

    let request = Message::new(100, 0, Request::new(0, "test".into()));
    let proposal = Proposal::Single(request.clone());
    let proposal_d = DefaultHasher::hash_proposal(&proposal);

    let request2 = Message::new(100, 1, Request::new(1, "test2".into()));
    let r2_d = DefaultHasher::hash_request(&request2);
    let proposal2 = Proposal::Single(request2.clone());
    let _proposal_d2 = DefaultHasher::hash_proposal(&proposal2);

    let pre_prepare = Message::broadcast(0, PrePrepare::new(1, 0, proposal_d.clone()));
    let assign = Message::broadcast(0, Assign::new(1, proposal));

    pbft.on_message(assign.pack().unwrap()).await.unwrap();
    pbft.on_message(pre_prepare.pack().unwrap()).await.unwrap();

    let prepares = prepare_set(0, 1, 0, 1, proposal_d.clone());
    for prepare in prepares.into_iter().skip(1) {
        pbft.on_message(prepare).await.unwrap();
    }

    let _prepare = r.next().await.unwrap();
    let _commit = r.next().await.unwrap();

    pbft.on_request(proposal2).await.unwrap();
    let _forward = r.next().await.unwrap();

    pbft.on_event(Info::Request(r2_d)).await.unwrap();
    // one timeout for the view change;

    let view_change: Message<ViewChange> = r.next().await.unwrap().unpack().unwrap();
    let mut view_change2 = view_change.clone();
    view_change2.source = 2;
    let mut view_change3 = view_change.clone();
    view_change3.source = 3;

    sign_vc(&mut view_change2);
    sign_vc(&mut view_change3);

    pbft.on_message(view_change2.pack().unwrap()).await.unwrap();
    pbft.on_message(view_change3.pack().unwrap()).await.unwrap();

    let _new_view: Message<NewView> = r.next().await.unwrap().unpack().unwrap();
    assert_eq!(pbft.reorder_buffer.len(), 1, "timers after view change");
    tracing::info!("Before reorder event");
    let reorder = pbft.next().await.unwrap();
    assert_eq!(Event::Reorder(Proposal::Single(request2)), reorder);
}

#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn get_proposal_wait_prepared() {
    let _ = tracing_subscriber::fmt::try_init();
    let PBFTContext {
        mut pbft, mut r, ..
    } = setup_pbft_backup();

    {
        let mut config: crate::PBFTConfig = pbft.config.as_ref().clone();
        config.request_proposals = true;
        pbft.config = Arc::new(config);
    }

    let request = Message::new(100, 0, Request::new(0, "test".into()));
    let proposal = Proposal::Single(request.clone());
    let digest = DefaultHasher::hash_proposal(&proposal);

    let prepares = prepare_set(0, 1, 0, pbft.id, digest.clone());

    pbft.on_message(prepares[0].clone()).await.unwrap();

    let get_proposal = r.next().await.unwrap().unpack().unwrap();
    assert_eq!(
        get_proposal,
        Message::new(pbft.id, 0, GetProposal::new(digest.clone()))
    );

    let forward = Message::new(2, pbft.id, GetResponse::new(proposal));
    pbft.on_message(forward.pack().unwrap()).await.unwrap();

    // We need a prepare quorum to agree on foreign requests
    assert_pending!(poll!(r.next()));
}

#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn get_proposal_prepared() {
    let _ = tracing_subscriber::fmt::try_init();
    let PBFTContext {
        mut pbft, mut r, ..
    } = setup_pbft_backup();

    {
        let mut config: crate::PBFTConfig = pbft.config.as_ref().clone();
        config.request_proposals = true;
        pbft.config = Arc::new(config);
    }

    let request = Message::new(100, 0, Request::new(0, "test".into()));
    let proposal = Proposal::Single(request.clone());
    let proposal_d = DefaultHasher::hash_proposal(&proposal);

    let prepares = prepare_set(0, 1, 0, pbft.id, proposal_d.clone());

    for prepare in prepares {
        pbft.on_message(prepare).await.unwrap();
    }

    let get_proposal = r.next().await.unwrap().unpack().unwrap();
    assert_eq!(
        get_proposal,
        Message::new(pbft.id, 0, GetProposal::new(proposal_d.clone()))
    );

    let forward = Message::new(2, pbft.id, GetResponse::new(proposal));
    pbft.on_message(forward.pack().unwrap()).await.unwrap();

    let prepare = r.next().await.unwrap().unpack().unwrap();
    assert_eq!(
        prepare,
        Message::broadcast(pbft.id, Prepare::new(1, 0, proposal_d.clone()))
    );
}

fn sign_vc(view_change: &mut Message<ViewChange>) {
    let mut signer = Dummy;

    for prepare_proof in view_change.inner.prepares.iter_mut() {
        signer.sign_unpacked(&mut prepare_proof.0).unwrap();
        for prepare in prepare_proof.1.iter_mut() {
            signer.sign_unpacked(prepare).unwrap();
        }
    }

    for checkpoint in view_change.inner.checkpoint_proof.iter_mut() {
        AuthenticatorExt::<messages::PBFT>::sign_unpacked(&mut signer, checkpoint).unwrap();
    }
}
