use std::task::Poll;

use bytes::Bytes;
use futures_util::{poll, stream::StreamExt};

use assert_matches::assert_matches;
use themis_core::{
    app::{Command, Request},
    net::{Message, Sequenced},
    protocol::{Error, Proposal, Protocol2, ProtocolTag, Rejection},
};

use crate::{
    messages::{
        Commit, Forward, NewView, PBFTMessage,
        PBFTTag::{self, *},
        PrePrepare, ViewChange, Viewed,
    },
    test::setup_pbft_backup,
    DefaultHasher, Event, ProposalHasher,
};

use super::{client, commit_set, prepare_set, regular::send_single_request};
use crate::{
    messages::{Assign, Prepare, PrepareProof},
    test::PBFTContext,
    timeout::Info,
};
use themis_core::authentication::{dummy::Dummy, AuthenticatorExt};

#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn test_timeout() {
    let me = 1;
    let view = 0;
    let mut pbft = setup_pbft_backup();
    let request = Message::new(client(), me, Request::new(1, Bytes::new()));
    let proposal: Proposal = Proposal::Single(request.clone());
    let hash = DefaultHasher::hash_proposal(&proposal);

    send_single_request(&mut pbft, proposal.clone()).await;

    assert_eq!(
        poll!(pbft.r.next()),
        Poll::Ready(Some(Message::new(me, 0, Forward(proposal)).pack().unwrap()))
    );
    assert_eq!(poll!(pbft.a.next()), Poll::Pending);

    let x = pbft.pbft.next().await; //timeout
    assert_eq!(x, Some(Event::Protocol(Info::Request(hash.clone()))));

    pbft.pbft
        .on_event(Info::Request(hash.clone()))
        .await
        .unwrap();

    assert_eq!(
        poll!(pbft.r.next()),
        Poll::Ready(Some(
            Message::broadcast(
                me,
                ViewChange::new(view + 1, 0, Vec::new().into(), Vec::new().into())
            )
            .pack()
            .unwrap()
        ))
    );
    assert_eq!(poll!(pbft.c.next()), Poll::Pending);
    assert_eq!(poll!(pbft.a.next()), Poll::Pending);
}

#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn test_view_change() {
    let _ = tracing_subscriber::fmt::try_init();

    let me = 1;
    let view = 0;
    let mut pbft = setup_pbft_backup();
    pbft.pbft.id = 1;

    let instances: usize = 10;
    let props = prepare_instances(me, view, &mut pbft, instances).await;

    pbft.pbft
        .on_event(Info::Request(props[0].clone()))
        .await
        .unwrap(); // trigger timeout

    let vc: PBFTMessage = pbft.r.next().await.unwrap();
    let mut vc: Message<ViewChange> = vc.unpack().unwrap();
    assert_eq!(vc.inner.checkpoint_proof, Vec::new().into_boxed_slice());
    assert_eq!(vc.inner.checkpoint, 0);
    assert_eq!(vc.inner.prepares.len(), instances);

    let mut prepares = vc.inner.prepares.into_vec();

    // Need to add some fake signatures to pretend this message comes from another replica
    // That means sign our own messages
    add_self_signatures(me, prepares.iter_mut());

    vc.source = 0;
    prepares.remove(1); // remove some
    vc.inner.prepares = prepares.into_boxed_slice();
    Protocol2::on_message(&mut pbft.pbft, vc.pack().unwrap())
        .await
        .unwrap();

    vc.source = 2;
    let mut prepares = vc.inner.prepares.into_vec();
    prepares.remove(0); // remove some more
    vc.inner.prepares = prepares.into_boxed_slice();
    Protocol2::on_message(&mut pbft.pbft, vc.pack().unwrap())
        .await
        .unwrap();

    let nv = assert_matches!(poll!(pbft.r.next()), Poll::Ready(Some(nv)) => nv);

    let nv: Message<NewView> = nv.unpack().unwrap();
    assert_eq!(1, nv.view());
    assert_eq!(1, pbft.pbft.view);
    assert_eq!(instances, nv.inner.pre_prepares.len());

    while poll!(pbft.r.next()).is_ready() {}
    for i in 1..3 {
        let hash = DefaultHasher::hash_proposal(&Proposal::Single(Message::new(
            100,
            me,
            Request::new(i, i.to_string().into()),
        )));
        let prepare_set = prepare_set(1, i, 1, 1, hash);
        for msg in prepare_set {
            Protocol2::on_message(&mut pbft.pbft, msg).await.unwrap();
        }
        assert_matches!(pbft.r.next().await, Some(ref msg) if msg.inner.tag == ProtocolTag::Protocol(PBFTTag::COMMIT) => {
            assert_eq!(1, msg.unpack::<Commit>().unwrap().view());
        });
    }

    // next view change

    pbft.pbft
        .on_event(Info::Request(props[0].clone()))
        .await
        .unwrap(); //view change some more
    let vc_packed: PBFTMessage = pbft.r.next().await.unwrap();
    let vc: Message<ViewChange> = vc_packed.unpack().unwrap();
    assert_eq!(vc.view(), 2);
    assert_eq!(vc.inner.checkpoint_proof, Vec::new().into_boxed_slice());
    assert_eq!(vc.inner.checkpoint, 0);
    assert_eq!(vc.inner.prepares.len(), instances);
    assert_eq!(
        vc.inner
            .prepares
            .iter()
            .min_by_key(|proof| proof.0.sequence())
            .unwrap()
            .0
            .view(),
        1
    ); // prepared in view 1 instead of 0
       //        assert_eq!(vc.inner.prepares[1].0.view(), 1);

    Protocol2::on_message(&mut pbft.pbft, vc.pack().unwrap())
        .await
        .unwrap();
    Protocol2::on_message(&mut pbft.pbft, vc.pack().unwrap())
        .await
        .unwrap();

    assert_matches!(poll!(pbft.r.next()), Poll::Pending); // no new view here, because we are not primary for view 2
}

async fn prepare_instances(
    me: u64,
    view: u64,
    pbft: &mut PBFTContext,
    instances: usize,
) -> Vec<Bytes> {
    let mut proposals = Vec::new();
    for i in 1..=instances as u64 {
        let request: Proposal = Message::new(100, me, Request::new(i, i.to_string().into())).into();
        let hash = DefaultHasher::hash_proposal(&request);
        proposals.push(hash.clone());
        let assign = Message::new(
            0,
            me,
            Assign {
                sequence: i,
                batch: request,
            },
        );
        Protocol2::on_message(&mut pbft.pbft, assign.pack().unwrap())
            .await
            .unwrap();
        let prepare_set = prepare_set(0, i, view, me, hash.clone());
        for msg in prepare_set {
            Protocol2::on_message(&mut pbft.pbft, msg).await.unwrap();
        }
        assert_matches!(poll!(pbft.r.next()), Poll::Ready(Some(ref m)) if m.tag() == ProtocolTag::Protocol(PREPARE));
        //prepare
        assert_matches!(poll!(pbft.r.next()), Poll::Ready(Some(ref m)) if m.tag() == ProtocolTag::Protocol(COMMIT));
        //commit
    }
    proposals
}

#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn test_nested_view_change() {
    let me = 1;
    let view = 0;
    let mut pbft = setup_pbft_backup();
    pbft.pbft.id = 1;

    let _ = tracing_subscriber::fmt::try_init();

    let instances: usize = 10;

    let props = prepare_instances(me, view, &mut pbft, instances).await;

    pbft.pbft
        .on_event(Info::Request(props[0].clone()))
        .await
        .unwrap(); // trigger timeout

    let vc: PBFTMessage = pbft.r.next().await.unwrap();
    let vc: Message<ViewChange> = vc.unpack().unwrap();
    assert_eq!(vc.view(), 1);
    assert_eq!(vc.inner.checkpoint_proof, Vec::new().into_boxed_slice());
    assert_eq!(vc.inner.checkpoint, 0);
    assert_eq!(vc.inner.prepares.len(), instances);

    pbft.pbft
        .on_event(Info::ViewChange { to: 1 })
        .await
        .unwrap(); // trigger timeout again

    let vc2: PBFTMessage = pbft.r.next().await.unwrap();
    let mut vc2: Message<ViewChange> = vc2.unpack().unwrap();
    assert_eq!(vc2.view(), 2);
    assert_eq!(vc2.inner.checkpoint_proof, Vec::new().into_boxed_slice());
    assert_eq!(vc2.inner.checkpoint, 0);
    assert_eq!(vc2.inner.prepares.len(), instances);

    // submit some outdated view changes
    assert_matches!(
        Protocol2::on_message(&mut pbft.pbft, vc.pack().unwrap()).await,
        Err(Error::Reject(Rejection::State { .. }))
    );

    assert_matches!(
        Protocol2::on_message(&mut pbft.pbft, vc.pack().unwrap()).await,
        Err(Error::Reject(Rejection::State { .. }))
    );

    assert_eq!(pbft.pbft.view, 0); // nothing happens
    assert!(matches!(
        pbft.pbft.state,
        crate::ViewState::ViewChange { new_view: 2, .. }
    ));
    assert_eq!(poll!(pbft.r.next()), Poll::Pending);

    // now for view 2
    Protocol2::on_message(&mut pbft.pbft, vc2.pack().unwrap())
        .await
        .unwrap();
    Protocol2::on_message(&mut pbft.pbft, vc2.pack().unwrap())
        .await
        .unwrap();
    assert_eq!(pbft.pbft.view, 0);
    assert!(matches!(
        pbft.pbft.state,
        crate::ViewState::ViewChange { new_view: 2, .. }
    ));
    assert_eq!(poll!(pbft.r.next()), Poll::Pending); // no message, we are not primary for 2

    let pre_p = (1..=instances as u64)
        .map(|i| {
            let request = Message::new(100, me, Request::new(i, i.to_string().into()));
            let proposal: Proposal = request.into();
            let hash = DefaultHasher::hash_proposal(&proposal);
            let mut msg = Message::broadcast(2, PrePrepare::new(i, 2, hash));
            Dummy.sign_unpacked(&mut msg).unwrap();
            msg
        })
        .collect();

    add_self_signatures(me, vc2.inner.prepares.iter_mut());
    Dummy.sign_unpacked(&mut vc2).unwrap();

    let mut vcs = Vec::new();
    for i in 1..=3 {
        let mut vc = vc2.clone();
        vc.source = i;
        vcs.push(vc);
    }

    let new_view = Message::new(2, 1, NewView::new(2, vcs.into(), pre_p));
    Protocol2::on_message(&mut pbft.pbft, new_view.pack().unwrap())
        .await
        .unwrap();
    assert_eq!(pbft.pbft.view, 2);
}

#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn empty_rounds() {
    let _ = tracing_subscriber::fmt::try_init();

    let PBFTContext {
        mut pbft,
        mut r,
        c: _,
        a: _,
    } = setup_pbft_backup();
    let me = pbft.id;

    // input 3 request
    let mut hash = Bytes::new();
    for i in 2..3_u64 {
        let request = Message::new(100, 0, Request::new(i, "0".into()));
        let proposal: Proposal = request.into();
        hash = DefaultHasher::hash_proposal(&proposal);
        let assign = Message::broadcast(0, Assign::new(i, proposal));
        pbft.on_message(assign.pack().unwrap()).await.unwrap();
    }

    // prepare #3
    let prepares = prepare_set(0, 3, 0, me, hash.clone());
    for message in prepares.clone() {
        pbft.on_message(message).await.unwrap();
    }

    assert_eq!(
        Some(
            Message::broadcast(me, Prepare::new(3, 0, hash.clone()))
                .pack()
                .unwrap()
        ),
        r.next().await
    );

    assert_eq!(
        Some(
            Message::broadcast(me, Commit::new(3, 0, hash.clone()))
                .pack()
                .unwrap()
        ),
        r.next().await
    );

    let vc2 = Message::broadcast(
        2,
        ViewChange::new(
            1,
            0,
            Box::new([]),
            vec![PrepareProof(
                prepares[0].unpack().unwrap(),
                prepares[1..].iter().map(|p| p.unpack().unwrap()).collect(),
            )]
            .into_boxed_slice(),
        ),
    );
    let vc3 = {
        let mut vc = vc2.clone();
        vc.source = 3;
        vc
    };

    let vc1 = {
        let mut vc = vc2.clone();
        vc.source = 1;
        vc
    };

    pbft.on_message(vc2.pack().unwrap()).await.unwrap();
    pbft.on_message(vc3.pack().unwrap()).await.unwrap();

    let mut pre_prepares: Vec<_> = (1..=3)
        .map(|i| Message::broadcast(me, PrePrepare::new(i, 1, "".into())))
        .collect();
    for pre_p in &mut pre_prepares {
        Dummy.sign_unpacked(pre_p).unwrap();
    }
    pre_prepares[2].inner.request = hash.clone();

    let nv = r.next().await.unwrap().unpack().unwrap();
    let expected_nv = Message::broadcast(
        me,
        NewView::new(
            1,
            // should generate it's own vc message after receiving two
            vec![vc2, vc3, vc1].into_boxed_slice(),
            pre_prepares.into_boxed_slice(),
        ),
    );
    assert_eq!(expected_nv, nv);

    // we only prepared 3, 1 and 2 must be empty
    assert!(nv.inner.pre_prepares[0].inner.request.is_empty());
    assert!(nv.inner.pre_prepares[1].inner.request.is_empty());
}

#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn order_empty_round() {
    let _ = tracing_subscriber::fmt::try_init();

    let PBFTContext {
        mut pbft,
        mut r,
        c: _,
        mut a,
    } = setup_pbft_backup();
    let me = pbft.id;

    let prepares = prepare_set(0, 1, 0, me, Bytes::new());
    for message in prepares {
        pbft.on_message(message).await.unwrap();
    }

    assert_eq!(
        Message::broadcast(me, Prepare::new(1, 0, Bytes::new())),
        r.next().await.unwrap().unpack().unwrap()
    );
    assert_eq!(
        Message::broadcast(me, Commit::new(1, 0, Bytes::new())),
        r.next().await.unwrap().unpack().unwrap()
    );

    let commits = commit_set(1, 0, me, Bytes::new());
    for message in commits {
        pbft.on_message(message).await.unwrap();
    }

    // after this we should be able to execute sequence 2

    let request = Message::new(100, 0, Request::new(0, "0".into()));
    let proposal: Proposal = request.clone().into();
    let hash = DefaultHasher::hash_proposal(&proposal);

    let assign = Message::broadcast(0, Assign::new(2, proposal));
    let prepares = prepare_set(0, 2, 0, me, hash.clone());
    let commits = commit_set(2, 0, me, hash.clone());

    pbft.on_message(assign.pack().unwrap()).await.unwrap();
    for message in prepares {
        pbft.on_message(message).await.unwrap();
    }
    for message in commits {
        pbft.on_message(message).await.unwrap();
    }

    assert_eq!(
        Message::broadcast(me, Prepare::new(2, 0, hash.clone())),
        r.next().await.unwrap().unpack().unwrap()
    );
    assert_eq!(
        Message::broadcast(me, Commit::new(2, 0, hash.clone())),
        r.next().await.unwrap().unpack().unwrap()
    );
    assert_eq!(Command::Request(request), a.next().await.unwrap());
}

pub(crate) fn add_self_signatures<'a>(me: u64, proofs: impl Iterator<Item = &'a mut PrepareProof>) {
    for proof in proofs {
        if proof.0.source == me {
            Dummy.sign_unpacked(&mut proof.0).unwrap();
        } else if let Some(prepare) = proof.1.iter_mut().find(|p| p.source == me) {
            Dummy.sign_unpacked(prepare).unwrap();
        }
    }
}
