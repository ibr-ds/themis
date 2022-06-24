use std::fmt::{self, Display, Formatter};

use themis_core::{
    authentication::{Authenticator, AuthenticatorExt},
    net::Message,
};

use crate::{
    messages,
    messages::{NewView, PrepareProof, ViewChange},
};

pub(crate) fn verify_new_view(
    verifier: &mut dyn Authenticator<messages::PBFT>,
    message: &Message<NewView>,
) -> Result {
    let mut failures = Vec::new();

    // check ViewChange messages' signatures. ViewChange from same origin as NewView can be skipped
    for view_change in message
        .inner
        .view_changes
        .iter()
        .filter(|vc| vc.source != message.source)
    {
        if verifier.verify_unpacked(&view_change).is_err() {
            failures.push(Error::ViewChange {
                message: view_change.clone(),
            });
        }

        if let Err(err) =
            verify_view_change(verifier, view_change).map_err(|source| Error::ViewChangeNested {
                message: view_change.clone(),
                source,
            })
        {
            failures.push(err);
        }
    }

    for pre_prepare in message.inner.pre_prepares.iter() {
        if verifier.verify_unpacked(&pre_prepare).is_err() {
            failures.push(Error::PrePrepare {
                message: pre_prepare.clone(),
            })
        }
    }

    if failures.is_empty() {
        Ok(())
    } else {
        Err(ErrorList(failures))
    }
}

pub(crate) fn verify_view_change(
    verifier: &mut dyn Authenticator<messages::PBFT>,
    message: &Message<ViewChange>,
) -> Result {
    let mut failures = Vec::new();
    // don't verify checkpoint message from the same source as the view change
    // the view change message itself is verified, so this is fine
    // those don't currently get signatures anyway
    for checkpoint in message
        .inner
        .checkpoint_proof
        .iter()
        .filter(|cp| cp.source != message.source)
    {
        if verifier.verify_unpacked(&checkpoint).is_err() {
            failures.push(Error::Checkpoint {
                message: checkpoint.clone(),
            });
        }
    }

    for PrepareProof(ref pre_prepare, ref prepares) in message.inner.prepares.iter() {
        if pre_prepare.source != message.source && verifier.verify_unpacked(&pre_prepare).is_err() {
            failures.push(Error::PrePrepare {
                message: pre_prepare.clone(),
            });
        }

        for prepare in prepares.iter().filter(|p| p.source != message.source) {
            if verifier.verify_unpacked(&prepare).is_err() {
                failures.push(Error::Prepare {
                    message: prepare.clone(),
                });
            }
        }
    }

    if failures.is_empty() {
        Ok(())
    } else {
        Err(ErrorList(failures))
    }
}

use themis_core::net::Sequenced;

use crate::messages::Viewed;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Could not verify checkpoint {} from {}", message.sequence(), message.source)]
    Checkpoint {
        message: Message<messages::Checkpoint>,
    },
    #[error("Could not verify pre-prepare {} from {}", message.sequence(), message.source)]
    PrePrepare {
        message: Message<messages::PrePrepare>,
    },
    #[error("Could not verify prepare {} from {}", message.sequence(), message.source)]
    Prepare { message: Message<messages::Prepare> },
    #[error("Could not verify view-change {} from {}", message.view(), message.source)]
    ViewChange {
        message: Message<messages::ViewChange>,
    },
    #[error("Could not verify new-view {} from {}.\n{}", message.view(), message.source, source)]
    ViewChangeNested {
        message: Message<messages::ViewChange>,
        source: ErrorList,
    },
}

type Result = std::result::Result<(), ErrorList>;

#[derive(Debug)]
pub struct ErrorList(Vec<Error>);

impl Display for ErrorList {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        for error in &self.0 {
            writeln!(f, "{}", error)?;
        }
        Ok(())
    }
}

impl std::error::Error for ErrorList {}
