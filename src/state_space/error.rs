use alloy::transports::TransportErrorKind;
use thiserror::Error;

use crate::amms::error::AMMError;

#[derive(Error, Debug)]
pub enum StateSpaceError {
    #[error(transparent)]
    AMMError(#[from] AMMError),
    #[error(transparent)]
    TransportError(#[from] alloy::transports::RpcError<TransportErrorKind>),
    #[error(transparent)]
    JoinError(#[from] tokio::task::JoinError),
    #[error("Block Number Does not Exist")]
    MissingBlockNumber,
    #[error(
        "Reorg detected from block {current_block} to {new_block}. Reorgs are not supported in V1."
    )]
    ReorgNotSupported { current_block: u64, new_block: u64 },
}
