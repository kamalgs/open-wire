//! Gateway inter-cluster traffic (RS+/RS-/RMSG protocol).

mod conn;
mod handler;

pub(crate) use conn::*;
pub(crate) use handler::*;
