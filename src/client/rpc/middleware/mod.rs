use std::error::Error;

pub mod retry;
pub mod timeout;

pub type BoxError = Box<dyn Error + Send + Sync + 'static>;
