use std::error::Error;
use std::fmt;

use crate::ckb::CkbError;

#[derive(Debug)]
pub enum AppError {
    ConfigError(Box<dyn Error + Send + Sync>),
    CkbError(CkbError),
    MessageHandlingError(String),
    SporeError(String),
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AppError::ConfigError(e) => write!(f, "Configuration error: {}", e),
            AppError::CkbError(e) => write!(f, "CKB error: {}", e),
            AppError::MessageHandlingError(e) => write!(f, "Message handling error: {}", e),
            AppError::SporeError(e) => write!(f, "Spore error: {}", e),
        }
    }
}

impl Error for AppError {}

impl From<CkbError> for AppError {
    fn from(error: CkbError) -> Self {
        AppError::CkbError(error)
    }
}

pub type Result<T> = std::result::Result<T, AppError>;
