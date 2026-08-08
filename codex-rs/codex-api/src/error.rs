use crate::rate_limits::RateLimitError;
use codex_client::TransportError;
use codex_protocol::error::UsageLimitReachedError;
use codex_protocol::protocol::MisalignmentErrorDetails;
use http::StatusCode;
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ApiError {
    #[error(transparent)]
    Transport(#[from] TransportError),
    #[error("api error {status}: {message}")]
    Api { status: StatusCode, message: String },
    #[error("stream error: {0}")]
    Stream(String),
    #[error("context window exceeded")]
    ContextWindowExceeded,
    #[error("quota exceeded")]
    QuotaExceeded,
    #[error("usage not included")]
    UsageNotIncluded,
    #[error("usage limit reached: {0}")]
    UsageLimitReached(UsageLimitReachedError),
    #[error("retryable error: {message}")]
    Retryable {
        message: String,
        delay: Option<Duration>,
    },
    #[error("rate limit exceeded: {message}")]
    RateLimitExceeded {
        message: String,
        delay: Option<Duration>,
    },
    #[error("rate limit: {0}")]
    RateLimit(String),
    #[error("invalid request: {message}")]
    InvalidRequest { message: String },
    /// The selected model, reasoning effort, or service tier is unavailable.
    #[error("model unavailable: {message}")]
    ModelUnavailable { message: String },
    #[error("cyber policy: {message}")]
    CyberPolicy { message: String },
    #[error("misalignment policy violation: {message}")]
    MisalignmentPolicyViolation {
        message: String,
        misalignment: Option<MisalignmentErrorDetails>,
    },
    #[error("server overloaded")]
    ServerOverloaded,
}

pub(crate) fn is_request_configuration_unavailable(
    code: Option<&str>,
    param: Option<&str>,
) -> bool {
    matches!(param, Some("model" | "service_tier" | "reasoning.effort"))
        || matches!(
            code,
            Some(
                "model_not_found"
                    | "model_not_supported"
                    | "unsupported_model"
                    | "service_tier_not_supported"
                    | "unsupported_service_tier"
                    | "reasoning_effort_not_supported"
                    | "unsupported_reasoning_effort"
            )
        )
}

impl From<RateLimitError> for ApiError {
    fn from(err: RateLimitError) -> Self {
        Self::RateLimit(err.to_string())
    }
}
