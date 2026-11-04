//! Structured logging with `tracing`.

use tracing_subscriber::EnvFilter;

use crate::config::LogFormat;

/// Installs the global subscriber. `RUST_LOG` sets the filter (default
/// `info`). Calling it twice is harmless, which integration tests rely on.
pub fn init(format: LogFormat) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    let _ = match format {
        LogFormat::Json => builder
            .json()
            // Event fields at the top level, plus the fields of the current
            // span (request_id, conn_id), so every line can be filtered by ID.
            .flatten_event(true)
            .with_current_span(true)
            .with_span_list(false)
            .try_init(),
        LogFormat::Pretty => builder.try_init(),
    };
}
