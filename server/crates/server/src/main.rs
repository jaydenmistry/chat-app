//! Command-line entry point.
//!
//!   server serve        run the server (the default)
//!   server migrate      apply database migrations, then exit
//!   server healthcheck  exit 0 if the local server is ready

use std::process::ExitCode;

use server::{config::Config, telemetry};

const USAGE: &str = "usage: server [serve | migrate | healthcheck]";

fn main() -> ExitCode {
    let command = std::env::args().nth(1);
    let config = match Config::from_env() {
        Ok(config) => config,
        Err(err) => {
            eprint!("{err}");
            return ExitCode::from(2);
        }
    };

    match command.as_deref() {
        None | Some("serve") => run(config, |config| async move {
            let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
            server::serve(config, listener, server::shutdown_signal()).await
        }),
        Some("migrate") => run(
            config,
            |config| async move { server::migrate(&config).await },
        ),
        Some("healthcheck") => {
            if server::healthcheck::check(config.bind_addr) {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            }
        }
        Some(other) => {
            eprintln!("unknown command `{other}`\n{USAGE}");
            ExitCode::from(2)
        }
    }
}

fn run<F, Fut>(config: Config, task: F) -> ExitCode
where
    F: FnOnce(Config) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<()>>,
{
    telemetry::init(config.log_format);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("building the Tokio runtime");
    match runtime.block_on(task(config)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            tracing::error!(error = format!("{err:#}"), "exiting");
            ExitCode::FAILURE
        }
    }
}
