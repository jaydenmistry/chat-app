use std::process::ExitCode;

use server::config::Config;

fn main() -> ExitCode {
    match Config::from_env() {
        Ok(_) => ExitCode::SUCCESS,
        Err(err) => {
            eprint!("{err}");
            ExitCode::from(2)
        }
    }
}
