//! Binary entry point for the coverage-gate tool. All logic lives in the [`kb_cov_gate`]
//! library so it is unit-tested; `main` is a thin shell that maps the result to an exit code.

use std::process::ExitCode;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match kb_cov_gate::run(&args) {
        Ok(0) => ExitCode::SUCCESS,
        Ok(_) => ExitCode::FAILURE,
        Err(e) => {
            eprintln!("kb-cov-gate: {e}");
            ExitCode::from(2)
        }
    }
}
