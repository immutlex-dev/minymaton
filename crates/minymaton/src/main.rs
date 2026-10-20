//! `minymaton`: the minymaton MCP server, installable as `cargo install
//! minymaton`. It takes the same options as `minymaton-server`.

use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    match minymaton_server::run(std::env::args().skip(1)).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("minymaton: {message}");
            ExitCode::FAILURE
        }
    }
}
