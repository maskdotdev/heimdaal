use anyhow::Result;
use clap::{Parser, Subcommand};

use super::schema::{protocol_schema, runner_check};
use super::session::run_stdio_interactive;
use super::RUNNER_NAME;

#[derive(Parser, Debug)]
#[command(name = RUNNER_NAME)]
#[command(about = "Muzen SDK runner protocol host")]
pub struct RunnerCli {
    #[command(subcommand)]
    command: RunnerCommand,
}

#[derive(Subcommand, Debug)]
pub enum RunnerCommand {
    /// Serve newline-delimited JSON-RPC over stdin/stdout.
    Stdio,
    /// Print local runner diagnostics.
    Check,
    /// Print protocol schema metadata.
    Schema {
        #[command(subcommand)]
        command: RunnerSchemaCommand,
    },
}

#[derive(Subcommand, Debug)]
pub enum RunnerSchemaCommand {
    /// Export the runner protocol schema metadata as JSON.
    Export,
}

pub fn main_entry() {
    let code = match run_main() {
        Ok(code) => code,
        Err(error) => {
            eprintln!("{error:#}");
            4
        }
    };
    std::process::exit(code);
}

pub fn run_main() -> Result<i32> {
    let cli = RunnerCli::parse();
    match cli.command {
        RunnerCommand::Stdio => {
            let reader = std::io::BufReader::new(std::io::stdin());
            let writer = std::io::stdout();
            run_stdio_interactive(reader, writer)
        }
        RunnerCommand::Check => {
            println!("{}", serde_json::to_string_pretty(&runner_check())?);
            Ok(0)
        }
        RunnerCommand::Schema {
            command: RunnerSchemaCommand::Export,
        } => {
            println!("{}", serde_json::to_string_pretty(&protocol_schema())?);
            Ok(0)
        }
    }
}
