use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand, ValueEnum};

use crate::bench::bench_job;
use crate::concurrent::bench::{
    run_compare, run_job_concurrent, run_job_concurrent_with_events, run_real_bench,
    ConcurrentBenchArgs, ConcurrentRealBenchArgs,
};
use crate::contracts::*;
use crate::events::EventEmitter;
use crate::util::{redact_known_secrets, DEFAULT_MODEL};

#[derive(Parser, Debug)]
#[command(name = "muzen")]
#[command(about = "Rust read-only review-runtime MVP for Heimdaal")]
pub(crate) struct Cli {
    #[command(subcommand)]
    pub(crate) command: Command,
}

#[derive(Subcommand, Debug)]
pub(crate) enum Command {
    /// Run a ReviewRunJobV1 from JSON and emit JSONL RunEventV1 records.
    Run(RunArgs),
    /// Convenience benchmark wrapper that builds a ReviewRunJobV1 for a repo.
    Bench(BenchArgs),
    /// Build the benchmark ReviewRunJobV1 JSON without executing it.
    BenchJob(BenchArgs),
    /// Compare a serial concurrent-owned baseline against the async concurrent runtime.
    CompareConcurrent(ConcurrentBenchArgs),
    /// Run the async concurrent runtime against an OpenAI-compatible model.
    BenchConcurrent(ConcurrentRealBenchArgs),
}

#[derive(Parser, Debug, Clone)]
pub(crate) struct RunArgs {
    #[arg(long, default_value = "-")]
    pub(crate) job: PathBuf,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, ValueEnum)]
pub(crate) enum BenchTerminalPolicy {
    Normal,
    FindingRequired,
}

#[derive(Parser, Debug, Clone)]
pub(crate) struct BenchArgs {
    #[arg(long, default_value = ".")]
    pub(crate) repo: PathBuf,

    #[arg(long, default_value_t = 10)]
    pub(crate) sessions: usize,

    #[arg(long, default_value_t = 10)]
    pub(crate) max_active: usize,

    #[arg(long, default_value_t = 7)]
    pub(crate) max_turns: usize,

    #[arg(long, default_value_t = 14)]
    pub(crate) max_tool_calls: usize,

    #[arg(long, default_value_t = 1000)]
    pub(crate) hold_ms: u64,

    #[arg(long, default_value_t = 200)]
    pub(crate) max_file_kb: usize,

    #[arg(long, default_value_t = 120)]
    pub(crate) max_search_matches: usize,

    #[arg(long, default_value = DEFAULT_MODEL)]
    pub(crate) model: String,

    #[arg(long, default_value_t = 256)]
    pub(crate) max_output_tokens: u32,

    #[arg(long, value_enum, default_value_t = BenchTerminalPolicy::Normal)]
    pub(crate) terminal_policy: BenchTerminalPolicy,
}

pub(crate) fn run_json(args: RunArgs) -> Result<i32> {
    let mut input = String::new();
    if args.job == Path::new("-") {
        std::io::stdin().read_to_string(&mut input)?;
    } else {
        input = fs::read_to_string(&args.job)
            .with_context(|| format!("failed to read job {}", args.job.display()))?;
    }
    let job: ReviewRunJobV1 =
        serde_json::from_str(&input).context("invalid ReviewRunJobV1 JSON")?;
    let emitter = Arc::new(EventEmitter::stdout(
        job.run_id.clone(),
        job.attempt,
        job.output_redaction.policy_id.clone(),
    ));
    let report = run_job_concurrent_with_events(job, Some(emitter))?;
    Ok(if report.completed_sessions == report.sessions {
        0
    } else {
        4
    })
}

pub fn main_entry() {
    let code = match run_main() {
        Ok(code) => code,
        Err(error) => {
            eprintln!("{}", redact_known_secrets(&format!("{error:#}"), &[]));
            4
        }
    };
    std::process::exit(code);
}

pub(crate) fn run_main() -> Result<i32> {
    let cli = Cli::parse();
    match cli.command {
        Command::Run(args) => run_json(args),
        Command::Bench(args) => {
            let hold_ms = args.hold_ms;
            let job = bench_job(&args)?;
            let report = run_job_concurrent(job)?;
            std::thread::sleep(std::time::Duration::from_millis(hold_ms));
            println!("{}", serde_json::to_string_pretty(&report)?);
            if !report.benchmark_valid {
                bail!(
                    "concurrent benchmark gates failed: {:?}",
                    report.benchmark_failures
                );
            }
            if report.completed_sessions != report.sessions {
                bail!(
                    "only {}/{} sessions completed",
                    report.completed_sessions,
                    report.sessions
                );
            }
            Ok(0)
        }
        Command::BenchJob(args) => {
            let job = bench_job(&args)?;
            println!("{}", serde_json::to_string_pretty(&job)?);
            Ok(0)
        }
        Command::CompareConcurrent(args) => {
            let report = run_compare(args)?;
            if !report.concurrent.benchmark_valid {
                bail!("concurrent comparison proof gates failed");
            }
            Ok(0)
        }
        Command::BenchConcurrent(args) => {
            let report = run_real_bench(args)?;
            if !report.benchmark_valid {
                bail!(
                    "concurrent real benchmark gates failed: {:?}",
                    report.benchmark_failures
                );
            }
            Ok(0)
        }
    }
}
