use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};
use codex_thread_manager::{
    CodexSyncEngine, default_backup_root, default_codex_home, default_log_path, default_state_path,
};
use serde_json::json;

#[derive(Parser)]
#[command(about = "Manage Codex threads across providers.")]
struct Cli {
    #[arg(long, default_value_os_t = default_codex_home())]
    codex_home: PathBuf,
    #[arg(long, default_value_os_t = default_state_path())]
    state_path: PathBuf,
    #[arg(long, default_value_os_t = default_backup_root())]
    backup_root: PathBuf,
    #[arg(long, default_value_os_t = default_log_path())]
    log_path: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Clone, Debug, ValueEnum)]
enum CleanupScopeArg {
    Archived,
    Active,
    All,
}

impl CleanupScopeArg {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Archived => "archived",
            Self::Active => "active",
            Self::All => "all",
        }
    }
}

#[derive(Subcommand)]
enum Command {
    Status {
        #[arg(long, default_value = "openai")]
        provider_a: String,
        #[arg(long, default_value = "cpa")]
        provider_b: String,
    },
    #[command(name = "status-all")]
    StatusAll,
    Space,
    Sync {
        #[arg(long)]
        source: String,
        #[arg(long)]
        target: String,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
    #[command(name = "sync-bidirectional")]
    SyncBidirectional {
        #[arg(long, default_value = "openai")]
        provider_a: String,
        #[arg(long, default_value = "cpa")]
        provider_b: String,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
    #[command(name = "sync-all")]
    SyncAll {
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
    #[command(name = "sync-selected")]
    SyncSelected {
        #[arg(long = "provider", required = true, num_args = 1.., value_delimiter = ',')]
        providers: Vec<String>,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
    Cleanup {
        #[arg(long, value_enum, default_value_t = CleanupScopeArg::Archived)]
        scope: CleanupScopeArg,
        #[arg(long)]
        older_than_days: Option<i64>,
        #[arg(long, default_value_t = 0)]
        keep_latest: usize,
        #[arg(long, default_value_t = false)]
        apply: bool,
    },
}

fn main() -> std::process::ExitCode {
    let cli = Cli::parse();
    let engine = CodexSyncEngine::new(
        cli.codex_home,
        cli.state_path,
        cli.backup_root,
        cli.log_path,
    );

    let result = match cli.command {
        Command::Status {
            provider_a,
            provider_b,
        } => engine
            .status(&provider_a, &provider_b)
            .map(|result| json!(result)),
        Command::StatusAll => engine.status_all().map(|result| json!(result)),
        Command::Space => engine.space().map(|result| json!(result)),
        Command::Sync {
            source,
            target,
            dry_run,
        } => engine
            .sync(&source, &target, dry_run)
            .map(|result| json!(result)),
        Command::SyncBidirectional {
            provider_a,
            provider_b,
            dry_run,
        } => engine
            .sync_bidirectional(&provider_a, &provider_b, dry_run)
            .map(|result| json!(result)),
        Command::SyncAll { dry_run } => engine.sync_all(dry_run).map(|result| json!(result)),
        Command::SyncSelected { providers, dry_run } => engine
            .sync_selected(&providers, dry_run)
            .map(|result| json!(result)),
        Command::Cleanup {
            scope,
            older_than_days,
            keep_latest,
            apply,
        } => engine
            .cleanup(scope.as_str(), older_than_days, keep_latest, apply)
            .map(|result| json!(result)),
    };

    match result {
        Ok(result) => {
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "ok": true,
                    "result": result,
                }))
                .expect("serialize result"),
            );
            std::process::ExitCode::SUCCESS
        }
        Err(error) => {
            engine.log_error(&format!("sync failed: {error:#}"));
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "ok": false,
                    "error": error.to_string(),
                }))
                .expect("serialize error"),
            );
            std::process::ExitCode::from(1)
        }
    }
}
