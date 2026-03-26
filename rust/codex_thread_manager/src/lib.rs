use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{Local, SecondsFormat, TimeZone, Utc};
use rusqlite::backup::Backup;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use uuid::Uuid;

pub const THREAD_COLUMNS: &[&str] = &[
    "id",
    "rollout_path",
    "created_at",
    "updated_at",
    "source",
    "model_provider",
    "cwd",
    "title",
    "sandbox_policy",
    "approval_mode",
    "tokens_used",
    "has_user_event",
    "archived",
    "archived_at",
    "git_sha",
    "git_branch",
    "git_origin_url",
    "cli_version",
    "first_user_message",
    "agent_nickname",
    "agent_role",
    "memory_mode",
];

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SyncState {
    #[serde(default = "default_state_version")]
    pub version: i64,
    #[serde(default)]
    pub links: Vec<LinkEntry>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LinkEntry {
    #[serde(default)]
    pub providers: BTreeMap<String, String>,
    #[serde(default)]
    pub rollout_paths: BTreeMap<String, String>,
    #[serde(default)]
    pub last_synced_at: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone)]
pub struct ThreadRecord {
    pub id: String,
    pub rollout_path: String,
    pub created_at: i64,
    pub updated_at: i64,
    pub source: String,
    pub model_provider: String,
    pub cwd: String,
    pub title: String,
    pub sandbox_policy: String,
    pub approval_mode: String,
    pub tokens_used: i64,
    pub has_user_event: i64,
    pub archived: i64,
    pub archived_at: Option<i64>,
    pub git_sha: Option<String>,
    pub git_branch: Option<String>,
    pub git_origin_url: Option<String>,
    pub cli_version: String,
    pub first_user_message: String,
    pub agent_nickname: Option<String>,
    pub agent_role: Option<String>,
    pub memory_mode: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct StatusResult {
    pub providers: BTreeMap<String, i64>,
    #[serde(rename = "pair_links")]
    pub pair_links: BTreeMap<String, i64>,
    pub paths: StatusPaths,
}

#[derive(Debug, Clone, Serialize)]
pub struct StatusPaths {
    #[serde(rename = "codex_home")]
    pub codex_home: String,
    #[serde(rename = "state_path")]
    pub state_path: String,
    #[serde(rename = "backup_root")]
    pub backup_root: String,
    #[serde(rename = "log_path")]
    pub log_path: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct MeshStatusResult {
    pub providers: BTreeMap<String, i64>,
    #[serde(rename = "provider_order")]
    pub provider_order: Vec<String>,
    #[serde(rename = "link_count")]
    pub link_count: i64,
    #[serde(rename = "complete_link_count")]
    pub complete_link_count: i64,
    pub paths: StatusPaths,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct OperationSummary {
    pub create: i64,
    pub adopt: i64,
    pub update: i64,
    pub repair: i64,
    pub skip: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct DirectionalResult {
    pub mode: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    pub dry_run: bool,
    pub planned: OperationSummary,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub applied: Option<OperationSummary>,
    #[serde(rename = "backup_dir", skip_serializing_if = "Option::is_none")]
    pub backup_dir: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<BTreeMap<String, i64>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BidirectionalResult {
    pub mode: String,
    pub runs: Vec<DirectionalResult>,
    #[serde(rename = "final_status")]
    pub final_status: StatusResult,
}

#[derive(Debug, Clone, Serialize)]
pub struct MeshSyncResult {
    pub mode: String,
    pub dry_run: bool,
    pub providers: Vec<String>,
    pub planned: OperationSummary,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub applied: Option<OperationSummary>,
    #[serde(rename = "backup_dir", skip_serializing_if = "Option::is_none")]
    pub backup_dir: Option<String>,
    #[serde(rename = "final_status")]
    pub final_status: MeshStatusResult,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct SpaceBucket {
    #[serde(rename = "thread_copies")]
    pub thread_copies: i64,
    pub bytes: i64,
    #[serde(rename = "missing_rollouts")]
    pub missing_rollouts: i64,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct ProviderSpaceUsage {
    #[serde(rename = "thread_copies")]
    pub thread_copies: i64,
    pub bytes: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct SpaceUsageResult {
    pub providers: BTreeMap<String, i64>,
    #[serde(rename = "provider_order")]
    pub provider_order: Vec<String>,
    pub active: SpaceBucket,
    pub archived: SpaceBucket,
    #[serde(rename = "per_provider")]
    pub per_provider: BTreeMap<String, ProviderSpaceUsage>,
    pub paths: StatusPaths,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct CleanupSummary {
    #[serde(rename = "logical_threads")]
    pub logical_threads: i64,
    #[serde(rename = "thread_copies")]
    pub thread_copies: i64,
    pub bytes: i64,
    #[serde(rename = "missing_rollouts")]
    pub missing_rollouts: i64,
    pub providers: BTreeMap<String, i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CleanupResult {
    pub mode: String,
    pub scope: String,
    pub dry_run: bool,
    #[serde(rename = "older_than_days", skip_serializing_if = "Option::is_none")]
    pub older_than_days: Option<i64>,
    #[serde(rename = "keep_latest")]
    pub keep_latest: usize,
    pub planned: CleanupSummary,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub applied: Option<CleanupSummary>,
    #[serde(rename = "backup_dir", skip_serializing_if = "Option::is_none")]
    pub backup_dir: Option<String>,
    #[serde(rename = "final_status")]
    pub final_status: MeshStatusResult,
    #[serde(rename = "final_space")]
    pub final_space: SpaceUsageResult,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum OperationKind {
    Create,
    Adopt,
    Update,
    Repair,
    Skip,
}

#[derive(Debug, Clone)]
struct Operation {
    kind: OperationKind,
    source_thread: ThreadRecord,
    target_thread: Option<ThreadRecord>,
    link_index: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum MeshOperationKind {
    Create,
    Adopt,
    Update,
    Repair,
    Skip,
}

#[derive(Debug, Clone)]
struct MeshOperation {
    kind: MeshOperationKind,
    winner_thread: ThreadRecord,
    target_provider: String,
    target_thread: Option<ThreadRecord>,
    link_index: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ThreadLocator {
    provider: String,
    thread_id: String,
}

#[derive(Debug, Clone)]
struct MeshBootstrapResult {
    state: SyncState,
    adopted: BTreeSet<ThreadLocator>,
    warnings: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CleanupScope {
    Archived,
    Active,
    All,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogicalArchiveState {
    Archived,
    Active,
    Mixed,
}

#[derive(Debug, Clone)]
struct CleanupCandidate {
    winner_thread: ThreadRecord,
    threads: Vec<ThreadRecord>,
}

pub struct CodexSyncEngine {
    codex_home: PathBuf,
    state_path: PathBuf,
    backup_root: PathBuf,
    log_path: PathBuf,
    db_path: PathBuf,
    session_index_path: PathBuf,
    global_state_path: PathBuf,
}

impl ThreadRecord {
    fn fingerprint_key(&self) -> String {
        build_key(vec![
            json!(self.created_at),
            json!(self.updated_at),
            json!(self.source),
            json!(self.cwd),
            json!(self.title),
            json!(self.sandbox_policy),
            json!(self.approval_mode),
            json!(self.tokens_used),
            json!(self.has_user_event),
            json!(self.archived),
            json!(self.archived_at),
            json!(self.git_sha),
            json!(self.git_branch),
            json!(self.git_origin_url),
            json!(self.cli_version),
            json!(self.first_user_message),
            json!(self.agent_nickname),
            json!(self.agent_role),
            json!(self.memory_mode),
        ])
    }

    fn identity_key(&self) -> String {
        build_key(vec![
            json!(self.created_at),
            json!(self.source),
            json!(self.cwd),
            json!(self.title),
            json!(self.sandbox_policy),
            json!(self.approval_mode),
            json!(self.git_branch),
            json!(self.git_origin_url),
            json!(self.cli_version),
            json!(self.first_user_message),
            json!(self.agent_nickname),
            json!(self.agent_role),
            json!(self.memory_mode),
        ])
    }
}

impl CodexSyncEngine {
    pub fn new(
        codex_home: PathBuf,
        state_path: PathBuf,
        backup_root: PathBuf,
        log_path: PathBuf,
    ) -> Self {
        Self {
            db_path: codex_home.join("state_5.sqlite"),
            session_index_path: codex_home.join("session_index.jsonl"),
            global_state_path: codex_home.join(".codex-global-state.json"),
            codex_home,
            state_path,
            backup_root,
            log_path,
        }
    }

    pub fn status(&self, provider_a: &str, provider_b: &str) -> Result<StatusResult> {
        let conn = self.connect()?;
        let counts = self.provider_counts(&conn)?;
        let threads = self.load_threads(&conn)?;
        let state = bootstrap_pair_links(
            self.load_state()?,
            provider_a,
            provider_b,
            threads.get(provider_a),
            threads.get(provider_b),
        );

        let pair_links = state
            .links
            .iter()
            .filter(|link| {
                link.providers.contains_key(provider_a) && link.providers.contains_key(provider_b)
            })
            .count() as i64;

        Ok(StatusResult {
            providers: counts,
            pair_links: BTreeMap::from([(format!("{provider_a}:{provider_b}"), pair_links)]),
            paths: StatusPaths {
                codex_home: self.codex_home.display().to_string(),
                state_path: self.state_path.display().to_string(),
                backup_root: self.backup_root.display().to_string(),
                log_path: self.log_path.display().to_string(),
            },
        })
    }

    pub fn status_all(&self) -> Result<MeshStatusResult> {
        let conn = self.connect()?;
        let counts = self.provider_counts(&conn)?;
        let threads = self.load_threads(&conn)?;
        let active_providers = ordered_active_providers(&counts);
        let bootstrap = self.bootstrap_mesh_links(self.load_state()?, &threads, &active_providers);
        Ok(build_mesh_status_result(
            counts,
            active_providers,
            &bootstrap.state,
            &self.codex_home,
            &self.state_path,
            &self.backup_root,
            &self.log_path,
        ))
    }

    pub fn space(&self) -> Result<SpaceUsageResult> {
        let conn = self.connect()?;
        let counts = self.provider_counts(&conn)?;
        let threads = self.load_threads(&conn)?;
        let active_providers = ordered_active_providers(&counts);
        Ok(build_space_usage_result(
            counts,
            active_providers,
            &threads,
            &self.codex_home,
            &self.state_path,
            &self.backup_root,
            &self.log_path,
        ))
    }

    pub fn cleanup(
        &self,
        scope: &str,
        older_than_days: Option<i64>,
        keep_latest: usize,
        apply: bool,
    ) -> Result<CleanupResult> {
        let scope = parse_cleanup_scope(scope)?;
        if let Some(days) = older_than_days {
            if days < 0 {
                bail!("older_than_days 不能小于 0");
            }
        }
        if apply && older_than_days.is_none() && keep_latest == 0 {
            bail!("实际清理前请至少提供 --older-than-days 或 --keep-latest 来限制范围");
        }

        let conn = self.connect()?;
        let counts = self.provider_counts(&conn)?;
        let threads = self.load_threads(&conn)?;
        let active_providers = ordered_active_providers(&counts);
        let bootstrap = self.bootstrap_mesh_links(self.load_state()?, &threads, &active_providers);
        let candidates = collect_cleanup_candidates(
            &bootstrap.state,
            &threads,
            scope,
            older_than_days,
            keep_latest,
        );
        let planned = summarize_cleanup_candidates(&candidates);
        let current_status = build_mesh_status_result(
            counts.clone(),
            active_providers.clone(),
            &bootstrap.state,
            &self.codex_home,
            &self.state_path,
            &self.backup_root,
            &self.log_path,
        );
        let current_space = build_space_usage_result(
            counts,
            active_providers.clone(),
            &threads,
            &self.codex_home,
            &self.state_path,
            &self.backup_root,
            &self.log_path,
        );

        if !apply {
            return Ok(CleanupResult {
                mode: "cleanup".to_string(),
                scope: cleanup_scope_label(scope).to_string(),
                dry_run: true,
                older_than_days,
                keep_latest,
                planned,
                applied: None,
                backup_dir: None,
                final_status: current_status,
                final_space: current_space,
            });
        }

        let touched_paths = collect_cleanup_backup_paths(&candidates);
        let backup_dir = self.create_backup(
            &format!("cleanup-{}", cleanup_scope_label(scope)),
            &touched_paths,
        )?;
        let mut state = bootstrap.state;
        let applied = self.apply_cleanup(&conn, &mut state, candidates)?;
        self.save_state(&state)?;
        let final_status = self.status_all()?;
        let final_space = self.space()?;

        let result = CleanupResult {
            mode: "cleanup".to_string(),
            scope: cleanup_scope_label(scope).to_string(),
            dry_run: false,
            older_than_days,
            keep_latest,
            planned,
            applied: Some(applied),
            backup_dir: Some(backup_dir.display().to_string()),
            final_status,
            final_space,
        };
        self.log_info(&format!(
            "cleanup finished: {}",
            serde_json::to_string(&result).unwrap_or_else(|_| "\"serialize-error\"".to_string())
        ));
        Ok(result)
    }

    pub fn sync_bidirectional(
        &self,
        provider_a: &str,
        provider_b: &str,
        dry_run: bool,
    ) -> Result<BidirectionalResult> {
        let first = self.sync(provider_a, provider_b, dry_run)?;
        let second = self.sync(provider_b, provider_a, dry_run)?;
        let result = BidirectionalResult {
            mode: "bidirectional".to_string(),
            runs: vec![first, second],
            final_status: self.status(provider_a, provider_b)?,
        };
        self.log_info(&format!(
            "bidirectional sync finished: {}",
            serde_json::to_string(&result).unwrap_or_else(|_| "\"serialize-error\"".to_string())
        ));
        Ok(result)
    }

    pub fn sync_all(&self, dry_run: bool) -> Result<MeshSyncResult> {
        let conn = self.connect()?;
        let counts = self.provider_counts(&conn)?;
        let threads = self.load_threads(&conn)?;
        let active_providers = ordered_active_providers(&counts);

        let bootstrap = self.bootstrap_mesh_links(self.load_state()?, &threads, &active_providers);
        let adopted = bootstrap.adopted.clone();
        let mut state = bootstrap.state;
        let operations = self.plan_mesh_operations(&state, &threads, &active_providers, &adopted);
        let planned = summarize_mesh_operations(&operations);
        let initial_status = build_mesh_status_result(
            counts.clone(),
            active_providers.clone(),
            &state,
            &self.codex_home,
            &self.state_path,
            &self.backup_root,
            &self.log_path,
        );

        if dry_run {
            return Ok(MeshSyncResult {
                mode: "mesh".to_string(),
                dry_run,
                providers: active_providers,
                planned,
                applied: None,
                backup_dir: None,
                final_status: initial_status,
            });
        }

        let touched_paths = collect_mesh_backup_paths(&operations);
        let backup_dir = self.create_backup("sync-all", &touched_paths)?;
        let applied = self.apply_mesh_operations(&conn, &mut state, operations)?;
        self.save_state(&state)?;
        let final_status = self.status_all()?;

        let result = MeshSyncResult {
            mode: "mesh".to_string(),
            dry_run,
            providers: active_providers,
            planned,
            applied: Some(applied),
            backup_dir: Some(backup_dir.display().to_string()),
            final_status,
        };
        self.log_info(&format!(
            "mesh sync finished: {}",
            serde_json::to_string(&result).unwrap_or_else(|_| "\"serialize-error\"".to_string())
        ));
        Ok(result)
    }

    pub fn sync_selected(
        &self,
        requested_providers: &[String],
        dry_run: bool,
    ) -> Result<MeshSyncResult> {
        let conn = self.connect()?;
        let counts = self.provider_counts(&conn)?;
        let threads = self.load_threads(&conn)?;
        let selected_providers = resolve_selected_providers(requested_providers, &counts)?;

        let bootstrap =
            self.bootstrap_mesh_links(self.load_state()?, &threads, &selected_providers);
        let adopted = bootstrap.adopted.clone();
        let mut state = bootstrap.state;
        let operations =
            self.plan_selected_mesh_operations(&state, &threads, &selected_providers, &adopted);
        let planned = summarize_mesh_operations(&operations);
        let current_status = self.status_all()?;

        if dry_run {
            return Ok(MeshSyncResult {
                mode: "mesh-selected".to_string(),
                dry_run,
                providers: selected_providers,
                planned,
                applied: None,
                backup_dir: None,
                final_status: current_status,
            });
        }

        let touched_paths = collect_mesh_backup_paths(&operations);
        let backup_dir = self.create_backup(
            &format!("sync-selected-{}", selected_providers.join("-")),
            &touched_paths,
        )?;
        let applied = self.apply_mesh_operations(&conn, &mut state, operations)?;
        self.save_state(&state)?;
        let final_status = self.status_all()?;

        let result = MeshSyncResult {
            mode: "mesh-selected".to_string(),
            dry_run,
            providers: selected_providers,
            planned,
            applied: Some(applied),
            backup_dir: Some(backup_dir.display().to_string()),
            final_status,
        };
        self.log_info(&format!(
            "selected mesh sync finished: {}",
            serde_json::to_string(&result).unwrap_or_else(|_| "\"serialize-error\"".to_string())
        ));
        Ok(result)
    }

    pub fn sync(
        &self,
        source_provider: &str,
        target_provider: &str,
        dry_run: bool,
    ) -> Result<DirectionalResult> {
        if source_provider == target_provider {
            bail!("source 和 target 不能相同");
        }

        let conn = self.connect()?;
        let threads = self.load_threads(&conn)?;
        let source_threads = threads.get(source_provider).cloned().unwrap_or_default();
        let target_threads = threads.get(target_provider).cloned().unwrap_or_default();

        let state = bootstrap_pair_links(
            self.load_state()?,
            source_provider,
            target_provider,
            Some(&source_threads),
            Some(&target_threads),
        );
        let (operations, mut state) = self.plan_operations(
            state,
            source_provider,
            target_provider,
            &source_threads,
            &target_threads,
        );

        let mut summary = DirectionalResult {
            mode: "directional".to_string(),
            source: Some(source_provider.to_string()),
            target: Some(target_provider.to_string()),
            dry_run,
            planned: summarize_operations(&operations),
            applied: None,
            backup_dir: None,
            status: None,
        };

        if dry_run {
            summary.status = Some(self.provider_counts(&conn)?);
            return Ok(summary);
        }

        let touched_paths = collect_backup_paths(&operations);
        let backup_dir = self.create_backup(
            &format!("{source_provider}-to-{target_provider}"),
            &touched_paths,
        )?;
        let applied = self.apply_operations(
            &conn,
            &mut state,
            source_provider,
            target_provider,
            operations,
        )?;
        summary.backup_dir = Some(backup_dir.display().to_string());
        summary.applied = Some(applied);
        summary.status = Some(self.provider_counts(&conn)?);

        self.save_state(&state)?;
        self.log_info(&format!(
            "sync finished: {}",
            serde_json::to_string(&summary).unwrap_or_else(|_| "\"serialize-error\"".to_string())
        ));
        Ok(summary)
    }

    fn connect(&self) -> Result<Connection> {
        if !self.db_path.exists() {
            bail!("找不到数据库: {}", self.db_path.display());
        }
        let conn = Connection::open(&self.db_path)
            .with_context(|| format!("打开数据库失败: {}", self.db_path.display()))?;
        conn.busy_timeout(std::time::Duration::from_secs(30))?;
        Ok(conn)
    }

    fn load_state(&self) -> Result<SyncState> {
        load_json_file(
            &self.state_path,
            SyncState {
                version: 1,
                links: Vec::new(),
                extra: BTreeMap::new(),
            },
        )
    }

    fn save_state(&self, state: &SyncState) -> Result<()> {
        save_json_pretty(&self.state_path, state)
    }

    fn provider_counts(&self, conn: &Connection) -> Result<BTreeMap<String, i64>> {
        let mut stmt = conn.prepare(
            "select trim(model_provider), count(*) as count \
             from threads \
             where trim(coalesce(model_provider, '')) <> '' \
             group by trim(model_provider) \
             order by trim(model_provider)",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?;
        let mut counts = BTreeMap::new();
        for row in rows {
            let (provider, count) = row?;
            counts.insert(provider, count);
        }
        Ok(counts)
    }

    fn load_threads(
        &self,
        conn: &Connection,
    ) -> Result<BTreeMap<String, BTreeMap<String, ThreadRecord>>> {
        let sql = format!("select {} from threads", THREAD_COLUMNS.join(","));
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map([], map_thread_row)?;
        let mut providers: BTreeMap<String, BTreeMap<String, ThreadRecord>> = BTreeMap::new();
        for row in rows {
            let mut thread = row?;
            thread.model_provider = thread.model_provider.trim().to_string();
            if thread.model_provider.is_empty() {
                continue;
            }
            providers
                .entry(thread.model_provider.clone())
                .or_default()
                .insert(thread.id.clone(), thread);
        }
        Ok(providers)
    }

    fn bootstrap_mesh_links(
        &self,
        state: SyncState,
        threads: &BTreeMap<String, BTreeMap<String, ThreadRecord>>,
        providers: &[String],
    ) -> MeshBootstrapResult {
        let bootstrap = build_mesh_links_state(state, threads, providers);
        for warning in &bootstrap.warnings {
            self.log_warning(&warning);
        }
        bootstrap
    }

    fn plan_mesh_operations(
        &self,
        state: &SyncState,
        threads: &BTreeMap<String, BTreeMap<String, ThreadRecord>>,
        providers: &[String],
        adopted: &BTreeSet<ThreadLocator>,
    ) -> Vec<MeshOperation> {
        let mut operations = Vec::new();

        for (link_index, link) in state.links.iter().enumerate() {
            let Some(winner) = select_authoritative_thread(link, threads) else {
                continue;
            };

            for provider in providers {
                let target_thread = link
                    .providers
                    .get(provider)
                    .and_then(|thread_id| {
                        threads
                            .get(provider)
                            .and_then(|bucket| bucket.get(thread_id))
                    })
                    .cloned();

                let kind = match &target_thread {
                    None => MeshOperationKind::Create,
                    Some(target_thread) => {
                        let rollout_path_mismatch = self
                            .synced_target_thread(&winner, target_thread, provider)
                            .rollout_path
                            != target_thread.rollout_path;
                        if target_thread.id == winner.id
                            && target_thread.model_provider == winner.model_provider
                        {
                            if rollout_path_mismatch {
                                MeshOperationKind::Update
                            } else if !rollout_permissions_match(target_thread) {
                                MeshOperationKind::Repair
                            } else {
                                MeshOperationKind::Skip
                            }
                        } else if adopted.contains(&ThreadLocator {
                            provider: provider.clone(),
                            thread_id: target_thread.id.clone(),
                        }) {
                            MeshOperationKind::Adopt
                        } else if target_thread.fingerprint_key() != winner.fingerprint_key()
                            || rollout_path_mismatch
                        {
                            MeshOperationKind::Update
                        } else if !rollout_permissions_match(target_thread) {
                            MeshOperationKind::Repair
                        } else {
                            MeshOperationKind::Skip
                        }
                    }
                };

                operations.push(MeshOperation {
                    kind,
                    winner_thread: winner.clone(),
                    target_provider: provider.clone(),
                    target_thread,
                    link_index,
                });
            }
        }

        operations
    }

    fn plan_selected_mesh_operations(
        &self,
        state: &SyncState,
        threads: &BTreeMap<String, BTreeMap<String, ThreadRecord>>,
        providers: &[String],
        adopted: &BTreeSet<ThreadLocator>,
    ) -> Vec<MeshOperation> {
        let selected_providers: BTreeSet<_> = providers.iter().cloned().collect();
        let mut operations = Vec::new();

        for (link_index, link) in state.links.iter().enumerate() {
            if !providers
                .iter()
                .any(|provider| link.providers.contains_key(provider))
            {
                continue;
            }
            let Some(winner) =
                select_authoritative_thread_for_providers(link, threads, &selected_providers)
            else {
                continue;
            };

            for provider in providers {
                let target_thread = link
                    .providers
                    .get(provider)
                    .and_then(|thread_id| {
                        threads
                            .get(provider)
                            .and_then(|bucket| bucket.get(thread_id))
                    })
                    .cloned();

                let kind = match &target_thread {
                    None => MeshOperationKind::Create,
                    Some(target_thread) => {
                        let rollout_path_mismatch = self
                            .synced_target_thread(&winner, target_thread, provider)
                            .rollout_path
                            != target_thread.rollout_path;
                        if target_thread.id == winner.id
                            && target_thread.model_provider == winner.model_provider
                        {
                            if rollout_path_mismatch {
                                MeshOperationKind::Update
                            } else if !rollout_permissions_match(target_thread) {
                                MeshOperationKind::Repair
                            } else {
                                MeshOperationKind::Skip
                            }
                        } else if adopted.contains(&ThreadLocator {
                            provider: provider.clone(),
                            thread_id: target_thread.id.clone(),
                        }) {
                            MeshOperationKind::Adopt
                        } else if target_thread.fingerprint_key() != winner.fingerprint_key()
                            || rollout_path_mismatch
                        {
                            MeshOperationKind::Update
                        } else if !rollout_permissions_match(target_thread) {
                            MeshOperationKind::Repair
                        } else {
                            MeshOperationKind::Skip
                        }
                    }
                };

                operations.push(MeshOperation {
                    kind,
                    winner_thread: winner.clone(),
                    target_provider: provider.clone(),
                    target_thread,
                    link_index,
                });
            }
        }

        operations
    }

    fn plan_operations(
        &self,
        mut state: SyncState,
        source_provider: &str,
        target_provider: &str,
        source_threads: &BTreeMap<String, ThreadRecord>,
        target_threads: &BTreeMap<String, ThreadRecord>,
    ) -> (Vec<Operation>, SyncState) {
        let mut entry_by_source: BTreeMap<String, usize> = BTreeMap::new();
        let mut used_target_ids: BTreeSet<String> = BTreeSet::new();

        for (index, link) in state.links.iter().enumerate() {
            if let Some(source_id) = link.providers.get(source_provider) {
                entry_by_source.insert(source_id.clone(), index);
            }
            if let Some(target_id) = link.providers.get(target_provider) {
                used_target_ids.insert(target_id.clone());
            }
        }

        let mut target_by_fingerprint: BTreeMap<String, VecDeque<ThreadRecord>> = BTreeMap::new();
        for thread in target_threads.values() {
            if used_target_ids.contains(&thread.id) {
                continue;
            }
            target_by_fingerprint
                .entry(thread.fingerprint_key())
                .or_default()
                .push_back(thread.clone());
        }
        for candidates in target_by_fingerprint.values_mut() {
            let mut sorted: Vec<_> = candidates.drain(..).collect();
            sorted.sort_by(|left, right| left.id.cmp(&right.id));
            *candidates = VecDeque::from(sorted);
        }

        let mut operations = Vec::new();
        let mut sorted_sources: Vec<_> = source_threads.values().cloned().collect();
        sorted_sources.sort_by(|left, right| {
            left.updated_at
                .cmp(&right.updated_at)
                .then_with(|| left.id.cmp(&right.id))
        });

        for source_thread in sorted_sources {
            let existing_link_index = entry_by_source.get(&source_thread.id).copied();
            let existing_target = existing_link_index
                .and_then(|index| state.links[index].providers.get(target_provider))
                .and_then(|target_id| target_threads.get(target_id))
                .cloned();

            if let Some(target_thread) = existing_target {
                let fingerprint_differs =
                    source_thread.fingerprint_key() != target_thread.fingerprint_key();
                let rollout_path_mismatch = self
                    .synced_target_thread(&source_thread, &target_thread, target_provider)
                    .rollout_path
                    != target_thread.rollout_path;
                let target_needs_repair = !rollout_permissions_match(&target_thread);
                let kind = if source_thread.updated_at > target_thread.updated_at
                    || fingerprint_differs
                    || rollout_path_mismatch
                {
                    OperationKind::Update
                } else if target_needs_repair {
                    OperationKind::Repair
                } else {
                    OperationKind::Skip
                };
                operations.push(Operation {
                    kind,
                    source_thread,
                    target_thread: Some(target_thread),
                    link_index: existing_link_index.expect("existing link index should exist"),
                });
                continue;
            }

            let adopted_target = target_by_fingerprint
                .get_mut(&source_thread.fingerprint_key())
                .and_then(VecDeque::pop_front);

            if let Some(target_thread) = adopted_target {
                let link_index = existing_link_index.unwrap_or_else(|| {
                    state.links.push(LinkEntry::default());
                    state.links.len() - 1
                });
                let link = &mut state.links[link_index];
                link.providers
                    .insert(source_provider.to_string(), source_thread.id.clone());
                link.providers
                    .insert(target_provider.to_string(), target_thread.id.clone());
                link.rollout_paths.insert(
                    source_provider.to_string(),
                    source_thread.rollout_path.clone(),
                );
                link.rollout_paths.insert(
                    target_provider.to_string(),
                    target_thread.rollout_path.clone(),
                );
                entry_by_source.insert(source_thread.id.clone(), link_index);

                operations.push(Operation {
                    kind: OperationKind::Adopt,
                    source_thread,
                    target_thread: Some(target_thread),
                    link_index,
                });
                continue;
            }

            let link_index = existing_link_index.unwrap_or_else(|| {
                state.links.push(LinkEntry::default());
                state.links.len() - 1
            });
            entry_by_source.insert(source_thread.id.clone(), link_index);
            operations.push(Operation {
                kind: OperationKind::Create,
                source_thread,
                target_thread: None,
                link_index,
            });
        }

        (operations, state)
    }

    fn create_backup(&self, label: &str, touched_rollouts: &[PathBuf]) -> Result<PathBuf> {
        let timestamp = Local::now().format("%Y%m%d-%H%M%S").to_string();
        let backup_dir = self.backup_root.join(format!("{timestamp}-{label}"));
        fs::create_dir_all(&backup_dir)
            .with_context(|| format!("创建备份目录失败: {}", backup_dir.display()))?;

        let backup_db_path = backup_dir.join("state_5.sqlite");
        let source_conn = self.connect()?;
        let mut backup_conn = Connection::open(&backup_db_path)?;
        let backup = Backup::new(&source_conn, &mut backup_conn)?;
        backup.step(-1)?;
        drop(backup);

        if self.session_index_path.exists() {
            fs::copy(
                &self.session_index_path,
                backup_dir.join("session_index.jsonl"),
            )?;
        }
        if self.global_state_path.exists() {
            fs::copy(
                &self.global_state_path,
                backup_dir.join(".codex-global-state.json"),
            )?;
        }
        if self.state_path.exists() {
            fs::copy(
                &self.state_path,
                backup_dir.join("provider_sync_state.json"),
            )?;
        }

        let archive_path = backup_dir.join("rollouts.tar.gz");
        let archive_file = fs::File::create(&archive_path)?;
        let encoder = flate2::write::GzEncoder::new(archive_file, flate2::Compression::default());
        let mut archive = tar::Builder::new(encoder);
        for rollout_path in touched_rollouts {
            if rollout_path.exists() {
                let archive_name = relative_archive_path(rollout_path);
                archive.append_path_with_name(rollout_path, archive_name)?;
            }
        }
        archive.finish()?;

        Ok(backup_dir)
    }

    fn apply_operations(
        &self,
        conn: &Connection,
        state: &mut SyncState,
        source_provider: &str,
        target_provider: &str,
        operations: Vec<Operation>,
    ) -> Result<OperationSummary> {
        let mut summary = OperationSummary::default();
        let mut session_index = self.load_session_index()?;
        let mut global_state = self.load_global_state()?;
        let mut created_rollout_paths = Vec::new();

        conn.execute_batch("BEGIN IMMEDIATE")?;
        let result = (|| -> Result<()> {
            for operation in operations {
                match operation.kind {
                    OperationKind::Skip => {
                        summary.skip += 1;
                        continue;
                    }
                    OperationKind::Create => {
                        let target_thread = self.create_target_thread(
                            conn,
                            &operation.source_thread,
                            target_provider,
                        )?;
                        created_rollout_paths.push(PathBuf::from(&target_thread.rollout_path));
                        self.update_link(
                            state,
                            operation.link_index,
                            source_provider,
                            target_provider,
                            &operation.source_thread,
                            &target_thread,
                        );
                        upsert_session_index_entry(&mut session_index, &target_thread)?;
                        copy_workspace_hint(
                            &mut global_state,
                            &operation.source_thread.id,
                            &target_thread.id,
                        );
                        summary.create += 1;
                    }
                    OperationKind::Adopt => {
                        let target_thread = operation
                            .target_thread
                            .as_ref()
                            .context("adopt 缺少 target_thread")?;
                        let synced_thread = self.synced_target_thread(
                            &operation.source_thread,
                            target_thread,
                            target_provider,
                        );
                        self.replace_target_thread(
                            conn,
                            &operation.source_thread,
                            target_thread,
                            target_provider,
                        )?;
                        self.update_link(
                            state,
                            operation.link_index,
                            source_provider,
                            target_provider,
                            &operation.source_thread,
                            &synced_thread,
                        );
                        upsert_session_index_entry(&mut session_index, &synced_thread)?;
                        copy_workspace_hint(
                            &mut global_state,
                            &operation.source_thread.id,
                            &target_thread.id,
                        );
                        summary.adopt += 1;
                    }
                    OperationKind::Update => {
                        let target_thread = operation
                            .target_thread
                            .as_ref()
                            .context("update 缺少 target_thread")?;
                        let synced_thread = self.synced_target_thread(
                            &operation.source_thread,
                            target_thread,
                            target_provider,
                        );
                        self.replace_target_thread(
                            conn,
                            &operation.source_thread,
                            target_thread,
                            target_provider,
                        )?;
                        self.update_link(
                            state,
                            operation.link_index,
                            source_provider,
                            target_provider,
                            &operation.source_thread,
                            &synced_thread,
                        );
                        upsert_session_index_entry(&mut session_index, &synced_thread)?;
                        copy_workspace_hint(
                            &mut global_state,
                            &operation.source_thread.id,
                            &target_thread.id,
                        );
                        summary.update += 1;
                    }
                    OperationKind::Repair => {
                        let target_thread = operation
                            .target_thread
                            .as_ref()
                            .context("repair 缺少 target_thread")?;
                        self.repair_target_rollout(target_thread)?;
                        self.update_link(
                            state,
                            operation.link_index,
                            source_provider,
                            target_provider,
                            &operation.source_thread,
                            target_thread,
                        );
                        upsert_session_index_entry(&mut session_index, target_thread)?;
                        copy_workspace_hint(
                            &mut global_state,
                            &operation.source_thread.id,
                            &target_thread.id,
                        );
                        summary.repair += 1;
                    }
                }
            }
            Ok(())
        })();

        if let Err(error) = result {
            let _ = conn.execute_batch("ROLLBACK");
            for rollout in created_rollout_paths {
                let _ = fs::remove_file(rollout);
            }
            return Err(error);
        }

        conn.execute_batch("COMMIT")?;
        self.save_session_index(&session_index)?;
        self.save_global_state(&global_state)?;
        Ok(summary)
    }

    fn apply_mesh_operations(
        &self,
        conn: &Connection,
        state: &mut SyncState,
        operations: Vec<MeshOperation>,
    ) -> Result<OperationSummary> {
        let mut summary = OperationSummary::default();
        let mut session_index = self.load_session_index()?;
        let mut global_state = self.load_global_state()?;
        let mut created_rollout_paths = Vec::new();

        conn.execute_batch("BEGIN IMMEDIATE")?;
        let result = (|| -> Result<()> {
            for operation in operations {
                match operation.kind {
                    MeshOperationKind::Skip => {
                        summary.skip += 1;
                    }
                    MeshOperationKind::Create => {
                        let target_thread = self.create_target_thread(
                            conn,
                            &operation.winner_thread,
                            &operation.target_provider,
                        )?;
                        created_rollout_paths.push(PathBuf::from(&target_thread.rollout_path));
                        self.upsert_link_thread(
                            state,
                            operation.link_index,
                            &operation.winner_thread,
                        );
                        self.upsert_link_thread(state, operation.link_index, &target_thread);
                        self.touch_link_synced_at(state, operation.link_index);
                        upsert_session_index_entry(&mut session_index, &target_thread)?;
                        copy_workspace_hint(
                            &mut global_state,
                            &operation.winner_thread.id,
                            &target_thread.id,
                        );
                        summary.create += 1;
                    }
                    MeshOperationKind::Adopt => {
                        let target_thread = operation
                            .target_thread
                            .as_ref()
                            .context("mesh adopt 缺少 target_thread")?;
                        let synced_thread = self.synced_target_thread(
                            &operation.winner_thread,
                            target_thread,
                            &operation.target_provider,
                        );
                        self.replace_target_thread(
                            conn,
                            &operation.winner_thread,
                            target_thread,
                            &operation.target_provider,
                        )?;
                        self.upsert_link_thread(
                            state,
                            operation.link_index,
                            &operation.winner_thread,
                        );
                        self.upsert_link_thread(state, operation.link_index, &synced_thread);
                        self.touch_link_synced_at(state, operation.link_index);
                        upsert_session_index_entry(&mut session_index, &synced_thread)?;
                        copy_workspace_hint(
                            &mut global_state,
                            &operation.winner_thread.id,
                            &target_thread.id,
                        );
                        summary.adopt += 1;
                    }
                    MeshOperationKind::Update => {
                        let target_thread = operation
                            .target_thread
                            .as_ref()
                            .context("mesh update 缺少 target_thread")?;
                        let synced_thread = self.synced_target_thread(
                            &operation.winner_thread,
                            target_thread,
                            &operation.target_provider,
                        );
                        self.replace_target_thread(
                            conn,
                            &operation.winner_thread,
                            target_thread,
                            &operation.target_provider,
                        )?;
                        self.upsert_link_thread(
                            state,
                            operation.link_index,
                            &operation.winner_thread,
                        );
                        self.upsert_link_thread(state, operation.link_index, &synced_thread);
                        self.touch_link_synced_at(state, operation.link_index);
                        upsert_session_index_entry(&mut session_index, &synced_thread)?;
                        copy_workspace_hint(
                            &mut global_state,
                            &operation.winner_thread.id,
                            &target_thread.id,
                        );
                        summary.update += 1;
                    }
                    MeshOperationKind::Repair => {
                        let target_thread = operation
                            .target_thread
                            .as_ref()
                            .context("mesh repair 缺少 target_thread")?;
                        self.repair_target_rollout(target_thread)?;
                        self.upsert_link_thread(
                            state,
                            operation.link_index,
                            &operation.winner_thread,
                        );
                        self.upsert_link_thread(state, operation.link_index, target_thread);
                        self.touch_link_synced_at(state, operation.link_index);
                        upsert_session_index_entry(&mut session_index, target_thread)?;
                        copy_workspace_hint(
                            &mut global_state,
                            &operation.winner_thread.id,
                            &target_thread.id,
                        );
                        summary.repair += 1;
                    }
                }
            }
            Ok(())
        })();

        if let Err(error) = result {
            let _ = conn.execute_batch("ROLLBACK");
            for rollout in created_rollout_paths {
                let _ = fs::remove_file(rollout);
            }
            return Err(error);
        }

        conn.execute_batch("COMMIT")?;
        self.save_session_index(&session_index)?;
        self.save_global_state(&global_state)?;
        Ok(summary)
    }

    fn apply_cleanup(
        &self,
        conn: &Connection,
        state: &mut SyncState,
        candidates: Vec<CleanupCandidate>,
    ) -> Result<CleanupSummary> {
        let mut applied = CleanupSummary::default();
        let mut session_index = self.load_session_index()?;
        let mut global_state = self.load_global_state()?;
        let deleted_ids: BTreeSet<String> = candidates
            .iter()
            .flat_map(|candidate| candidate.threads.iter().map(|thread| thread.id.clone()))
            .collect();

        conn.execute_batch("BEGIN IMMEDIATE")?;
        let result = (|| -> Result<()> {
            for candidate in &candidates {
                applied.logical_threads += 1;
                for thread in &candidate.threads {
                    self.delete_thread_records(conn, &thread.id)?;
                    applied.thread_copies += 1;
                    *applied
                        .providers
                        .entry(thread.model_provider.clone())
                        .or_default() += 1;
                }
            }
            Ok(())
        })();

        if let Err(error) = result {
            let _ = conn.execute_batch("ROLLBACK");
            return Err(error);
        }

        conn.execute_batch("COMMIT")?;

        prune_state_thread_ids(state, &deleted_ids);
        remove_session_index_entries(&mut session_index, &deleted_ids);
        remove_thread_ids_from_global_state(&mut global_state, &deleted_ids);
        self.save_session_index(&session_index)?;
        self.save_global_state(&global_state)?;

        for candidate in candidates {
            for thread in candidate.threads {
                match remove_rollout_file(Path::new(&thread.rollout_path), &self.codex_home) {
                    Ok(Some(size)) => {
                        applied.bytes += size;
                    }
                    Ok(None) => {
                        applied.missing_rollouts += 1;
                    }
                    Err(error) => {
                        self.log_warning(&format!(
                            "删除 rollout 失败: {} ({error:#})",
                            thread.rollout_path
                        ));
                    }
                }
            }
        }

        Ok(applied)
    }

    fn delete_thread_records(&self, conn: &Connection, thread_id: &str) -> Result<()> {
        if table_exists(conn, "agent_job_items")? {
            conn.execute(
                "update agent_job_items set assigned_thread_id=NULL where assigned_thread_id=?",
                [thread_id],
            )?;
        }
        if table_exists(conn, "logs")? {
            conn.execute("delete from logs where thread_id=?", [thread_id])?;
        }
        if table_exists(conn, "thread_dynamic_tools")? {
            conn.execute(
                "delete from thread_dynamic_tools where thread_id=?",
                [thread_id],
            )?;
        }
        if table_exists(conn, "stage1_outputs")? {
            conn.execute("delete from stage1_outputs where thread_id=?", [thread_id])?;
        }
        conn.execute("delete from threads where id=?", [thread_id])?;
        Ok(())
    }

    fn update_link(
        &self,
        state: &mut SyncState,
        link_index: usize,
        source_provider: &str,
        target_provider: &str,
        source_thread: &ThreadRecord,
        target_thread: &ThreadRecord,
    ) {
        let _ = source_provider;
        let _ = target_provider;
        self.upsert_link_thread(state, link_index, source_thread);
        self.upsert_link_thread(state, link_index, target_thread);
        self.touch_link_synced_at(state, link_index);
    }

    fn upsert_link_thread(&self, state: &mut SyncState, link_index: usize, thread: &ThreadRecord) {
        let link = &mut state.links[link_index];
        link.providers
            .insert(thread.model_provider.clone(), thread.id.clone());
        link.rollout_paths
            .insert(thread.model_provider.clone(), thread.rollout_path.clone());
    }

    fn touch_link_synced_at(&self, state: &mut SyncState, link_index: usize) {
        state.links[link_index].last_synced_at = Some(utc_now_iso());
    }

    fn synced_target_thread(
        &self,
        source_thread: &ThreadRecord,
        target_thread: &ThreadRecord,
        target_provider: &str,
    ) -> ThreadRecord {
        synced_target_thread(
            &self.codex_home,
            source_thread,
            target_thread,
            target_provider,
        )
    }

    fn create_target_thread(
        &self,
        conn: &Connection,
        source_thread: &ThreadRecord,
        target_provider: &str,
    ) -> Result<ThreadRecord> {
        let source_id = source_thread.id.clone();
        let target_id = Uuid::new_v4().to_string();
        let target_path =
            normalized_target_rollout_path(&self.codex_home, source_thread, &source_id, &target_id);
        if target_path.exists() {
            bail!("目标 rollout 已存在: {}", target_path.display());
        }

        let mut target_thread = source_thread.clone();
        target_thread.id = target_id.clone();
        target_thread.model_provider = target_provider.to_string();
        target_thread.rollout_path = target_path.display().to_string();

        let source_rollout = fs::read_to_string(&source_thread.rollout_path)
            .with_context(|| format!("读取 rollout 失败: {}", source_thread.rollout_path))?;
        let rewritten = rewrite_rollout_text(
            &source_rollout,
            &target_id,
            target_provider,
            &source_thread.sandbox_policy,
            &source_thread.approval_mode,
        )?;
        atomic_write_text(&target_path, &rewritten)?;

        let placeholders = vec!["?"; THREAD_COLUMNS.len()].join(",");
        let sql = format!(
            "insert into threads ({}) values ({})",
            THREAD_COLUMNS.join(","),
            placeholders
        );
        conn.execute(
            &sql,
            params![
                target_thread.id,
                target_thread.rollout_path,
                target_thread.created_at,
                target_thread.updated_at,
                target_thread.source,
                target_thread.model_provider,
                target_thread.cwd,
                target_thread.title,
                target_thread.sandbox_policy,
                target_thread.approval_mode,
                target_thread.tokens_used,
                target_thread.has_user_event,
                target_thread.archived,
                target_thread.archived_at,
                target_thread.git_sha,
                target_thread.git_branch,
                target_thread.git_origin_url,
                target_thread.cli_version,
                target_thread.first_user_message,
                target_thread.agent_nickname,
                target_thread.agent_role,
                target_thread.memory_mode,
            ],
        )?;

        self.replace_auxiliary_tables(conn, &source_id, &target_id)?;
        Ok(target_thread)
    }

    fn replace_target_thread(
        &self,
        conn: &Connection,
        source_thread: &ThreadRecord,
        target_thread: &ThreadRecord,
        target_provider: &str,
    ) -> Result<()> {
        let synced_thread =
            self.synced_target_thread(source_thread, target_thread, target_provider);
        let current_target_path = PathBuf::from(&target_thread.rollout_path);
        let target_path = PathBuf::from(&synced_thread.rollout_path);
        let source_rollout = fs::read_to_string(&source_thread.rollout_path)
            .with_context(|| format!("读取 rollout 失败: {}", source_thread.rollout_path))?;
        let rewritten = rewrite_rollout_text(
            &source_rollout,
            &target_thread.id,
            target_provider,
            &source_thread.sandbox_policy,
            &source_thread.approval_mode,
        )?;
        atomic_write_text(&target_path, &rewritten)?;
        if current_target_path != target_path && current_target_path.exists() {
            fs::remove_file(&current_target_path).with_context(|| {
                format!("删除旧 rollout 失败: {}", current_target_path.display())
            })?;
        }

        conn.execute(
            "update threads set rollout_path=?, created_at=?, updated_at=?, source=?, cwd=?, title=?, sandbox_policy=?, approval_mode=?, tokens_used=?, has_user_event=?, archived=?, archived_at=?, git_sha=?, git_branch=?, git_origin_url=?, cli_version=?, first_user_message=?, agent_nickname=?, agent_role=?, memory_mode=? where id=?",
            params![
                synced_thread.rollout_path,
                source_thread.created_at,
                source_thread.updated_at,
                source_thread.source,
                source_thread.cwd,
                source_thread.title,
                source_thread.sandbox_policy,
                source_thread.approval_mode,
                source_thread.tokens_used,
                source_thread.has_user_event,
                source_thread.archived,
                source_thread.archived_at,
                source_thread.git_sha,
                source_thread.git_branch,
                source_thread.git_origin_url,
                source_thread.cli_version,
                source_thread.first_user_message,
                source_thread.agent_nickname,
                source_thread.agent_role,
                source_thread.memory_mode,
                target_thread.id,
            ],
        )?;

        if source_thread.id != target_thread.id {
            self.replace_auxiliary_tables(conn, &source_thread.id, &target_thread.id)?;
        }
        Ok(())
    }

    fn repair_target_rollout(&self, target_thread: &ThreadRecord) -> Result<()> {
        let target_path = PathBuf::from(&target_thread.rollout_path);
        let current_rollout = fs::read_to_string(&target_path)
            .with_context(|| format!("读取 rollout 失败: {}", target_path.display()))?;
        let rewritten = rewrite_rollout_text(
            &current_rollout,
            &target_thread.id,
            &target_thread.model_provider,
            &target_thread.sandbox_policy,
            &target_thread.approval_mode,
        )?;
        if rewritten != current_rollout {
            atomic_write_text(&target_path, &rewritten)?;
        }
        Ok(())
    }

    fn replace_auxiliary_tables(
        &self,
        conn: &Connection,
        source_id: &str,
        target_id: &str,
    ) -> Result<()> {
        conn.execute(
            "delete from thread_dynamic_tools where thread_id=?",
            [target_id],
        )?;
        conn.execute(
            "insert into thread_dynamic_tools (thread_id, position, name, description, input_schema, defer_loading) \
             select ?, position, name, description, input_schema, defer_loading from thread_dynamic_tools where thread_id=?",
            params![target_id, source_id],
        )?;

        conn.execute("delete from stage1_outputs where thread_id=?", [target_id])?;
        conn.execute(
            "insert into stage1_outputs (thread_id, source_updated_at, raw_memory, rollout_summary, generated_at, rollout_slug, usage_count, last_usage, selected_for_phase2, selected_for_phase2_source_updated_at) \
             select ?, source_updated_at, raw_memory, rollout_summary, generated_at, rollout_slug, usage_count, last_usage, selected_for_phase2, selected_for_phase2_source_updated_at from stage1_outputs where thread_id=?",
            params![target_id, source_id],
        )?;

        conn.execute("delete from logs where thread_id=?", [target_id])?;
        conn.execute(
            "insert into logs (ts, ts_nanos, level, target, message, module_path, file, line, thread_id, process_uuid, estimated_bytes) \
             select ts, ts_nanos, level, target, message, module_path, file, line, ?, process_uuid, estimated_bytes from logs where thread_id=?",
            params![target_id, source_id],
        )?;

        Ok(())
    }

    fn load_session_index(&self) -> Result<Vec<Value>> {
        if !self.session_index_path.exists() {
            return Ok(Vec::new());
        }
        let content = fs::read_to_string(&self.session_index_path).with_context(|| {
            format!(
                "读取 session_index 失败: {}",
                self.session_index_path.display()
            )
        })?;
        let mut entries = Vec::new();
        for line in content.lines() {
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<Value>(line) {
                Ok(value) => entries.push(value),
                Err(_) => self.log_warning("skip invalid session index line"),
            }
        }
        Ok(entries)
    }

    fn save_session_index(&self, entries: &[Value]) -> Result<()> {
        let mut lines = Vec::new();
        for entry in entries {
            lines.push(serde_json::to_string(entry)?);
        }
        let mut payload = lines.join("\n");
        if !payload.is_empty() {
            payload.push('\n');
        }
        atomic_write_text(&self.session_index_path, &payload)
    }

    fn load_global_state(&self) -> Result<Value> {
        load_json_file(&self.global_state_path, json!({}))
    }

    fn save_global_state(&self, state: &Value) -> Result<()> {
        let Some(map) = state.as_object() else {
            return Ok(());
        };
        if map.is_empty() {
            return atomic_write_text(&self.global_state_path, "{}\n");
        }
        atomic_write_text(&self.global_state_path, &serde_json::to_string(state)?)
    }

    pub fn log_info(&self, message: &str) {
        append_log(&self.log_path, "INFO", message);
    }

    pub fn log_warning(&self, message: &str) {
        append_log(&self.log_path, "WARNING", message);
    }

    pub fn log_error(&self, message: &str) {
        append_log(&self.log_path, "ERROR", message);
    }
}

pub fn default_codex_home() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home).join(".codex")
}

pub fn default_state_path() -> PathBuf {
    default_codex_home().join("provider_sync_state.json")
}

pub fn default_log_path() -> PathBuf {
    default_codex_home().join("provider_sync.log")
}

pub fn default_backup_root() -> PathBuf {
    default_codex_home().join("provider_sync_backups")
}

pub fn rewrite_rollout_text(
    content: &str,
    new_thread_id: &str,
    provider: &str,
    sandbox_policy_json: &str,
    approval_mode: &str,
) -> Result<String> {
    let mut lines: Vec<String> = content
        .split_inclusive('\n')
        .map(ToOwned::to_owned)
        .collect();
    if lines.is_empty() && !content.is_empty() {
        lines.push(content.to_string());
    }
    if lines.is_empty() {
        bail!("rollout 文件为空");
    }

    let first_line = lines[0].trim_end_matches('\n');
    let mut session_meta: Value =
        serde_json::from_str(first_line).context("rollout 首行不是合法的 session_meta")?;
    let sandbox_policy_value: Value =
        serde_json::from_str(sandbox_policy_json).context("sandbox_policy 不是合法 JSON")?;
    if session_meta.get("type").and_then(Value::as_str) != Some("session_meta") {
        bail!("rollout 首行不是合法的 session_meta");
    }
    let payload = session_meta
        .get_mut("payload")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| anyhow!("rollout 首行不是合法的 session_meta"))?;

    payload.insert("id".to_string(), Value::String(new_thread_id.to_string()));
    payload.insert(
        "model_provider".to_string(),
        Value::String(provider.to_string()),
    );

    let newline = if lines[0].ends_with('\n') { "\n" } else { "" };
    lines[0] = format!("{}{}", serde_json::to_string(&session_meta)?, newline);

    let mut last_turn_context_index = None;
    for (index, line) in lines.iter().enumerate().skip(1) {
        let trimmed = line.trim_end_matches('\n');
        let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
            continue;
        };
        if value.get("type").and_then(Value::as_str) == Some("turn_context") {
            last_turn_context_index = Some(index);
        }
    }

    if let Some(index) = last_turn_context_index {
        let newline = if lines[index].ends_with('\n') {
            "\n"
        } else {
            ""
        };
        let trimmed = lines[index].trim_end_matches('\n');
        let mut turn_context: Value =
            serde_json::from_str(trimmed).context("turn_context 不是合法 JSON")?;
        let payload = turn_context
            .get_mut("payload")
            .and_then(Value::as_object_mut)
            .ok_or_else(|| anyhow!("turn_context payload 非法"))?;
        payload.insert(
            "approval_policy".to_string(),
            Value::String(approval_mode.to_string()),
        );
        payload.insert("sandbox_policy".to_string(), sandbox_policy_value);
        lines[index] = format!("{}{}", serde_json::to_string(&turn_context)?, newline);
    }

    Ok(lines.concat())
}

pub fn make_target_rollout_path(
    source_rollout_path: &str,
    source_thread_id: &str,
    target_thread_id: &str,
) -> PathBuf {
    let source_path = Path::new(source_rollout_path);
    let stem = source_path
        .file_stem()
        .map(|value| value.to_string_lossy().to_string())
        .unwrap_or_else(|| source_thread_id.to_string());
    let extension = source_path
        .extension()
        .map(|value| format!(".{}", value.to_string_lossy()))
        .unwrap_or_default();
    let new_stem = if stem.ends_with(source_thread_id) {
        format!(
            "{}{}",
            &stem[..stem.len() - source_thread_id.len()],
            target_thread_id
        )
    } else {
        format!("{stem}-{target_thread_id}")
    };
    source_path.with_file_name(format!("{new_stem}{extension}"))
}

fn normalized_target_rollout_path(
    codex_home: &Path,
    source_thread: &ThreadRecord,
    source_thread_id: &str,
    target_thread_id: &str,
) -> PathBuf {
    let swapped_path = make_target_rollout_path(
        &source_thread.rollout_path,
        source_thread_id,
        target_thread_id,
    );
    let file_name = swapped_path
        .file_name()
        .map(|value| value.to_os_string())
        .or_else(|| {
            Path::new(&source_thread.rollout_path)
                .file_name()
                .map(|value| value.to_os_string())
        })
        .unwrap_or_else(|| std::ffi::OsString::from(format!("rollout-{target_thread_id}.jsonl")));

    if source_thread.archived != 0 {
        return codex_home.join("archived_sessions").join(file_name);
    }

    if let Some((year, month, day)) = rollout_date_parts_from_filename(Path::new(&file_name)) {
        return codex_home
            .join("sessions")
            .join(year)
            .join(month)
            .join(day)
            .join(file_name);
    }

    swapped_path
}

fn rollout_date_parts_from_filename(path: &Path) -> Option<(String, String, String)> {
    let file_name = path.file_name()?.to_string_lossy();
    let file_name = file_name.strip_prefix("rollout-")?;
    let date = file_name.get(0..10)?;
    let mut parts = date.split('-');
    let year = parts.next()?.to_string();
    let month = parts.next()?.to_string();
    let day = parts.next()?.to_string();
    Some((year, month, day))
}

fn ordered_active_providers(counts: &BTreeMap<String, i64>) -> Vec<String> {
    let mut providers: Vec<_> = counts
        .iter()
        .filter(|(provider, count)| !provider.trim().is_empty() && **count > 0)
        .map(|(provider, _)| provider.clone())
        .collect();
    providers.sort_by(|left, right| preferred_provider_order(left, right));
    providers
}

fn resolve_selected_providers(
    requested_providers: &[String],
    counts: &BTreeMap<String, i64>,
) -> Result<Vec<String>> {
    let active_providers = ordered_active_providers(counts);
    let mut requested = BTreeSet::new();
    for provider in requested_providers {
        let trimmed = provider.trim();
        if trimmed.is_empty() {
            continue;
        }
        requested.insert(trimmed.to_string());
    }

    if requested.len() < 2 {
        bail!("至少选择 2 个 provider");
    }

    let selected: Vec<_> = active_providers
        .into_iter()
        .filter(|provider| requested.contains(provider))
        .collect();
    let selected_set: BTreeSet<_> = selected.iter().cloned().collect();
    let missing: Vec<_> = requested.difference(&selected_set).cloned().collect();

    if !missing.is_empty() {
        bail!("找不到有效 provider: {}", missing.join(", "));
    }

    if selected.len() < 2 {
        bail!("至少选择 2 个有线程数据的 provider");
    }

    Ok(selected)
}

fn build_mesh_status_result(
    counts: BTreeMap<String, i64>,
    active_providers: Vec<String>,
    state: &SyncState,
    codex_home: &Path,
    state_path: &Path,
    backup_root: &Path,
    log_path: &Path,
) -> MeshStatusResult {
    let link_count = state.links.len() as i64;
    let complete_link_count = if active_providers.is_empty() {
        0
    } else {
        state
            .links
            .iter()
            .filter(|link| {
                active_providers
                    .iter()
                    .all(|provider| link.providers.contains_key(provider))
            })
            .count() as i64
    };

    MeshStatusResult {
        providers: counts,
        provider_order: active_providers,
        link_count,
        complete_link_count,
        paths: StatusPaths {
            codex_home: codex_home.display().to_string(),
            state_path: state_path.display().to_string(),
            backup_root: backup_root.display().to_string(),
            log_path: log_path.display().to_string(),
        },
    }
}

fn build_space_usage_result(
    counts: BTreeMap<String, i64>,
    active_providers: Vec<String>,
    threads: &BTreeMap<String, BTreeMap<String, ThreadRecord>>,
    codex_home: &Path,
    state_path: &Path,
    backup_root: &Path,
    log_path: &Path,
) -> SpaceUsageResult {
    let mut active = SpaceBucket::default();
    let mut archived = SpaceBucket::default();
    let mut per_provider = BTreeMap::new();

    for bucket in threads.values() {
        for thread in bucket.values() {
            let size = rollout_file_size(Path::new(&thread.rollout_path)).unwrap_or(None);
            let bucket = if thread.archived != 0 {
                &mut archived
            } else {
                &mut active
            };
            bucket.thread_copies += 1;
            match size {
                Some(bytes) => bucket.bytes += bytes,
                None => bucket.missing_rollouts += 1,
            }

            let provider_entry = per_provider
                .entry(thread.model_provider.clone())
                .or_insert_with(ProviderSpaceUsage::default);
            provider_entry.thread_copies += 1;
            if let Some(bytes) = size {
                provider_entry.bytes += bytes;
            }
        }
    }

    SpaceUsageResult {
        providers: counts,
        provider_order: active_providers,
        active,
        archived,
        per_provider,
        paths: StatusPaths {
            codex_home: codex_home.display().to_string(),
            state_path: state_path.display().to_string(),
            backup_root: backup_root.display().to_string(),
            log_path: log_path.display().to_string(),
        },
    }
}

fn parse_cleanup_scope(scope: &str) -> Result<CleanupScope> {
    match scope {
        "archived" => Ok(CleanupScope::Archived),
        "active" => Ok(CleanupScope::Active),
        "all" => Ok(CleanupScope::All),
        other => bail!("未知 cleanup scope: {other}"),
    }
}

fn cleanup_scope_label(scope: CleanupScope) -> &'static str {
    match scope {
        CleanupScope::Archived => "archived",
        CleanupScope::Active => "active",
        CleanupScope::All => "all",
    }
}

fn collect_cleanup_candidates(
    state: &SyncState,
    threads: &BTreeMap<String, BTreeMap<String, ThreadRecord>>,
    scope: CleanupScope,
    older_than_days: Option<i64>,
    keep_latest: usize,
) -> Vec<CleanupCandidate> {
    let cutoff = older_than_days.map(|days| Utc::now().timestamp_millis() - days * 86_400_000);
    let mut candidates = Vec::new();

    for link in &state.links {
        let Some(winner_thread) = select_authoritative_thread(link, threads) else {
            continue;
        };
        let link_threads: Vec<_> = link
            .providers
            .iter()
            .filter_map(|(provider, thread_id)| {
                threads
                    .get(provider)
                    .and_then(|bucket| bucket.get(thread_id))
                    .cloned()
            })
            .collect();
        if link_threads.is_empty() {
            continue;
        }
        if !cleanup_scope_matches(scope, logical_archive_state(&link_threads)) {
            continue;
        }
        if let Some(cutoff) = cutoff {
            if winner_thread.updated_at >= cutoff {
                continue;
            }
        }
        candidates.push(CleanupCandidate {
            winner_thread,
            threads: link_threads,
        });
    }

    candidates.sort_by(|left, right| {
        right
            .winner_thread
            .updated_at
            .cmp(&left.winner_thread.updated_at)
            .then_with(|| {
                preferred_provider_order(
                    &left.winner_thread.model_provider,
                    &right.winner_thread.model_provider,
                )
            })
            .then_with(|| left.winner_thread.id.cmp(&right.winner_thread.id))
    });

    candidates.into_iter().skip(keep_latest).collect()
}

fn cleanup_scope_matches(scope: CleanupScope, state: LogicalArchiveState) -> bool {
    match scope {
        CleanupScope::Archived => state == LogicalArchiveState::Archived,
        CleanupScope::Active => state == LogicalArchiveState::Active,
        CleanupScope::All => true,
    }
}

fn logical_archive_state(threads: &[ThreadRecord]) -> LogicalArchiveState {
    let archived_count = threads.iter().filter(|thread| thread.archived != 0).count();
    if archived_count == 0 {
        LogicalArchiveState::Active
    } else if archived_count == threads.len() {
        LogicalArchiveState::Archived
    } else {
        LogicalArchiveState::Mixed
    }
}

fn summarize_cleanup_candidates(candidates: &[CleanupCandidate]) -> CleanupSummary {
    let mut summary = CleanupSummary::default();
    for candidate in candidates {
        summary.logical_threads += 1;
        for thread in &candidate.threads {
            summary.thread_copies += 1;
            *summary
                .providers
                .entry(thread.model_provider.clone())
                .or_default() += 1;
            match rollout_file_size(Path::new(&thread.rollout_path)).unwrap_or(None) {
                Some(bytes) => summary.bytes += bytes,
                None => summary.missing_rollouts += 1,
            }
        }
    }
    summary
}

fn collect_cleanup_backup_paths(candidates: &[CleanupCandidate]) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    for candidate in candidates {
        for thread in &candidate.threads {
            paths.push(PathBuf::from(&thread.rollout_path));
        }
    }
    paths
}

fn prune_state_thread_ids(state: &mut SyncState, deleted_ids: &BTreeSet<String>) {
    for link in &mut state.links {
        let removed_providers: Vec<_> = link
            .providers
            .iter()
            .filter(|(_, thread_id)| deleted_ids.contains(*thread_id))
            .map(|(provider, _)| provider.clone())
            .collect();
        for provider in removed_providers {
            link.providers.remove(&provider);
            link.rollout_paths.remove(&provider);
        }
    }
    state.links.retain(|link| !link.providers.is_empty());
}

fn remove_session_index_entries(entries: &mut Vec<Value>, deleted_ids: &BTreeSet<String>) {
    entries.retain(|entry| {
        let thread_id = entry.get("id").and_then(Value::as_str);
        !thread_id.is_some_and(|value| deleted_ids.contains(value))
    });
}

fn remove_thread_ids_from_global_state(state: &mut Value, deleted_ids: &BTreeSet<String>) {
    match state {
        Value::Object(map) => {
            let keys_to_remove: Vec<_> = map
                .keys()
                .filter(|key| deleted_ids.contains(key.as_str()))
                .cloned()
                .collect();
            for key in keys_to_remove {
                map.remove(&key);
            }
            for value in map.values_mut() {
                remove_thread_ids_from_global_state(value, deleted_ids);
            }
            let empty_keys: Vec<_> = map
                .iter()
                .filter(|(_, value)| value_is_empty_container(value))
                .map(|(key, _)| key.clone())
                .collect();
            for key in empty_keys {
                map.remove(&key);
            }
        }
        Value::Array(items) => {
            items.retain(
                |item| !matches!(item, Value::String(value) if deleted_ids.contains(value)),
            );
            for item in items.iter_mut() {
                remove_thread_ids_from_global_state(item, deleted_ids);
            }
            items.retain(|item| !value_is_empty_container(item));
        }
        _ => {}
    }
}

fn value_is_empty_container(value: &Value) -> bool {
    match value {
        Value::Object(map) => map.is_empty(),
        Value::Array(items) => items.is_empty(),
        _ => false,
    }
}

fn table_exists(conn: &Connection, table_name: &str) -> Result<bool> {
    let exists = conn.query_row(
        "select exists(select 1 from sqlite_master where type='table' and name=?)",
        [table_name],
        |row| row.get::<_, i64>(0),
    )?;
    Ok(exists != 0)
}

fn rollout_file_size(path: &Path) -> Result<Option<i64>> {
    match fs::metadata(path) {
        Ok(metadata) => Ok(Some(metadata.len() as i64)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => {
            Err(error).with_context(|| format!("读取 rollout 大小失败: {}", path.display()))
        }
    }
}

fn remove_rollout_file(path: &Path, codex_home: &Path) -> Result<Option<i64>> {
    let size = rollout_file_size(path)?;
    let Some(size) = size else {
        return Ok(None);
    };
    fs::remove_file(path).with_context(|| format!("删除 rollout 失败: {}", path.display()))?;
    prune_empty_rollout_dirs(path, codex_home);
    Ok(Some(size))
}

fn prune_empty_rollout_dirs(path: &Path, codex_home: &Path) {
    let roots = [
        codex_home.join("sessions"),
        codex_home.join("archived_sessions"),
    ];
    let mut current = path.parent().map(Path::to_path_buf);
    while let Some(dir) = current {
        if roots.iter().any(|root| root == &dir) {
            break;
        }
        match fs::remove_dir(&dir) {
            Ok(()) => {
                current = dir.parent().map(Path::to_path_buf);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                current = dir.parent().map(Path::to_path_buf);
            }
            Err(error) if error.kind() == std::io::ErrorKind::DirectoryNotEmpty => break,
            Err(_) => break,
        }
    }
}

fn provider_sort_key(provider: &str) -> (u8, &str) {
    match provider {
        "openai" => (0, ""),
        "cpa" => (1, ""),
        other => (2, other),
    }
}

fn preferred_provider_order(left: &str, right: &str) -> Ordering {
    provider_sort_key(left).cmp(&provider_sort_key(right))
}

fn authoritative_provider_order(left: &str, right: &str) -> Ordering {
    match preferred_provider_order(left, right) {
        Ordering::Less => Ordering::Greater,
        Ordering::Greater => Ordering::Less,
        Ordering::Equal => Ordering::Equal,
    }
}

fn stable_id_preference(left: &str, right: &str) -> Ordering {
    match left.cmp(right) {
        Ordering::Less => Ordering::Greater,
        Ordering::Greater => Ordering::Less,
        Ordering::Equal => Ordering::Equal,
    }
}

fn compare_thread_authority(left: &ThreadRecord, right: &ThreadRecord) -> Ordering {
    left.updated_at
        .cmp(&right.updated_at)
        .then_with(|| authoritative_provider_order(&left.model_provider, &right.model_provider))
        .then_with(|| stable_id_preference(&left.id, &right.id))
}

fn select_authoritative_thread(
    link: &LinkEntry,
    threads: &BTreeMap<String, BTreeMap<String, ThreadRecord>>,
) -> Option<ThreadRecord> {
    link.providers
        .iter()
        .filter_map(|(provider, thread_id)| {
            threads
                .get(provider)
                .and_then(|bucket| bucket.get(thread_id))
        })
        .cloned()
        .max_by(compare_thread_authority)
}

fn select_authoritative_thread_for_providers(
    link: &LinkEntry,
    threads: &BTreeMap<String, BTreeMap<String, ThreadRecord>>,
    selected_providers: &BTreeSet<String>,
) -> Option<ThreadRecord> {
    link.providers
        .iter()
        .filter(|(provider, _)| selected_providers.contains(*provider))
        .filter_map(|(provider, thread_id)| {
            threads
                .get(provider)
                .and_then(|bucket| bucket.get(thread_id))
        })
        .cloned()
        .max_by(compare_thread_authority)
}

fn build_mesh_links_state(
    mut state: SyncState,
    threads: &BTreeMap<String, BTreeMap<String, ThreadRecord>>,
    providers: &[String],
) -> MeshBootstrapResult {
    if state.version == 0 {
        state.version = 1;
    }

    let mut groups: Vec<Option<LinkEntry>> = Vec::new();
    let mut assignments: BTreeMap<String, usize> = BTreeMap::new();
    let mut adopted = BTreeSet::new();
    let mut warnings = Vec::new();

    for link in state.links {
        let cleaned = clean_mesh_link(link, threads);
        if cleaned.providers.is_empty() {
            continue;
        }
        merge_link_group(
            &mut groups,
            &mut assignments,
            cleaned,
            &mut adopted,
            &mut warnings,
            false,
            "已有映射",
        );
    }

    for (index, provider_a) in providers.iter().enumerate() {
        let Some(provider_a_threads) = threads.get(provider_a) else {
            continue;
        };
        for provider_b in providers.iter().skip(index + 1) {
            let Some(provider_b_threads) = threads.get(provider_b) else {
                continue;
            };
            for link in collect_pair_candidate_links(
                provider_a,
                provider_b,
                provider_a_threads,
                provider_b_threads,
            ) {
                merge_link_group(
                    &mut groups,
                    &mut assignments,
                    link,
                    &mut adopted,
                    &mut warnings,
                    true,
                    "自动匹配",
                );
            }
        }
    }

    for provider in providers {
        let Some(provider_threads) = threads.get(provider) else {
            continue;
        };
        for thread in provider_threads.values() {
            let key = assignment_key(provider, &thread.id);
            if assignments.contains_key(&key) {
                continue;
            }
            merge_link_group(
                &mut groups,
                &mut assignments,
                singleton_link(thread),
                &mut adopted,
                &mut warnings,
                false,
                "补齐单边线程",
            );
        }
    }

    let mut links: Vec<_> = groups.into_iter().flatten().collect();
    links.sort_by(|left, right| mesh_link_sort_key(left).cmp(&mesh_link_sort_key(right)));
    state.links = links;
    MeshBootstrapResult {
        state,
        adopted,
        warnings,
    }
}

fn clean_mesh_link(
    link: LinkEntry,
    threads: &BTreeMap<String, BTreeMap<String, ThreadRecord>>,
) -> LinkEntry {
    let mut providers = BTreeMap::new();
    let mut rollout_paths = BTreeMap::new();

    for (provider, thread_id) in link.providers {
        let Some(thread) = threads
            .get(&provider)
            .and_then(|bucket| bucket.get(&thread_id))
        else {
            continue;
        };
        providers.insert(provider.clone(), thread_id);
        rollout_paths.insert(provider, thread.rollout_path.clone());
    }

    LinkEntry {
        providers,
        rollout_paths,
        last_synced_at: link.last_synced_at,
        extra: link.extra,
    }
}

fn collect_pair_candidate_links(
    provider_a: &str,
    provider_b: &str,
    provider_a_threads: &BTreeMap<String, ThreadRecord>,
    provider_b_threads: &BTreeMap<String, ThreadRecord>,
) -> Vec<LinkEntry> {
    let mut cleaned_links = Vec::new();
    let mut used_a = BTreeSet::new();
    let mut used_b = BTreeSet::new();

    match_unlinked_threads(
        &mut cleaned_links,
        &mut used_a,
        &mut used_b,
        provider_a,
        provider_b,
        provider_a_threads,
        provider_b_threads,
        |thread| thread.fingerprint_key(),
    );
    match_unlinked_threads(
        &mut cleaned_links,
        &mut used_a,
        &mut used_b,
        provider_a,
        provider_b,
        provider_a_threads,
        provider_b_threads,
        |thread| thread.identity_key(),
    );
    match_unlinked_threads_by_signature(
        &mut cleaned_links,
        &mut used_a,
        &mut used_b,
        provider_a,
        provider_b,
        provider_a_threads,
        provider_b_threads,
    );

    cleaned_links
}

fn singleton_link(thread: &ThreadRecord) -> LinkEntry {
    LinkEntry {
        providers: BTreeMap::from([(thread.model_provider.clone(), thread.id.clone())]),
        rollout_paths: BTreeMap::from([(
            thread.model_provider.clone(),
            thread.rollout_path.clone(),
        )]),
        last_synced_at: None,
        extra: BTreeMap::new(),
    }
}

fn assignment_key(provider: &str, thread_id: &str) -> String {
    format!("{provider}\u{0}{thread_id}")
}

fn mesh_link_sort_key(link: &LinkEntry) -> String {
    let mut parts = Vec::new();
    for (provider, thread_id) in &link.providers {
        parts.push(format!("{provider}:{thread_id}"));
    }
    parts.join("|")
}

fn can_merge_links(left: &LinkEntry, right: &LinkEntry) -> bool {
    for (provider, thread_id) in &right.providers {
        if let Some(existing) = left.providers.get(provider) {
            if existing != thread_id {
                return false;
            }
        }
    }
    true
}

fn merge_link_entries(base: &mut LinkEntry, incoming: LinkEntry) {
    for (provider, thread_id) in incoming.providers {
        base.providers.insert(provider, thread_id);
    }
    for (provider, rollout_path) in incoming.rollout_paths {
        base.rollout_paths.insert(provider, rollout_path);
    }
    if base.last_synced_at.is_none() {
        base.last_synced_at = incoming.last_synced_at;
    }
    for (key, value) in incoming.extra {
        base.extra.entry(key).or_insert(value);
    }
}

fn merge_link_group(
    groups: &mut Vec<Option<LinkEntry>>,
    assignments: &mut BTreeMap<String, usize>,
    incoming: LinkEntry,
    adopted: &mut BTreeSet<ThreadLocator>,
    warnings: &mut Vec<String>,
    track_adoption: bool,
    source_label: &str,
) {
    let new_members: Vec<_> = incoming
        .providers
        .iter()
        .map(|(provider, thread_id)| ThreadLocator {
            provider: provider.clone(),
            thread_id: thread_id.clone(),
        })
        .collect();
    let mut indices = BTreeSet::new();
    for (provider, thread_id) in &incoming.providers {
        if let Some(index) = assignments.get(&assignment_key(provider, thread_id)) {
            indices.insert(*index);
        }
    }

    if indices.is_empty() {
        let new_index = groups.len();
        for (provider, thread_id) in &incoming.providers {
            assignments.insert(assignment_key(provider, thread_id), new_index);
        }
        if track_adoption {
            adopted.extend(new_members);
        }
        groups.push(Some(incoming));
        return;
    }

    let mut indices: Vec<_> = indices.into_iter().collect();
    indices.sort_unstable();
    let base_index = indices[0];

    let Some(base_group) = groups[base_index].as_ref().cloned() else {
        return;
    };
    if !can_merge_links(&base_group, &incoming) || !can_merge_links(&incoming, &base_group) {
        warnings.push(format!(
            "{source_label} 命中 provider 冲突，已跳过: {}",
            mesh_link_sort_key(&incoming)
        ));
        return;
    }

    for index in indices.iter().skip(1).copied() {
        let Some(group) = groups[index].as_ref() else {
            continue;
        };
        if !can_merge_links(&base_group, group) || !can_merge_links(group, &base_group) {
            warnings.push(format!(
                "{source_label} 需要合并两个映射组，但 provider 指向冲突，已跳过: {} + {}",
                mesh_link_sort_key(&base_group),
                mesh_link_sort_key(group)
            ));
            return;
        }
    }

    {
        let base = groups[base_index]
            .as_mut()
            .expect("base group should exist");
        if track_adoption {
            for locator in &new_members {
                if !base.providers.contains_key(&locator.provider) {
                    adopted.insert(locator.clone());
                }
            }
        }
        merge_link_entries(base, incoming);
    }

    for index in indices.into_iter().skip(1) {
        if let Some(other) = groups[index].take() {
            {
                let base = groups[base_index]
                    .as_mut()
                    .expect("base group should exist");
                merge_link_entries(base, other.clone());
            }
            for (provider, thread_id) in other.providers {
                assignments.insert(assignment_key(&provider, &thread_id), base_index);
            }
        }
    }

    if let Some(base) = groups[base_index].as_ref() {
        for (provider, thread_id) in &base.providers {
            assignments.insert(assignment_key(provider, thread_id), base_index);
        }
    }
}

pub fn bootstrap_pair_links(
    mut state: SyncState,
    provider_a: &str,
    provider_b: &str,
    provider_a_threads: Option<&BTreeMap<String, ThreadRecord>>,
    provider_b_threads: Option<&BTreeMap<String, ThreadRecord>>,
) -> SyncState {
    if state.version == 0 {
        state.version = 1;
    }

    let empty_a = BTreeMap::new();
    let empty_b = BTreeMap::new();
    let provider_a_threads = provider_a_threads.unwrap_or(&empty_a);
    let provider_b_threads = provider_b_threads.unwrap_or(&empty_b);

    let mut cleaned_links = Vec::new();
    let mut used_a = BTreeSet::new();
    let mut used_b = BTreeSet::new();

    for mut link in state.links {
        if let Some(thread_id) = link.providers.get(provider_a).cloned() {
            if !provider_a_threads.contains_key(&thread_id) {
                link.providers.remove(provider_a);
            }
        }
        if let Some(thread_id) = link.providers.get(provider_b).cloned() {
            if !provider_b_threads.contains_key(&thread_id) {
                link.providers.remove(provider_b);
            }
        }
        if link.providers.is_empty() {
            continue;
        }

        if let Some(thread_id) = link.providers.get(provider_a) {
            used_a.insert(thread_id.clone());
        }
        if let Some(thread_id) = link.providers.get(provider_b) {
            used_b.insert(thread_id.clone());
        }
        cleaned_links.push(link);
    }

    match_unlinked_threads(
        &mut cleaned_links,
        &mut used_a,
        &mut used_b,
        provider_a,
        provider_b,
        provider_a_threads,
        provider_b_threads,
        |thread| thread.fingerprint_key(),
    );
    match_unlinked_threads(
        &mut cleaned_links,
        &mut used_a,
        &mut used_b,
        provider_a,
        provider_b,
        provider_a_threads,
        provider_b_threads,
        |thread| thread.identity_key(),
    );
    match_unlinked_threads_by_signature(
        &mut cleaned_links,
        &mut used_a,
        &mut used_b,
        provider_a,
        provider_b,
        provider_a_threads,
        provider_b_threads,
    );

    state.links = cleaned_links;
    state
}

fn map_thread_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ThreadRecord> {
    Ok(ThreadRecord {
        id: row.get("id")?,
        rollout_path: row.get("rollout_path")?,
        created_at: row.get("created_at")?,
        updated_at: row.get("updated_at")?,
        source: row.get("source")?,
        model_provider: row.get("model_provider")?,
        cwd: row.get("cwd")?,
        title: row.get("title")?,
        sandbox_policy: row.get("sandbox_policy")?,
        approval_mode: row.get("approval_mode")?,
        tokens_used: row.get("tokens_used")?,
        has_user_event: row.get("has_user_event")?,
        archived: row.get("archived")?,
        archived_at: row.get("archived_at")?,
        git_sha: row.get("git_sha")?,
        git_branch: row.get("git_branch")?,
        git_origin_url: row.get("git_origin_url")?,
        cli_version: row.get("cli_version")?,
        first_user_message: row.get("first_user_message")?,
        agent_nickname: row.get("agent_nickname")?,
        agent_role: row.get("agent_role")?,
        memory_mode: row.get("memory_mode")?,
    })
}

fn default_state_version() -> i64 {
    1
}

fn build_key(values: Vec<Value>) -> String {
    serde_json::to_string(&values).unwrap_or_else(|_| "[]".to_string())
}

fn match_unlinked_threads<F>(
    cleaned_links: &mut Vec<LinkEntry>,
    used_a: &mut BTreeSet<String>,
    used_b: &mut BTreeSet<String>,
    provider_a: &str,
    provider_b: &str,
    provider_a_threads: &BTreeMap<String, ThreadRecord>,
    provider_b_threads: &BTreeMap<String, ThreadRecord>,
    make_key: F,
) where
    F: Fn(&ThreadRecord) -> String,
{
    let mut bucket_a: BTreeMap<String, Vec<ThreadRecord>> = BTreeMap::new();
    let mut bucket_b: BTreeMap<String, Vec<ThreadRecord>> = BTreeMap::new();

    for thread in provider_a_threads.values() {
        if used_a.contains(&thread.id) {
            continue;
        }
        bucket_a
            .entry(make_key(thread))
            .or_default()
            .push(thread.clone());
    }

    for thread in provider_b_threads.values() {
        if used_b.contains(&thread.id) {
            continue;
        }
        bucket_b
            .entry(make_key(thread))
            .or_default()
            .push(thread.clone());
    }

    let shared_keys: Vec<_> = bucket_a
        .keys()
        .filter(|key| bucket_b.contains_key(*key))
        .cloned()
        .collect();
    for key in shared_keys {
        let mut left = bucket_a.remove(&key).unwrap_or_default();
        let mut right = bucket_b.remove(&key).unwrap_or_default();
        left.sort_by(|a, b| a.id.cmp(&b.id));
        right.sort_by(|a, b| a.id.cmp(&b.id));

        for (source_thread, target_thread) in left.into_iter().zip(right.into_iter()) {
            cleaned_links.push(make_link(
                provider_a,
                provider_b,
                &source_thread,
                &target_thread,
            ));
            used_a.insert(source_thread.id);
            used_b.insert(target_thread.id);
        }
    }
}

fn match_unlinked_threads_by_signature(
    cleaned_links: &mut Vec<LinkEntry>,
    used_a: &mut BTreeSet<String>,
    used_b: &mut BTreeSet<String>,
    provider_a: &str,
    provider_b: &str,
    provider_a_threads: &BTreeMap<String, ThreadRecord>,
    provider_b_threads: &BTreeMap<String, ThreadRecord>,
) {
    let mut signature_a: BTreeMap<String, Vec<ThreadRecord>> = BTreeMap::new();
    let mut signature_b: BTreeMap<String, Vec<ThreadRecord>> = BTreeMap::new();

    for thread in provider_a_threads.values() {
        if used_a.contains(&thread.id) {
            continue;
        }
        if let Some(signature) = make_rollout_signature(thread) {
            signature_a
                .entry(signature)
                .or_default()
                .push(thread.clone());
        }
    }

    for thread in provider_b_threads.values() {
        if used_b.contains(&thread.id) {
            continue;
        }
        if let Some(signature) = make_rollout_signature(thread) {
            signature_b
                .entry(signature)
                .or_default()
                .push(thread.clone());
        }
    }

    let shared_keys: Vec<_> = signature_a
        .keys()
        .filter(|key| signature_b.contains_key(*key))
        .cloned()
        .collect();
    for key in shared_keys {
        let mut left = signature_a.remove(&key).unwrap_or_default();
        let mut right = signature_b.remove(&key).unwrap_or_default();
        left.sort_by(|a, b| a.id.cmp(&b.id));
        right.sort_by(|a, b| a.id.cmp(&b.id));

        for (source_thread, target_thread) in left.into_iter().zip(right.into_iter()) {
            cleaned_links.push(make_link(
                provider_a,
                provider_b,
                &source_thread,
                &target_thread,
            ));
            used_a.insert(source_thread.id);
            used_b.insert(target_thread.id);
        }
    }
}

fn make_link(
    provider_a: &str,
    provider_b: &str,
    source_thread: &ThreadRecord,
    target_thread: &ThreadRecord,
) -> LinkEntry {
    LinkEntry {
        providers: BTreeMap::from([
            (provider_a.to_string(), source_thread.id.clone()),
            (provider_b.to_string(), target_thread.id.clone()),
        ]),
        rollout_paths: BTreeMap::from([
            (provider_a.to_string(), source_thread.rollout_path.clone()),
            (provider_b.to_string(), target_thread.rollout_path.clone()),
        ]),
        last_synced_at: None,
        extra: BTreeMap::new(),
    }
}

fn summarize_operations(operations: &[Operation]) -> OperationSummary {
    let mut summary = OperationSummary::default();
    for operation in operations {
        match operation.kind {
            OperationKind::Create => summary.create += 1,
            OperationKind::Adopt => summary.adopt += 1,
            OperationKind::Update => summary.update += 1,
            OperationKind::Repair => summary.repair += 1,
            OperationKind::Skip => summary.skip += 1,
        }
    }
    summary
}

fn summarize_mesh_operations(operations: &[MeshOperation]) -> OperationSummary {
    let mut summary = OperationSummary::default();
    for operation in operations {
        match operation.kind {
            MeshOperationKind::Create => summary.create += 1,
            MeshOperationKind::Adopt => summary.adopt += 1,
            MeshOperationKind::Update => summary.update += 1,
            MeshOperationKind::Repair => summary.repair += 1,
            MeshOperationKind::Skip => summary.skip += 1,
        }
    }
    summary
}

fn collect_backup_paths(operations: &[Operation]) -> Vec<PathBuf> {
    let mut paths: BTreeMap<String, PathBuf> = BTreeMap::new();
    for operation in operations {
        let source_path = PathBuf::from(&operation.source_thread.rollout_path);
        paths.insert(source_path.display().to_string(), source_path);
        if let Some(target_thread) = &operation.target_thread {
            let target_path = PathBuf::from(&target_thread.rollout_path);
            paths.insert(target_path.display().to_string(), target_path);
        }
    }
    paths.into_values().collect()
}

fn collect_mesh_backup_paths(operations: &[MeshOperation]) -> Vec<PathBuf> {
    let mut paths: BTreeMap<String, PathBuf> = BTreeMap::new();
    for operation in operations {
        let winner_path = PathBuf::from(&operation.winner_thread.rollout_path);
        paths.insert(winner_path.display().to_string(), winner_path);
        if let Some(target_thread) = &operation.target_thread {
            let target_path = PathBuf::from(&target_thread.rollout_path);
            paths.insert(target_path.display().to_string(), target_path);
        }
    }
    paths.into_values().collect()
}

fn atomic_write_text(path: &Path, content: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("创建目录失败: {}", parent.display()))?;
    }
    let temp_path = path.with_extension(format!("{}.tmp", Uuid::new_v4().simple()));
    fs::write(&temp_path, content.as_bytes())
        .with_context(|| format!("写入临时文件失败: {}", temp_path.display()))?;
    fs::rename(&temp_path, path).with_context(|| format!("原子替换失败: {}", path.display()))?;
    Ok(())
}

fn load_json_file<T>(path: &Path, default: T) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    if !path.exists() {
        return Ok(default);
    }
    let content =
        fs::read_to_string(path).with_context(|| format!("读取 JSON 失败: {}", path.display()))?;
    Ok(serde_json::from_str(&content)?)
}

fn save_json_pretty<T: Serialize>(path: &Path, payload: &T) -> Result<()> {
    let content = format!("{}\n", serde_json::to_string_pretty(payload)?);
    atomic_write_text(path, &content)
}

fn utc_now_iso() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn latest_turn_context(path: &Path) -> Option<(Value, String)> {
    let content = fs::read_to_string(path).ok()?;
    let mut latest = None;
    for line in content.lines() {
        let value: Value = serde_json::from_str(line).ok()?;
        if value.get("type").and_then(Value::as_str) != Some("turn_context") {
            continue;
        }
        let payload = value.get("payload")?.as_object()?;
        let sandbox_policy = payload.get("sandbox_policy")?.clone();
        let approval_policy = payload.get("approval_policy")?.as_str()?.to_string();
        latest = Some((sandbox_policy, approval_policy));
    }
    latest
}

fn rollout_permissions_match(thread: &ThreadRecord) -> bool {
    let expected_sandbox: Value = match serde_json::from_str(&thread.sandbox_policy) {
        Ok(value) => value,
        Err(_) => return true,
    };
    let Some((actual_sandbox, actual_approval)) =
        latest_turn_context(Path::new(&thread.rollout_path))
    else {
        return true;
    };
    actual_approval == thread.approval_mode && actual_sandbox == expected_sandbox
}

fn make_rollout_signature(thread: &ThreadRecord) -> Option<String> {
    let rollout_path = Path::new(&thread.rollout_path);
    let content = fs::read_to_string(rollout_path).ok()?;
    let lines: Vec<&str> = content.lines().collect();
    if lines.is_empty() {
        return None;
    }

    let mut first: Value = serde_json::from_str(lines[0]).ok()?;
    if let Some(payload) = first.get_mut("payload").and_then(Value::as_object_mut) {
        payload.remove("id");
        payload.remove("model_provider");
    }

    let mut normalized = vec![canonical_json_string(&first)];
    normalized.extend(lines.iter().skip(1).map(|line| (*line).to_string()));
    Some(normalized.join("\n"))
}

fn canonical_json_string(value: &Value) -> String {
    serde_json::to_string(&canonicalize_json(value)).unwrap_or_else(|_| "null".to_string())
}

fn canonicalize_json(value: &Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.iter().map(canonicalize_json).collect()),
        Value::Object(object) => {
            let mut ordered = Map::new();
            let mut keys: Vec<_> = object.keys().cloned().collect();
            keys.sort();
            for key in keys {
                if let Some(inner) = object.get(&key) {
                    ordered.insert(key, canonicalize_json(inner));
                }
            }
            Value::Object(ordered)
        }
        _ => value.clone(),
    }
}

fn relative_archive_path(path: &Path) -> String {
    let display = path.display().to_string();
    display.trim_start_matches('/').to_string()
}

fn upsert_session_index_entry(entries: &mut Vec<Value>, thread: &ThreadRecord) -> Result<()> {
    let updated_at = Utc
        .timestamp_millis_opt(thread.updated_at)
        .single()
        .ok_or_else(|| anyhow!("非法 updated_at: {}", thread.updated_at))?
        .to_rfc3339_opts(SecondsFormat::Micros, true);

    for entry in entries.iter_mut() {
        if entry.get("id").and_then(Value::as_str) == Some(thread.id.as_str()) {
            let object = entry
                .as_object_mut()
                .ok_or_else(|| anyhow!("session_index 条目不是对象"))?;
            object.insert(
                "thread_name".to_string(),
                Value::String(thread.title.clone()),
            );
            object.insert("updated_at".to_string(), Value::String(updated_at));
            return Ok(());
        }
    }

    entries.push(json!({
        "id": thread.id,
        "thread_name": thread.title,
        "updated_at": updated_at,
    }));
    Ok(())
}

fn copy_workspace_hint(global_state: &mut Value, source_id: &str, target_id: &str) {
    let Some(root) = global_state.as_object_mut() else {
        return;
    };
    let Some(hints) = root
        .get_mut("thread-workspace-root-hints")
        .and_then(Value::as_object_mut)
    else {
        return;
    };
    if let Some(source_hint) = hints.get(source_id).cloned() {
        hints.insert(target_id.to_string(), source_hint);
    }
}

fn synced_target_thread(
    codex_home: &Path,
    source_thread: &ThreadRecord,
    target_thread: &ThreadRecord,
    target_provider: &str,
) -> ThreadRecord {
    let mut synced = source_thread.clone();
    synced.id = target_thread.id.clone();
    synced.model_provider = target_provider.to_string();
    synced.rollout_path = normalized_target_rollout_path(
        codex_home,
        source_thread,
        &source_thread.id,
        &target_thread.id,
    )
    .display()
    .to_string();
    synced
}

fn append_log(path: &Path, level: &str, message: &str) {
    let line = format!(
        "{} {} {}\n",
        Local::now().to_rfc3339_opts(SecondsFormat::Millis, true),
        level,
        message
    );
    if try_append_log(path, &line).is_err() {
        let fallback = std::env::temp_dir().join("codex-provider-sync.log");
        let _ = try_append_log(&fallback, &line);
    }
}

fn try_append_log(path: &Path, line: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    file.write_all(line.as_bytes())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::fs;
    use std::path::{Path, PathBuf};

    fn sample_engine() -> CodexSyncEngine {
        let root = env::temp_dir().join(format!("codex-provider-sync-test-{}", Uuid::new_v4()));
        CodexSyncEngine::new(
            root.clone(),
            root.join("provider_sync_state.json"),
            root.join("provider_sync_backups"),
            root.join("provider_sync.log"),
        )
    }

    fn sample_thread(id: &str, provider: &str, rollout_path: &str) -> ThreadRecord {
        ThreadRecord {
            id: id.to_string(),
            rollout_path: rollout_path.to_string(),
            created_at: 1,
            updated_at: 2,
            source: "vscode".to_string(),
            model_provider: provider.to_string(),
            cwd: "/tmp/project".to_string(),
            title: "Hello".to_string(),
            sandbox_policy: "workspace-write".to_string(),
            approval_mode: "on-request".to_string(),
            tokens_used: 10,
            has_user_event: 1,
            archived: 0,
            archived_at: None,
            git_sha: Some("abc".to_string()),
            git_branch: Some("main".to_string()),
            git_origin_url: Some("git@example.com/repo.git".to_string()),
            cli_version: "1.0.0".to_string(),
            first_user_message: "hi".to_string(),
            agent_nickname: None,
            agent_role: None,
            memory_mode: "enabled".to_string(),
        }
    }

    fn sample_threads_by_provider(
        threads: Vec<ThreadRecord>,
    ) -> BTreeMap<String, BTreeMap<String, ThreadRecord>> {
        let mut providers = BTreeMap::new();
        for thread in threads {
            providers
                .entry(thread.model_provider.clone())
                .or_insert_with(BTreeMap::new)
                .insert(thread.id.clone(), thread);
        }
        providers
    }

    fn create_test_schema(engine: &CodexSyncEngine) {
        fs::create_dir_all(&engine.codex_home).expect("create codex home");
        let conn = Connection::open(&engine.db_path).expect("open test db");
        conn.execute_batch(
            "
            create table threads (
                id text primary key,
                rollout_path text not null,
                created_at integer not null,
                updated_at integer not null,
                source text not null,
                model_provider text not null,
                cwd text not null,
                title text not null,
                sandbox_policy text not null,
                approval_mode text not null,
                tokens_used integer not null default 0,
                has_user_event integer not null default 0,
                archived integer not null default 0,
                archived_at integer,
                git_sha text,
                git_branch text,
                git_origin_url text,
                cli_version text not null default '',
                first_user_message text not null default '',
                agent_nickname text,
                agent_role text,
                memory_mode text not null default 'enabled',
                model text,
                reasoning_effort text
            );
            create table logs (
                id integer primary key autoincrement,
                ts integer not null,
                ts_nanos integer not null,
                level text not null,
                target text not null,
                message text,
                module_path text,
                file text,
                line integer,
                thread_id text,
                process_uuid text,
                estimated_bytes integer not null default 0
            );
            create table thread_dynamic_tools (
                thread_id text not null,
                position integer not null,
                name text not null,
                description text not null,
                input_schema text not null,
                defer_loading integer not null default 0,
                primary key(thread_id, position)
            );
            create table stage1_outputs (
                thread_id text primary key,
                source_updated_at integer not null,
                raw_memory text not null,
                rollout_summary text not null,
                generated_at integer not null,
                rollout_slug text,
                usage_count integer,
                last_usage integer,
                selected_for_phase2 integer not null default 0,
                selected_for_phase2_source_updated_at integer
            );
            create table agent_job_items (
                job_id text not null,
                item_id text not null,
                row_index integer not null,
                source_id text,
                row_json text not null,
                status text not null,
                assigned_thread_id text,
                attempt_count integer not null default 0,
                result_json text,
                last_error text,
                created_at integer not null,
                updated_at integer not null,
                completed_at integer,
                reported_at integer,
                primary key (job_id, item_id)
            );
            ",
        )
        .expect("create schema");
    }

    fn insert_thread(conn: &Connection, thread: &ThreadRecord) {
        conn.execute(
            "insert into threads (
                id, rollout_path, created_at, updated_at, source, model_provider, cwd, title,
                sandbox_policy, approval_mode, tokens_used, has_user_event, archived, archived_at,
                git_sha, git_branch, git_origin_url, cli_version, first_user_message,
                agent_nickname, agent_role, memory_mode
            ) values (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )",
            params![
                &thread.id,
                &thread.rollout_path,
                thread.created_at,
                thread.updated_at,
                &thread.source,
                &thread.model_provider,
                &thread.cwd,
                &thread.title,
                &thread.sandbox_policy,
                &thread.approval_mode,
                thread.tokens_used,
                thread.has_user_event,
                thread.archived,
                thread.archived_at,
                &thread.git_sha,
                &thread.git_branch,
                &thread.git_origin_url,
                &thread.cli_version,
                &thread.first_user_message,
                &thread.agent_nickname,
                &thread.agent_role,
                &thread.memory_mode,
            ],
        )
        .expect("insert thread");
    }

    fn write_rollout(thread: &ThreadRecord, body: &str) {
        let path = Path::new(&thread.rollout_path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("create rollout parent");
        }
        fs::write(path, body).expect("write rollout");
    }

    #[test]
    fn rewrite_rollout_text_sets_id_and_provider() {
        let original = concat!(
            "{\"timestamp\":\"2026-03-19T00:00:00Z\",\"type\":\"session_meta\",\"payload\":{\"id\":\"old-id\",\"source\":\"vscode\"}}\n",
            "{\"timestamp\":\"2026-03-19T00:00:01Z\",\"type\":\"response_item\",\"payload\":{\"type\":\"message\"}}\n"
        );

        let rewritten = rewrite_rollout_text(
            original,
            "new-id",
            "cpa",
            r#"{"type":"danger-full-access"}"#,
            "never",
        )
        .expect("rewrite should succeed");
        let first_line = rewritten.lines().next().expect("first line should exist");
        assert!(first_line.contains("\"id\":\"new-id\""));
        assert!(first_line.contains("\"model_provider\":\"cpa\""));
    }

    #[test]
    fn rewrite_rollout_text_normalizes_latest_turn_context_permissions() {
        let original = concat!(
            "{\"timestamp\":\"2026-03-19T00:00:00Z\",\"type\":\"session_meta\",\"payload\":{\"id\":\"old-id\",\"source\":\"vscode\"}}\n",
            "{\"timestamp\":\"2026-03-19T00:00:01Z\",\"type\":\"turn_context\",\"payload\":{\"approval_policy\":\"on-request\",\"sandbox_policy\":{\"type\":\"workspace-write\"}}}\n"
        );

        let rewritten = rewrite_rollout_text(
            original,
            "new-id",
            "cpa",
            r#"{"type":"danger-full-access"}"#,
            "never",
        )
        .expect("rewrite should succeed");
        let latest_line = rewritten
            .lines()
            .last()
            .expect("turn context line should exist");
        assert!(latest_line.contains("\"approval_policy\":\"never\""));
        assert!(latest_line.contains("\"sandbox_policy\":{\"type\":\"danger-full-access\"}"));
    }

    #[test]
    fn bootstrap_pair_links_matches_by_fingerprint() {
        let mut left = BTreeMap::new();
        let mut right = BTreeMap::new();
        left.insert(
            "a1".to_string(),
            sample_thread("a1", "openai", "/tmp/a1.jsonl"),
        );
        right.insert(
            "b1".to_string(),
            sample_thread("b1", "cpa", "/tmp/b1.jsonl"),
        );

        let result = bootstrap_pair_links(
            SyncState {
                version: 1,
                links: Vec::new(),
                extra: BTreeMap::new(),
            },
            "openai",
            "cpa",
            Some(&left),
            Some(&right),
        );

        assert_eq!(result.links.len(), 1);
        assert_eq!(
            result.links[0].providers.get("openai"),
            Some(&"a1".to_string())
        );
        assert_eq!(
            result.links[0].providers.get("cpa"),
            Some(&"b1".to_string())
        );
    }

    #[test]
    fn make_target_rollout_path_replaces_suffix_id() {
        let target = make_target_rollout_path("/tmp/rollout-old-id.jsonl", "old-id", "new-id");
        assert_eq!(target, PathBuf::from("/tmp/rollout-new-id.jsonl"));
    }

    #[test]
    fn ordered_active_providers_prefers_openai_then_cpa() {
        let counts = BTreeMap::from([
            ("zzz".to_string(), 2),
            ("cpa".to_string(), 1),
            ("openai".to_string(), 3),
            ("".to_string(), 5),
        ]);

        assert_eq!(
            ordered_active_providers(&counts),
            vec!["openai".to_string(), "cpa".to_string(), "zzz".to_string()]
        );
    }

    #[test]
    fn build_mesh_links_state_merges_third_provider_into_existing_group() {
        let openai = sample_thread("a1", "openai", "/tmp/a1.jsonl");
        let cpa = sample_thread("b1", "cpa", "/tmp/b1.jsonl");
        let anthropic = sample_thread("c1", "anthropic", "/tmp/c1.jsonl");
        let threads =
            sample_threads_by_provider(vec![openai.clone(), cpa.clone(), anthropic.clone()]);
        let providers = vec![
            "openai".to_string(),
            "cpa".to_string(),
            "anthropic".to_string(),
        ];
        let existing_state = SyncState {
            version: 1,
            links: vec![make_link("openai", "cpa", &openai, &cpa)],
            extra: BTreeMap::new(),
        };

        let bootstrap = build_mesh_links_state(existing_state, &threads, &providers);

        assert_eq!(bootstrap.state.links.len(), 1);
        let link = &bootstrap.state.links[0];
        assert_eq!(link.providers.get("openai"), Some(&"a1".to_string()));
        assert_eq!(link.providers.get("cpa"), Some(&"b1".to_string()));
        assert_eq!(link.providers.get("anthropic"), Some(&"c1".to_string()));
        assert!(bootstrap.adopted.contains(&ThreadLocator {
            provider: "anthropic".to_string(),
            thread_id: "c1".to_string(),
        }));
    }

    #[test]
    fn select_authoritative_thread_prefers_openai_on_same_timestamp() {
        let mut openai = sample_thread("a1", "openai", "/tmp/a1.jsonl");
        let mut cpa = sample_thread("b1", "cpa", "/tmp/b1.jsonl");
        openai.updated_at = 10;
        cpa.updated_at = 10;
        cpa.title = "different".to_string();
        let threads = sample_threads_by_provider(vec![openai.clone(), cpa.clone()]);
        let link = make_link("openai", "cpa", &openai, &cpa);

        let winner = select_authoritative_thread(&link, &threads).expect("winner should exist");
        assert_eq!(winner.model_provider, "openai");
        assert_eq!(winner.id, "a1");
    }

    #[test]
    fn select_authoritative_thread_for_selected_providers_ignores_unselected_provider() {
        let mut openai = sample_thread("a1", "openai", "/tmp/a1.jsonl");
        let mut cpa = sample_thread("b1", "cpa", "/tmp/b1.jsonl");
        let mut anthropic = sample_thread("c1", "anthropic", "/tmp/c1.jsonl");
        openai.updated_at = 10;
        cpa.updated_at = 10;
        anthropic.updated_at = 100;
        let threads =
            sample_threads_by_provider(vec![openai.clone(), cpa.clone(), anthropic.clone()]);
        let link = LinkEntry {
            providers: BTreeMap::from([
                ("openai".to_string(), openai.id.clone()),
                ("cpa".to_string(), cpa.id.clone()),
                ("anthropic".to_string(), anthropic.id.clone()),
            ]),
            rollout_paths: BTreeMap::new(),
            last_synced_at: None,
            extra: BTreeMap::new(),
        };
        let selected = BTreeSet::from(["openai".to_string(), "cpa".to_string()]);

        let winner = select_authoritative_thread_for_providers(&link, &threads, &selected)
            .expect("winner should exist");

        assert_eq!(winner.model_provider, "openai");
        assert_eq!(winner.id, "a1");
    }

    #[test]
    fn resolve_selected_providers_preserves_preferred_order() {
        let counts = BTreeMap::from([
            ("anthropic".to_string(), 2),
            ("cpa".to_string(), 1),
            ("openai".to_string(), 3),
        ]);

        let selected = resolve_selected_providers(
            &[
                "anthropic".to_string(),
                "openai".to_string(),
                "cpa".to_string(),
            ],
            &counts,
        )
        .expect("selected providers should resolve");

        assert_eq!(
            selected,
            vec![
                "openai".to_string(),
                "cpa".to_string(),
                "anthropic".to_string()
            ]
        );
    }

    #[test]
    fn plan_mesh_operations_marks_adopt_for_existing_new_provider_thread() {
        let engine = sample_engine();
        let openai = sample_thread("a1", "openai", "/tmp/a1.jsonl");
        let cpa = sample_thread("b1", "cpa", "/tmp/b1.jsonl");
        let anthropic = sample_thread("c1", "anthropic", "/tmp/c1.jsonl");
        let threads =
            sample_threads_by_provider(vec![openai.clone(), cpa.clone(), anthropic.clone()]);
        let providers = vec![
            "openai".to_string(),
            "cpa".to_string(),
            "anthropic".to_string(),
        ];
        let state = SyncState {
            version: 1,
            links: vec![make_link("openai", "cpa", &openai, &cpa)],
            extra: BTreeMap::new(),
        };

        let bootstrap = build_mesh_links_state(state, &threads, &providers);
        let operations =
            engine.plan_mesh_operations(&bootstrap.state, &threads, &providers, &bootstrap.adopted);

        let adopt = operations
            .iter()
            .find(|operation| operation.target_provider == "anthropic")
            .expect("anthropic operation should exist");
        assert_eq!(adopt.kind, MeshOperationKind::Adopt);
    }

    #[test]
    fn plan_mesh_operations_updates_when_archived_state_differs() {
        let engine = sample_engine();
        let mut openai = sample_thread("a1", "openai", "/tmp/a1.jsonl");
        let mut cpa = sample_thread("b1", "cpa", "/tmp/b1.jsonl");
        openai.updated_at = 20;
        openai.archived = 1;
        openai.archived_at = Some(123);
        cpa.updated_at = 20;
        let threads = sample_threads_by_provider(vec![openai.clone(), cpa.clone()]);
        let providers = vec!["openai".to_string(), "cpa".to_string()];
        let state = SyncState {
            version: 1,
            links: vec![make_link("openai", "cpa", &openai, &cpa)],
            extra: BTreeMap::new(),
        };

        let operations =
            engine.plan_mesh_operations(&state, &threads, &providers, &BTreeSet::new());

        let cpa_op = operations
            .iter()
            .find(|operation| operation.target_provider == "cpa")
            .expect("cpa operation should exist");
        assert_eq!(cpa_op.kind, MeshOperationKind::Update);
    }

    #[test]
    fn synced_target_thread_normalizes_unarchived_path_back_to_sessions() {
        let engine = sample_engine();
        let openai = sample_thread(
            "a1",
            "openai",
            "/Users/example/.codex/sessions/2026/03/19/rollout-2026-03-19T09-09-41-a1.jsonl",
        );
        let cpa = sample_thread(
            "b1",
            "cpa",
            "/Users/example/.codex/archived_sessions/rollout-2026-03-19T09-09-41-b1.jsonl",
        );

        let synced = engine.synced_target_thread(&openai, &cpa, "cpa");

        assert_eq!(
            synced.rollout_path,
            engine
                .codex_home
                .join("sessions/2026/03/19/rollout-2026-03-19T09-09-41-b1.jsonl")
                .display()
                .to_string()
        );
    }

    #[test]
    fn plan_mesh_operations_updates_when_rollout_directory_mismatch() {
        let engine = sample_engine();
        let openai = sample_thread(
            "a1",
            "openai",
            "/Users/example/.codex/sessions/2026/03/19/rollout-2026-03-19T09-09-41-a1.jsonl",
        );
        let cpa = sample_thread(
            "b1",
            "cpa",
            "/Users/example/.codex/archived_sessions/rollout-2026-03-19T09-09-41-b1.jsonl",
        );
        let threads = sample_threads_by_provider(vec![openai.clone(), cpa.clone()]);
        let providers = vec!["openai".to_string(), "cpa".to_string()];
        let state = SyncState {
            version: 1,
            links: vec![make_link("openai", "cpa", &openai, &cpa)],
            extra: BTreeMap::new(),
        };

        let operations =
            engine.plan_mesh_operations(&state, &threads, &providers, &BTreeSet::new());

        let cpa_op = operations
            .iter()
            .find(|operation| operation.target_provider == "cpa")
            .expect("cpa operation should exist");
        assert_eq!(cpa_op.kind, MeshOperationKind::Update);
    }

    #[test]
    fn collect_cleanup_candidates_only_keeps_fully_archived_links() {
        let mut openai_archived = sample_thread("a1", "openai", "/tmp/a1.jsonl");
        let mut cpa_archived = sample_thread("b1", "cpa", "/tmp/b1.jsonl");
        let mut openai_active = sample_thread("a2", "openai", "/tmp/a2.jsonl");
        let mut cpa_active = sample_thread("b2", "cpa", "/tmp/b2.jsonl");
        let mut mixed_archived = sample_thread("a3", "openai", "/tmp/a3.jsonl");
        let mixed_active = sample_thread("b3", "cpa", "/tmp/b3.jsonl");
        openai_archived.archived = 1;
        cpa_archived.archived = 1;
        openai_archived.updated_at = 100;
        cpa_archived.updated_at = 100;
        openai_active.updated_at = 200;
        cpa_active.updated_at = 200;
        mixed_archived.archived = 1;
        mixed_archived.updated_at = 300;

        let state = SyncState {
            version: 1,
            links: vec![
                make_link("openai", "cpa", &openai_archived, &cpa_archived),
                make_link("openai", "cpa", &openai_active, &cpa_active),
                make_link("openai", "cpa", &mixed_archived, &mixed_active),
            ],
            extra: BTreeMap::new(),
        };
        let threads = sample_threads_by_provider(vec![
            openai_archived.clone(),
            cpa_archived.clone(),
            openai_active,
            cpa_active,
            mixed_archived,
            mixed_active,
        ]);

        let archived_candidates =
            collect_cleanup_candidates(&state, &threads, CleanupScope::Archived, None, 0);
        let active_candidates =
            collect_cleanup_candidates(&state, &threads, CleanupScope::Active, None, 0);

        assert_eq!(archived_candidates.len(), 1);
        assert_eq!(archived_candidates[0].winner_thread.id, "a1");
        assert_eq!(active_candidates.len(), 1);
        assert_eq!(active_candidates[0].winner_thread.id, "a2");
    }

    #[test]
    fn cleanup_apply_removes_threads_rollouts_and_state() {
        let engine = sample_engine();
        create_test_schema(&engine);

        let mut openai = sample_thread(
            "a1",
            "openai",
            &engine
                .codex_home
                .join("archived_sessions/rollout-openai-a1.jsonl")
                .display()
                .to_string(),
        );
        let mut cpa = sample_thread(
            "b1",
            "cpa",
            &engine
                .codex_home
                .join("archived_sessions/rollout-cpa-b1.jsonl")
                .display()
                .to_string(),
        );
        openai.archived = 1;
        cpa.archived = 1;
        openai.archived_at = Some(10);
        cpa.archived_at = Some(10);
        openai.updated_at = 10;
        cpa.updated_at = 10;

        write_rollout(
            &openai,
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"a1\",\"model_provider\":\"openai\"}}\n",
        );
        write_rollout(
            &cpa,
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"b1\",\"model_provider\":\"cpa\"}}\n",
        );

        let conn = Connection::open(&engine.db_path).expect("open test db");
        insert_thread(&conn, &openai);
        insert_thread(&conn, &cpa);
        conn.execute(
            "insert into logs (ts, ts_nanos, level, target, message, thread_id, process_uuid, estimated_bytes) values (1, 0, 'INFO', 'test', 'msg', ?, 'p', 1)",
            [openai.id.as_str()],
        )
        .expect("insert log");
        conn.execute(
            "insert into thread_dynamic_tools (thread_id, position, name, description, input_schema, defer_loading) values (?, 0, 'tool', 'desc', '{}', 0)",
            [cpa.id.as_str()],
        )
        .expect("insert tool");
        conn.execute(
            "insert into stage1_outputs (thread_id, source_updated_at, raw_memory, rollout_summary, generated_at) values (?, 1, 'raw', 'summary', 1)",
            [cpa.id.as_str()],
        )
        .expect("insert stage1");
        conn.execute(
            "insert into agent_job_items (job_id, item_id, row_index, row_json, status, assigned_thread_id, created_at, updated_at) values ('job', 'item', 0, '{}', 'running', ?, 1, 1)",
            [cpa.id.as_str()],
        )
        .expect("insert agent item");
        drop(conn);

        save_json_pretty(
            &engine.state_path,
            &SyncState {
                version: 1,
                links: vec![make_link("openai", "cpa", &openai, &cpa)],
                extra: BTreeMap::new(),
            },
        )
        .expect("save state");
        atomic_write_text(
            &engine.session_index_path,
            "{\"id\":\"a1\",\"thread_name\":\"Hello\",\"updated_at\":\"2026-03-19T00:00:00Z\"}\n{\"id\":\"b1\",\"thread_name\":\"Hello\",\"updated_at\":\"2026-03-19T00:00:00Z\"}\n",
        )
        .expect("save session index");
        atomic_write_text(
            &engine.global_state_path,
            "{\"thread-workspace-root-hints\":{\"a1\":\"/tmp/a\",\"b1\":\"/tmp/b\"}}\n",
        )
        .expect("save global state");

        let result = engine
            .cleanup("archived", None, 0, false)
            .expect("dry run cleanup should succeed");
        assert_eq!(result.planned.logical_threads, 1);
        assert_eq!(result.planned.thread_copies, 2);

        let applied = engine
            .cleanup("archived", Some(1), 0, true)
            .expect("apply cleanup should succeed");
        assert_eq!(
            applied
                .applied
                .expect("applied summary should exist")
                .thread_copies,
            2
        );

        let conn = Connection::open(&engine.db_path).expect("reopen test db");
        let thread_count: i64 = conn
            .query_row("select count(*) from threads", [], |row| row.get(0))
            .expect("count threads");
        let log_count: i64 = conn
            .query_row("select count(*) from logs", [], |row| row.get(0))
            .expect("count logs");
        let stage1_count: i64 = conn
            .query_row("select count(*) from stage1_outputs", [], |row| row.get(0))
            .expect("count stage1");
        let tool_count: i64 = conn
            .query_row("select count(*) from thread_dynamic_tools", [], |row| {
                row.get(0)
            })
            .expect("count tools");
        let assigned_thread: Option<String> = conn
            .query_row(
                "select assigned_thread_id from agent_job_items where job_id='job' and item_id='item'",
                [],
                |row| row.get(0),
            )
            .expect("read agent job item");
        assert_eq!(thread_count, 0);
        assert_eq!(log_count, 0);
        assert_eq!(stage1_count, 0);
        assert_eq!(tool_count, 0);
        assert_eq!(assigned_thread, None);

        let state = load_json_file(
            &engine.state_path,
            SyncState {
                version: 1,
                links: Vec::new(),
                extra: BTreeMap::new(),
            },
        )
        .expect("load state");
        assert!(state.links.is_empty());
        let session_index =
            fs::read_to_string(&engine.session_index_path).expect("read session index");
        assert!(session_index.trim().is_empty());
        let global_state =
            fs::read_to_string(&engine.global_state_path).expect("read global state");
        assert_eq!(global_state.trim(), "{}");
        assert!(!Path::new(&openai.rollout_path).exists());
        assert!(!Path::new(&cpa.rollout_path).exists());
    }
}
