//! Pipeline orchestration engine.
//!
//! [`PipelineOrchestrator`] drives the multi-phase pipeline.  Which phases
//! are run depends on the [`presets::PresetKind`] stored in [`PipelineConfig`].
//!
//! The Review phase optionally creates a GitHub PR via octocrab when
//! `GITHUB_TOKEN` is set.

pub mod presets;

use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use koklo_agent_runtime::{AgentConfig, AgentRunResult, AgentRunner};
use koklo_events::{
    CompletionUsage, CostDisplay, EventBus, GateAction, GateChannel, GateDisplay, GateResponse,
    Phase, PipelineEvent,
};
use koklo_providers::{ClaudeCodeCliProvider, CodexCliProvider, LlmProvider, ProviderRegistry};
use koklo_shell::{BubblewrapSandbox, ControlledShell, LandlockSandbox, Sandbox};
use koklo_storage::{Session, SessionManager, UsageRecordInput};
use presets::{phases_for_preset, PresetKind};
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

/// Optional GitHub integration config.
#[derive(Debug, Clone)]
pub struct GithubConfig {
    /// Personal access token (from `GITHUB_TOKEN`).
    pub token: String,
    /// Repository owner, e.g. `"koklo-dev"`.
    pub owner: String,
    /// Repository name, e.g. `"koklo"`.
    pub repo: String,
    /// Base branch for the PR (default: `"develop"`).
    pub base_branch: String,
}

impl GithubConfig {
    /// Read from environment variables; returns `None` if `GITHUB_TOKEN` is unset.
    pub fn from_env() -> Option<Self> {
        let token = std::env::var("GITHUB_TOKEN").ok()?;
        Some(Self {
            token,
            owner: std::env::var("KOKLO_GITHUB_OWNER").unwrap_or_else(|_| "koklo-dev".to_string()),
            repo: std::env::var("KOKLO_GITHUB_REPO").unwrap_or_else(|_| "koklo".to_string()),
            base_branch: std::env::var("KOKLO_BASE_BRANCH")
                .unwrap_or_else(|_| "develop".to_string()),
        })
    }
}

/// Configuration for the pipeline.
#[derive(Clone)]
pub struct PipelineConfig {
    /// Path to the global SQLite database (`~/.koklo/koklo.db`).
    pub db_path: String,
    pub artifacts_dir: PathBuf,
    /// Global koklo home directory (`~/.koklo/`).
    pub global_home: PathBuf,
    /// Project-level `.koklo/` directory. `None` when outside any project.
    pub project_context: Option<PathBuf>,
    /// Absolute path of the current project root (recorded on each session).
    pub project_path: String,
    /// Workflow preset used when none is specified at the call site.
    pub preset: PresetKind,
    /// Default provider used when no per-agent override is set.
    pub default_provider: Arc<dyn LlmProvider>,
    /// Per-agent provider overrides (keyed by agent name, e.g. `"pm"`, `"developer"`).
    pub agent_providers: HashMap<String, Arc<dyn LlmProvider>>,
    /// Optional per-agent sandbox directives from `pipeline.toml`.
    pub agent_sandboxes: HashMap<String, String>,
    /// When true, wrap local CLI execution in `ControlledShell`.
    pub controlled_shell: bool,
    /// Registry for env-var runtime overrides (`KOKLO_PROVIDER_<AGENT>`).
    pub provider_registry: Arc<ProviderRegistry>,
    /// When `Some`, the Review phase creates a GitHub PR.
    pub github: Option<GithubConfig>,
}

/// Handles a gate checkpoint.
#[async_trait]
pub trait GateHandler: Send + Sync {
    async fn handle(&self, display: GateDisplay) -> Result<GateResponse>;
}

/// StdinGateHandler: reads from stdin. Used by --no-tui / CI.
pub struct StdinGateHandler;

#[async_trait]
impl GateHandler for StdinGateHandler {
    async fn handle(&self, display: GateDisplay) -> Result<GateResponse> {
        println!(
            "\n[GATE] Phase '{}' complete. Approve? [y/N] ",
            display.phase
        );
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        let trimmed = input.trim().to_lowercase();
        if trimmed == "y" || trimmed == "yes" {
            Ok(GateResponse::Approve)
        } else {
            Ok(GateResponse::Reject)
        }
    }
}

/// TuiGateHandler: deposits request in GateChannel, awaits TUI response.
pub struct TuiGateHandler {
    channel: GateChannel,
}

impl TuiGateHandler {
    pub fn new(channel: GateChannel) -> Self {
        Self { channel }
    }
}

#[async_trait]
impl GateHandler for TuiGateHandler {
    async fn handle(&self, display: GateDisplay) -> Result<GateResponse> {
        self.channel.deposit_and_await(display).await
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SandboxMode {
    ReadOnly,
    WorkspaceWrite,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SandboxDirective {
    mode: Option<SandboxMode>,
    controlled: bool,
}

enum PhaseRunOutcome {
    Completed {
        output: String,
        usage: CompletionUsage,
        cost: Option<CostDisplay>,
    },
}

enum SessionControlAction {
    Pause,
    Cancel,
}

impl SessionControlAction {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "pause" => Some(Self::Pause),
            "cancel" => Some(Self::Cancel),
            _ => None,
        }
    }
}

impl PipelineConfig {
    /// Select the provider for a given agent.
    ///
    /// Priority:
    /// 1. `KOKLO_PROVIDER_<AGENT_UPPER>` env var → registry lookup (warns if unknown)
    /// 2. `agent_providers` map (from TOML)
    /// 3. `default_provider`
    pub fn resolve_provider_for_agent(&self, agent_name: &str) -> Arc<dyn LlmProvider> {
        let env_key = format!("KOKLO_PROVIDER_{}", agent_name.to_uppercase());
        if let Ok(name) = std::env::var(&env_key) {
            if let Some(p) = self.provider_registry.get(&name) {
                tracing::debug!(
                    "Agent '{}': using provider '{}' from env {}",
                    agent_name,
                    name,
                    env_key
                );
                return p;
            }
            tracing::warn!(
                "Agent '{}': env {}='{}' not found in registry, falling back to default",
                agent_name,
                env_key,
                name
            );
        }
        if let Some(p) = self.agent_providers.get(agent_name) {
            tracing::debug!("Agent '{}': using TOML-configured provider", agent_name);
            return p.clone();
        }
        tracing::debug!(
            "Agent '{}': using default provider '{}'",
            agent_name,
            self.default_provider.provider_name()
        );
        self.default_provider.clone()
    }

    /// Select the provider for a given agent and pin CLI-based providers to a session workspace.
    pub fn resolve_provider_for_agent_in_workspace(
        &self,
        agent_name: &str,
        phase: Phase,
        workspace_root: Option<&Path>,
    ) -> Arc<dyn LlmProvider> {
        let provider = self.resolve_provider_for_agent(agent_name);
        let Some(workspace_root) = workspace_root else {
            return provider;
        };
        let sandbox = self.make_phase_sandbox(agent_name, phase, workspace_root);

        match provider.provider_name() {
            "claude-code-cli" => match match sandbox {
                Some(sandbox) => {
                    ClaudeCodeCliProvider::with_context(workspace_root.to_path_buf(), sandbox)
                }
                None => ClaudeCodeCliProvider::with_working_dir(workspace_root.to_path_buf()),
            } {
                Ok(p) => Arc::new(p),
                Err(err) => {
                    tracing::warn!(
                        "Failed to pin Claude Code provider to workspace {}: {}",
                        workspace_root.display(),
                        err
                    );
                    provider
                }
            },
            "codex-cli" => match match sandbox {
                Some(sandbox) => {
                    CodexCliProvider::with_context(workspace_root.to_path_buf(), sandbox)
                }
                None => CodexCliProvider::with_working_dir(workspace_root.to_path_buf()),
            } {
                Ok(p) => Arc::new(p),
                Err(err) => {
                    tracing::warn!(
                        "Failed to pin Codex provider to workspace {}: {}",
                        workspace_root.display(),
                        err
                    );
                    provider
                }
            },
            _ => provider,
        }
    }

    fn make_phase_sandbox(
        &self,
        agent_name: &str,
        phase: Phase,
        workspace_root: &Path,
    ) -> Option<Arc<dyn Sandbox>> {
        let directive = self.agent_sandbox_directive(agent_name);
        let mode = directive
            .mode
            .unwrap_or_else(|| default_sandbox_mode_for_phase(phase));
        let sandbox: Box<dyn Sandbox> = match mode {
            SandboxMode::ReadOnly => Box::new(LandlockSandbox::with_network(
                vec![workspace_root.to_path_buf()],
                true,
            )),
            SandboxMode::WorkspaceWrite => Box::new(BubblewrapSandbox::with_writable_paths(
                vec![workspace_root.to_path_buf()],
                true,
            )),
        };

        if directive.controlled || self.controlled_shell {
            Some(Arc::new(ControlledShell::new(sandbox)))
        } else {
            Some(Arc::from(sandbox))
        }
    }

    fn agent_sandbox_directive(&self, agent_name: &str) -> SandboxDirective {
        self.agent_sandboxes
            .get(agent_name)
            .and_then(|value| parse_sandbox_directive(value))
            .unwrap_or(SandboxDirective {
                mode: None,
                controlled: false,
            })
    }
}

#[derive(Debug, Clone)]
struct SessionWorkspace {
    root: PathBuf,
    branch: String,
}

/// The main pipeline orchestrator.
pub struct PipelineOrchestrator {
    config: PipelineConfig,
    storage: Arc<SessionManager>,
    bus: EventBus,
    gate_handler: Arc<dyn GateHandler>,
}

impl PipelineOrchestrator {
    pub async fn new(config: PipelineConfig) -> Result<Self> {
        let storage = Arc::new(SessionManager::open(&config.db_path).await?);
        let bus = EventBus::new(256);
        Ok(Self {
            config,
            storage,
            bus,
            gate_handler: Arc::new(StdinGateHandler),
        })
    }

    pub async fn new_with_gate(
        config: PipelineConfig,
        gate_handler: Arc<dyn GateHandler>,
    ) -> Result<Self> {
        let storage = Arc::new(SessionManager::open(&config.db_path).await?);
        let bus = EventBus::new(256);
        Ok(Self {
            config,
            storage,
            bus,
            gate_handler,
        })
    }

    pub fn event_bus(&self) -> EventBus {
        self.bus.clone()
    }

    pub fn storage_handle(&self) -> Arc<SessionManager> {
        Arc::clone(&self.storage)
    }

    /// Run the full pipeline for a new feature using the preset in [`PipelineConfig`].
    pub async fn run_feature(&self, feature_title: &str) -> Result<String> {
        self.run_feature_with_preset(feature_title, self.config.preset)
            .await
    }

    /// Run the full pipeline for a new feature using the given preset.
    ///
    /// This is the primary entry point for `koklo run --preset <P>`.
    pub async fn run_feature_with_preset(
        &self,
        feature_title: &str,
        preset: PresetKind,
    ) -> Result<String> {
        let mut session = self
            .storage
            .create_session(feature_title, preset.as_str(), &self.config.project_path)
            .await?;
        let session_id = session.id.clone();
        let workspace = match self.ensure_session_workspace(&session).await {
            Ok(workspace) => workspace,
            Err(err) => {
                self.storage
                    .update_session_status(&session_id, "failed")
                    .await
                    .ok();
                return Err(err);
            }
        };
        session.workspace_path = workspace.root.to_string_lossy().into_owned();
        session.workspace_branch = workspace.branch.clone();
        tracing::info!(
            "Pipeline started: session={} feature={} preset={} workspace={}",
            session_id,
            feature_title,
            preset,
            session.workspace_path
        );
        self.storage
            .update_session_status(&session_id, "running")
            .await?;

        // Spawn background task: persist EventBus AgentLog events to SQLite.
        let log_bus = self.bus.clone();
        let log_storage = Arc::clone(&self.storage);
        let log_session_id = session_id.clone();
        let phase_map: HashMap<String, &'static str> = phases_for_preset(preset)
            .into_iter()
            .map(|(p, a)| (p.to_string(), a))
            .collect();

        tokio::spawn(async move {
            let mut rx = log_bus.subscribe();
            while let Ok(event) = rx.recv().await {
                if let PipelineEvent::AgentLog {
                    phase,
                    session_id: _,
                    message,
                } = event
                {
                    let agent_name = phase_map
                        .get(&phase.to_string())
                        .copied()
                        .unwrap_or("unknown");
                    log_storage
                        .record_agent_log(&log_session_id, &phase.to_string(), agent_name, &message)
                        .await
                        .ok();
                }
            }
        });

        let start_time = Utc::now();
        let completed = self
            .run_phases_from(&session, preset, &HashSet::new())
            .await?;
        if !completed {
            let status = self
                .storage
                .get_session(&session_id)
                .await?
                .map(|session| session.status)
                .unwrap_or_else(|| "stopped".to_string());
            tracing::info!(
                "Pipeline stopped early: session={} status={}",
                session_id,
                status
            );
            return Ok(session_id);
        }

        self.bus.send(PipelineEvent::SessionCompleted {
            session_id: session_id.clone(),
        });

        self.storage
            .update_session_status(&session_id, "completed")
            .await?;
        tracing::info!("Pipeline completed: session={}", session_id);

        // Append session summary to .koklo/memories/YYYY-MM-DD.md
        if let Err(e) = self.write_memory_log(&session, preset, start_time).await {
            tracing::warn!("Memory log write failed (non-fatal): {}", e);
        }

        Ok(session_id)
    }

    /// Resume a failed/paused session, skipping already-completed phases.
    ///
    /// The preset is read from the session record so the same workflow
    /// methodology is used as when the session was originally started.
    pub async fn resume(&self, session_id: &str) -> Result<()> {
        let mut session = self
            .storage
            .get_session(session_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Session not found: {}", session_id))?;
        let workspace = self.ensure_session_workspace(&session).await?;
        session.workspace_path = workspace.root.to_string_lossy().into_owned();
        session.workspace_branch = workspace.branch.clone();

        let preset = PresetKind::parse(&session.preset).unwrap_or_else(|| {
            tracing::warn!(
                "Unknown preset '{}' in session {}, falling back to SDD",
                session.preset,
                session_id
            );
            PresetKind::Sdd
        });

        let phase_records = self.storage.get_phases_for_session(session_id).await?;
        let completed: HashSet<String> = phase_records
            .iter()
            .filter(|p| p.status == "completed")
            .map(|p| p.phase.clone())
            .collect();

        tracing::info!(
            "Resuming session {}: preset={} skipping {} completed phase(s)",
            session_id,
            preset,
            completed.len()
        );

        self.storage.clear_session_control(session_id).await?;
        self.storage
            .update_session_status(session_id, "running")
            .await?;
        let finished = self.run_phases_from(&session, preset, &completed).await?;
        if !finished {
            tracing::info!("Session {} paused awaiting user input", session_id);
            return Ok(());
        }
        self.storage
            .update_session_status(session_id, "completed")
            .await?;
        Ok(())
    }

    /// Resume a session from a specific phase, clearing persisted state for that phase and later.
    pub async fn rerun_from_phase(&self, session_id: &str, phase_name: &str) -> Result<()> {
        let mut session = self
            .storage
            .get_session(session_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Session not found: {}", session_id))?;
        let workspace = self.ensure_session_workspace(&session).await?;
        session.workspace_path = workspace.root.to_string_lossy().into_owned();
        session.workspace_branch = workspace.branch.clone();

        let preset = PresetKind::parse(&session.preset).unwrap_or(PresetKind::Sdd);
        let ordered_phases = phases_for_preset(preset);
        let start_index = ordered_phases
            .iter()
            .position(|(phase, _)| phase.to_string() == phase_name)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Phase '{}' does not exist in preset '{}'",
                    phase_name,
                    preset
                )
            })?;

        let completed: HashSet<String> = ordered_phases[..start_index]
            .iter()
            .map(|(phase, _)| phase.to_string())
            .collect();
        let reset: Vec<String> = ordered_phases[start_index..]
            .iter()
            .map(|(phase, _)| phase.to_string())
            .collect();

        self.storage.reset_phases(session_id, &reset).await?;
        self.storage.clear_session_control(session_id).await?;
        self.storage
            .update_session_status(session_id, "running")
            .await?;

        let finished = self.run_phases_from(&session, preset, &completed).await?;
        if !finished {
            tracing::info!("Session {} paused awaiting user input", session_id);
            return Ok(());
        }

        self.storage
            .update_session_status(session_id, "completed")
            .await?;
        Ok(())
    }

    /// Resume from the first failed phase recorded for the session.
    pub async fn rerun_failed(&self, session_id: &str) -> Result<()> {
        let phases = self.storage.get_phases_for_session(session_id).await?;
        let failed = phases
            .iter()
            .find(|phase| phase.status == "failed")
            .map(|phase| phase.phase.clone())
            .ok_or_else(|| {
                anyhow::anyhow!("Session {} has no failed phase to rerun", session_id)
            })?;
        self.rerun_from_phase(session_id, &failed).await
    }

    // ── internals ─────────────────────────────────────────────────────────────

    async fn run_phases_from(
        &self,
        session: &Session,
        preset: PresetKind,
        skip: &HashSet<String>,
    ) -> Result<bool> {
        let phases = phases_for_preset(preset);

        for (phase, agent_name) in &phases {
            if self.handle_pending_session_control(session, None).await? {
                return Ok(false);
            }
            if skip.contains(&phase.to_string()) {
                tracing::info!("Skipping completed phase: {}", phase);
                continue;
            }

            let phase_outcome = match self.run_phase(session, *phase, agent_name).await {
                Ok(result) => result,
                Err(e) => {
                    tracing::error!("Phase {} failed: {}", phase, e);
                    self.bus.send(PipelineEvent::PhaseFailed {
                        phase: *phase,
                        session_id: session.id.to_string(),
                        error: e.to_string(),
                    });
                    self.storage
                        .update_session_status(&session.id, "failed")
                        .await?;
                    return Err(e);
                }
            };

            let PhaseRunOutcome::Completed {
                output,
                usage,
                cost,
            } = phase_outcome;

            if self
                .handle_pending_session_control(session, Some(*phase))
                .await?
            {
                return Ok(false);
            }

            if *phase == Phase::Review {
                if let Some(gh) = &self.config.github {
                    match self
                        .create_github_pr(&session.id, &session.feature_title, &output, gh)
                        .await
                    {
                        Ok(url) => {
                            println!("\nPR created: {}", url);
                            self.bus.send(PipelineEvent::PrCreated {
                                session_id: session.id.to_string(),
                                url: url.clone(),
                                title: format!("feat: {}", session.feature_title),
                            });
                        }
                        Err(e) => {
                            tracing::warn!("PR creation failed (non-fatal): {}", e);
                        }
                    }
                } else {
                    tracing::info!("GITHUB_TOKEN not set — skipping PR creation");
                }
            }

            self.gate(&session.id, *phase, Some(usage), cost).await?;

            if self
                .handle_pending_session_control(session, Some(*phase))
                .await?
            {
                return Ok(false);
            }
        }

        Ok(true)
    }

    async fn run_phase(
        &self,
        session: &Session,
        phase: Phase,
        agent_name: &str,
    ) -> Result<PhaseRunOutcome> {
        self.bus.send(PipelineEvent::PhaseStarted {
            phase,
            session_id: session.id.to_string(),
        });

        let phase_record = self
            .storage
            .create_phase_record(&session.id, &phase.to_string())
            .await?;

        let config = AgentConfig {
            name: agent_name.to_string(),
            phase,
            agent_slug: agent_name.to_string(),
            timeout_secs: 300,
            global_home: self.config.global_home.clone(),
            project_context: self.workspace_project_context(session),
        };

        let workspace_root = self.workspace_root(session);
        let provider = self.config.resolve_provider_for_agent_in_workspace(
            agent_name,
            phase,
            Some(&workspace_root),
        );
        let runner = AgentRunner::new(config, Arc::clone(&provider), self.bus.clone());
        let prompt = format!(
            "Feature: {}\nSession: {}\nWorkspace: {}",
            session.feature_title,
            session.id,
            workspace_root.display()
        );

        // Emit an immediate log entry so the monitor shows activity before the LLM responds.
        self.bus.send(PipelineEvent::AgentLog {
            phase,
            session_id: session.id.clone(),
            message: format!("▶ {} — waiting for {} response…", phase, agent_name),
        });
        let AgentRunResult { output, usage } = runner.run(&session.id, &prompt).await?;

        let artifact_path = self.resolve_artifact_path(&workspace_root, &session.id, phase);
        if let Some(parent) = artifact_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&artifact_path, &output).await?;

        // Record artifact in the database.
        let size_bytes = output.len() as i64;
        self.storage
            .record_artifact(
                &session.id,
                &phase.to_string(),
                &artifact_path.to_string_lossy(),
                size_bytes,
            )
            .await?;

        // Compute cost
        let cost = provider.compute_cost(&usage);
        let (cost_usd, cost_kind) = match &cost {
            Some(CostDisplay::Usd(v)) => (Some(*v), "usd"),
            Some(CostDisplay::Subscription) => (None, "subscription"),
            Some(CostDisplay::Free) => (None, "free"),
            None => (None, "usd"),
        };

        let phase_name = phase.to_string();

        // Record usage in DB
        self.storage
            .record_usage(UsageRecordInput {
                session_id: &session.id,
                phase: &phase_name,
                agent_name,
                provider: provider.provider_name(),
                model: provider.model_name(),
                prompt_tokens: usage.prompt_tokens,
                completion_tokens: usage.completion_tokens,
                cost_usd,
                cost_kind,
            })
            .await?;

        // Record full output for FTS search
        self.storage
            .record_agent_output(&session.id, &phase.to_string(), agent_name, &output)
            .await?;

        // Emit usage event
        self.bus.send(PipelineEvent::UsageUpdate {
            session_id: session.id.clone(),
            phase,
            agent_name: agent_name.to_string(),
            provider: provider.provider_name().to_string(),
            model: provider.model_name().map(|s| s.to_string()),
            prompt_tokens: usage.prompt_tokens,
            completion_tokens: usage.completion_tokens,
            cost: cost.clone(),
        });

        self.storage
            .complete_phase(&phase_record.id, "completed", None)
            .await?;
        self.bus.send(PipelineEvent::PhaseCompleted {
            phase,
            session_id: session.id.to_string(),
        });
        Ok(PhaseRunOutcome::Completed {
            output,
            usage,
            cost,
        })
    }

    async fn create_github_pr(
        &self,
        session_id: &str,
        feature_title: &str,
        reviewer_output: &str,
        gh: &GithubConfig,
    ) -> Result<String> {
        let octo = octocrab::OctocrabBuilder::default()
            .personal_token(gh.token.clone())
            .build()?;

        let branch = format!("koklo/session/{}", session_id);
        let title = format!("feat: {}", feature_title);
        let body = format!(
            "## Summary\n\nAutonomously developed by the Koklo pipeline.\n\n\
             **Session:** `{}`\n\n---\n\n{}",
            session_id, reviewer_output
        );

        let pr = octo
            .pulls(&gh.owner, &gh.repo)
            .create(&title, &branch, &gh.base_branch)
            .body(&body)
            .send()
            .await?;

        let url = pr
            .html_url
            .map(|u| u.to_string())
            .unwrap_or_else(|| format!("https://github.com/{}/{}/pulls", gh.owner, gh.repo));
        Ok(url)
    }

    async fn gate(
        &self,
        session_id: &str,
        phase: Phase,
        usage: Option<CompletionUsage>,
        cost: Option<CostDisplay>,
    ) -> Result<()> {
        let description = format!("Review '{}' phase output and approve to continue.", phase);

        self.bus.send(PipelineEvent::GateRequired {
            phase,
            session_id: session_id.to_string(),
            description: description.clone(),
        });

        let display = GateDisplay {
            phase,
            session_id: session_id.to_string(),
            description,
            usage,
            cost,
        };

        let response = self.gate_handler.handle(display).await?;

        let action_str = match &response {
            GateResponse::Approve => "approve",
            GateResponse::Reject => "reject",
            GateResponse::Edit(_) => "edit",
        };

        self.storage
            .record_gate_decision(session_id, &phase.to_string(), action_str, None)
            .await?;

        let gate_action = match &response {
            GateResponse::Approve => GateAction::Approve,
            GateResponse::Reject => GateAction::Reject,
            GateResponse::Edit(p) => GateAction::Edit(p.clone()),
        };

        self.bus.send(PipelineEvent::GateResolved {
            phase,
            session_id: session_id.to_string(),
            action: gate_action,
        });

        match response {
            GateResponse::Approve => Ok(()),
            _ => {
                self.storage
                    .update_session_status(session_id, "paused")
                    .await?;
                anyhow::bail!("Gate rejected at phase '{}' — session paused", phase)
            }
        }
    }

    async fn handle_pending_session_control(
        &self,
        session: &Session,
        phase: Option<Phase>,
    ) -> Result<bool> {
        let Some(action) = self.storage.take_session_control(&session.id).await? else {
            return Ok(false);
        };
        let Some(action) = SessionControlAction::parse(&action) else {
            tracing::warn!(
                "Ignoring unknown control action '{}' for {}",
                action,
                session.id
            );
            return Ok(false);
        };

        let (status, message) = match action {
            SessionControlAction::Pause => (
                "paused",
                format!(
                    "Session paused{} by user request.",
                    phase
                        .map(|phase| format!(" after phase '{}'", phase))
                        .unwrap_or_else(|| " before next phase".to_string())
                ),
            ),
            SessionControlAction::Cancel => (
                "canceled",
                format!(
                    "Session canceled{} by user request.",
                    phase
                        .map(|phase| format!(" after phase '{}'", phase))
                        .unwrap_or_else(|| " before next phase".to_string())
                ),
            ),
        };

        self.storage
            .update_session_status(&session.id, status)
            .await?;
        self.storage
            .record_agent_log(&session.id, "", "system", &message)
            .await?;
        tracing::info!("{} session={}", message, session.id);
        Ok(true)
    }

    async fn ensure_session_workspace(&self, session: &Session) -> Result<SessionWorkspace> {
        let current_root = self.workspace_root(session);
        if !session.workspace_branch.is_empty() && current_root.exists() {
            return Ok(SessionWorkspace {
                root: current_root,
                branch: session.workspace_branch.clone(),
            });
        }

        if let Some(workspace) =
            self.create_git_workspace(&session.id, Path::new(&session.project_path))?
        {
            self.storage
                .update_session_workspace(
                    &session.id,
                    &workspace.root.to_string_lossy(),
                    &workspace.branch,
                )
                .await?;
            return Ok(workspace);
        }

        if session.workspace_path != session.project_path || !session.workspace_branch.is_empty() {
            self.storage
                .update_session_workspace(&session.id, &session.project_path, "")
                .await?;
        }

        Ok(SessionWorkspace {
            root: PathBuf::from(&session.project_path),
            branch: String::new(),
        })
    }

    fn workspace_root(&self, session: &Session) -> PathBuf {
        if session.workspace_path.is_empty() {
            PathBuf::from(&session.project_path)
        } else {
            PathBuf::from(&session.workspace_path)
        }
    }

    fn workspace_project_context(&self, session: &Session) -> Option<PathBuf> {
        self.config
            .project_context
            .as_ref()
            .map(|_| self.workspace_root(session).join(".koklo"))
    }

    fn resolve_artifact_path(
        &self,
        workspace_root: &Path,
        session_id: &str,
        phase: Phase,
    ) -> PathBuf {
        let base = if self.config.artifacts_dir.is_absolute() {
            self.config.artifacts_dir.clone()
        } else {
            workspace_root.join(&self.config.artifacts_dir)
        };
        base.join(format!("{}-{}.md", session_id, phase))
    }

    fn create_git_workspace(
        &self,
        session_id: &str,
        project_path: &Path,
    ) -> Result<Option<SessionWorkspace>> {
        let Some(repo_root) = self.git_repo_root(project_path)? else {
            return Ok(None);
        };

        let branch = format!("koklo/session/{}", session_id);
        let workspace_root = self
            .config
            .global_home
            .join("worktrees")
            .join(project_workspace_key(&repo_root))
            .join(session_id);

        if workspace_root.exists() {
            return Ok(Some(SessionWorkspace {
                root: workspace_root,
                branch,
            }));
        }

        if let Some(parent) = workspace_root.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let branch_ref = format!("refs/heads/{}", branch);
        let branch_exists = self
            .run_git(
                &repo_root,
                &["show-ref", "--verify", "--quiet", branch_ref.as_str()],
            )?
            .status
            .success();

        let args: Vec<String> = if branch_exists {
            vec![
                "worktree".to_string(),
                "add".to_string(),
                workspace_root.to_string_lossy().into_owned(),
                branch.clone(),
            ]
        } else {
            let base_ref = self
                .run_git(&repo_root, &["rev-parse", "--verify", "HEAD"])?
                .stdout;
            let base_ref = String::from_utf8_lossy(&base_ref).trim().to_string();
            vec![
                "worktree".to_string(),
                "add".to_string(),
                "-b".to_string(),
                branch.clone(),
                workspace_root.to_string_lossy().into_owned(),
                base_ref,
            ]
        };

        let output = self.run_git_dynamic(&repo_root, &args)?;
        if !output.status.success() {
            anyhow::bail!(
                "Failed to create isolated worktree for session {}: {}",
                session_id,
                render_git_error(&output)
            );
        }

        Ok(Some(SessionWorkspace {
            root: workspace_root,
            branch,
        }))
    }

    fn git_repo_root(&self, project_path: &Path) -> Result<Option<PathBuf>> {
        let output = self.run_git(project_path, &["rev-parse", "--show-toplevel"])?;
        if !output.status.success() {
            return Ok(None);
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let root = stdout.trim();
        if root.is_empty() {
            return Ok(None);
        }
        Ok(Some(PathBuf::from(root)))
    }

    fn run_git(&self, dir: &Path, args: &[&str]) -> Result<std::process::Output> {
        Ok(Command::new("git").arg("-C").arg(dir).args(args).output()?)
    }

    fn run_git_dynamic(&self, dir: &Path, args: &[String]) -> Result<std::process::Output> {
        Ok(Command::new("git").arg("-C").arg(dir).args(args).output()?)
    }

    /// Append a session summary to the memories directory.
    ///
    /// Writes to the project memories dir if available, otherwise global.
    async fn write_memory_log(
        &self,
        session: &Session,
        preset: PresetKind,
        start_time: chrono::DateTime<Utc>,
    ) -> Result<()> {
        let ctx_dir = self
            .workspace_project_context(session)
            .unwrap_or_else(|| self.config.global_home.clone());

        let phases = self.storage.get_phases_for_session(&session.id).await?;
        let artifacts = self.storage.list_artifacts(&session.id).await?;

        let agent_map: HashMap<String, &'static str> = phases_for_preset(preset)
            .into_iter()
            .map(|(p, a)| (p.to_string(), a))
            .collect();

        let human_names: HashMap<&str, &str> = [
            ("pm", "John"),
            ("architect", "Winston"),
            ("developer", "Amelia"),
            ("qa", "Quinn"),
            ("reviewer", "Rex"),
            ("analyst", "Mary"),
            ("security", "Nova"),
            ("doc-writer", "Iris"),
            ("constitution-writer", "Sage"),
            ("task-planner", "Bob"),
        ]
        .into_iter()
        .collect();

        let total_secs = (Utc::now() - start_time).num_seconds();
        let duration = format_duration(total_secs);
        let header_time = chrono::Local::now().format("%H:%M");

        let mut content = format!(
            "\n## {}  —  {}  [{} · {} phases · {}]\n\n",
            header_time,
            session.feature_title,
            preset.as_str(),
            phases.len(),
            duration
        );

        for phase in &phases {
            let agent_slug = agent_map.get(&phase.phase).copied().unwrap_or("unknown");
            let human = human_names.get(agent_slug).copied().unwrap_or(agent_slug);

            let phase_dur = phase_duration_str(&phase.started_at, &phase.completed_at);

            let artifact_info = artifacts
                .iter()
                .find(|a| a.phase == phase.phase)
                .map(|a| {
                    let kb = a.size_bytes as f64 / 1024.0;
                    let filename = Path::new(&a.path)
                        .file_name()
                        .and_then(|f| f.to_str())
                        .unwrap_or(&a.path);
                    format!(" → {} ({:.1} KB)", filename, kb)
                })
                .unwrap_or_default();

            let status = if phase.status == "completed" {
                format!("completed in {}{}", phase_dur, artifact_info)
            } else {
                format!(
                    "failed: {}",
                    phase.error.as_deref().unwrap_or("unknown error")
                )
            };

            content.push_str(&format!("- {} ({}): {}\n", phase.phase, human, status));
        }

        let memories_dir = ctx_dir.join("memories");
        tokio::fs::create_dir_all(&memories_dir).await?;
        let today = chrono::Local::now().format("%Y-%m-%d").to_string();
        let memory_file = memories_dir.join(format!("{}.md", today));

        use tokio::io::AsyncWriteExt;
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&memory_file)
            .await?;
        file.write_all(content.as_bytes()).await?;

        tracing::info!("Memory log appended: {}", memory_file.display());
        Ok(())
    }
}

fn project_workspace_key(project_root: &Path) -> String {
    let label = project_root
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or("project");
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    project_root.to_string_lossy().hash(&mut hasher);
    format!(
        "{}-{:016x}",
        sanitize_workspace_component(label),
        hasher.finish()
    )
}

fn sanitize_workspace_component(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut last_dash = false;
    for ch in value.chars() {
        let normalized = if ch.is_ascii_alphanumeric() { ch } else { '-' };
        if normalized == '-' {
            if !last_dash {
                out.push(normalized);
            }
            last_dash = true;
        } else {
            out.push(normalized.to_ascii_lowercase());
            last_dash = false;
        }
    }

    out.trim_matches('-').to_string()
}

fn render_git_error(output: &std::process::Output) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if !stderr.is_empty() {
        return stderr;
    }

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if !stdout.is_empty() {
        return stdout;
    }

    format!("git exited with status {}", output.status)
}

fn default_sandbox_mode_for_phase(phase: Phase) -> SandboxMode {
    match phase {
        Phase::Implement | Phase::Test => SandboxMode::WorkspaceWrite,
        Phase::Spec
        | Phase::Plan
        | Phase::Review
        | Phase::Analysis
        | Phase::Security
        | Phase::Docs
        | Phase::Constitution
        | Phase::Tasks => SandboxMode::ReadOnly,
    }
}

fn parse_sandbox_directive(value: &str) -> Option<SandboxDirective> {
    let normalized = value.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return None;
    }

    let controlled = normalized.contains("controlled");
    let mode = if normalized.contains("workspace-write")
        || normalized.contains("read-write")
        || normalized == "write"
        || normalized == "rw"
    {
        Some(SandboxMode::WorkspaceWrite)
    } else if normalized.contains("read-only")
        || normalized.contains("readonly")
        || normalized == "read"
        || normalized == "ro"
    {
        Some(SandboxMode::ReadOnly)
    } else {
        None
    };

    if mode.is_none() && !controlled {
        return None;
    }

    Some(SandboxDirective { mode, controlled })
}

fn format_duration(secs: i64) -> String {
    if secs < 60 {
        format!("{}s", secs)
    } else {
        format!("{}m {}s", secs / 60, secs % 60)
    }
}

fn phase_duration_str(started_at: &Option<String>, completed_at: &Option<String>) -> String {
    if let (Some(start), Some(end)) = (started_at, completed_at) {
        if let (Ok(s), Ok(e)) = (
            chrono::DateTime::parse_from_rfc3339(start),
            chrono::DateTime::parse_from_rfc3339(end),
        ) {
            return format_duration((e - s).num_seconds());
        }
    }
    "?s".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use koklo_providers::{
        Message, OllamaProvider, OpenRouterProvider, PipelineTomlConfig, StreamChunk,
    };
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    struct RecordingProvider {
        provider_name: &'static str,
        model_name: Option<&'static str>,
        response: String,
        messages: Arc<Mutex<Vec<Message>>>,
    }

    impl RecordingProvider {
        fn new(
            provider_name: &'static str,
            model_name: Option<&'static str>,
            response: impl Into<String>,
        ) -> Self {
            Self {
                provider_name,
                model_name,
                response: response.into(),
                messages: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn recorded_messages(&self) -> Vec<Message> {
            self.messages.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl LlmProvider for RecordingProvider {
        async fn complete_stream(
            &self,
            messages: Vec<Message>,
            on_chunk: &mut (dyn FnMut(StreamChunk) + Send),
        ) -> Result<(String, koklo_events::CompletionUsage)> {
            *self.messages.lock().unwrap() = messages;
            on_chunk(StreamChunk {
                text: self.response.clone(),
                finished: false,
                tool_event: None,
            });
            on_chunk(StreamChunk {
                text: String::new(),
                finished: true,
                tool_event: None,
            });
            Ok((
                self.response.clone(),
                koklo_events::CompletionUsage::default(),
            ))
        }

        fn provider_name(&self) -> &str {
            self.provider_name
        }

        fn model_name(&self) -> Option<&str> {
            self.model_name
        }
    }

    #[test]
    fn test_phase_ordering_sdd() {
        use koklo_events::Phase;
        let phases = presets::phases_for_preset(PresetKind::Sdd);
        assert_eq!(phases[0].0, Phase::Spec);
        assert_eq!(phases[4].0, Phase::Review);
    }

    #[test]
    fn test_phase_ordering_bmad() {
        let phases = presets::phases_for_preset(PresetKind::Bmad);
        assert_eq!(phases.len(), 8);
    }

    #[test]
    fn test_phase_ordering_speckit() {
        let phases = presets::phases_for_preset(PresetKind::SpecKit);
        assert_eq!(phases.len(), 6);
    }

    #[tokio::test]
    async fn test_rerun_failed_requires_failed_phase() {
        let config = make_test_config(Arc::new(OllamaProvider::from_env()), HashMap::new());
        let storage = Arc::new(SessionManager::in_memory().await.unwrap());
        let session = storage.create_session("feature", "sdd", "").await.unwrap();
        let orchestrator = PipelineOrchestrator {
            config,
            storage,
            bus: EventBus::new(16),
            gate_handler: Arc::new(StdinGateHandler),
        };

        let err = orchestrator.rerun_failed(&session.id).await.unwrap_err();
        assert!(err.to_string().contains("no failed phase"));
    }

    #[tokio::test]
    async fn test_handle_pending_pause_control_marks_session_paused() {
        let config = make_test_config(Arc::new(OllamaProvider::from_env()), HashMap::new());
        let storage = Arc::new(SessionManager::in_memory().await.unwrap());
        let session = storage.create_session("feature", "sdd", "").await.unwrap();
        storage
            .update_session_status(&session.id, "running")
            .await
            .unwrap();
        storage
            .request_session_control(&session.id, "pause")
            .await
            .unwrap();
        let orchestrator = PipelineOrchestrator {
            config,
            storage: Arc::clone(&storage),
            bus: EventBus::new(16),
            gate_handler: Arc::new(StdinGateHandler),
        };

        assert!(orchestrator
            .handle_pending_session_control(&session, Some(Phase::Spec))
            .await
            .unwrap());
        assert_eq!(
            storage
                .get_session(&session.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            "paused"
        );
    }

    #[tokio::test]
    async fn test_handle_pending_cancel_control_marks_session_canceled() {
        let config = make_test_config(Arc::new(OllamaProvider::from_env()), HashMap::new());
        let storage = Arc::new(SessionManager::in_memory().await.unwrap());
        let session = storage.create_session("feature", "sdd", "").await.unwrap();
        storage
            .update_session_status(&session.id, "running")
            .await
            .unwrap();
        storage
            .request_session_control(&session.id, "cancel")
            .await
            .unwrap();
        let orchestrator = PipelineOrchestrator {
            config,
            storage: Arc::clone(&storage),
            bus: EventBus::new(16),
            gate_handler: Arc::new(StdinGateHandler),
        };

        assert!(orchestrator
            .handle_pending_session_control(&session, None)
            .await
            .unwrap());
        assert_eq!(
            storage
                .get_session(&session.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            "canceled"
        );
    }

    #[test]
    fn test_github_config_from_env() {
        let saved = std::env::var("GITHUB_TOKEN").ok();

        std::env::remove_var("GITHUB_TOKEN");
        assert!(GithubConfig::from_env().is_none());

        std::env::set_var("GITHUB_TOKEN", "ghp_test");
        std::env::set_var("KOKLO_GITHUB_OWNER", "my-org");
        let cfg = GithubConfig::from_env().unwrap();
        assert_eq!(cfg.owner, "my-org");
        assert_eq!(cfg.base_branch, "develop");

        std::env::remove_var("KOKLO_GITHUB_OWNER");
        match saved {
            Some(v) => std::env::set_var("GITHUB_TOKEN", v),
            None => std::env::remove_var("GITHUB_TOKEN"),
        }
    }

    fn make_test_config(
        default: Arc<dyn LlmProvider>,
        agent_providers: HashMap<String, Arc<dyn LlmProvider>>,
    ) -> PipelineConfig {
        let registry = Arc::new(ProviderRegistry::build(&PipelineTomlConfig::default()).unwrap());
        PipelineConfig {
            db_path: "sqlite://test.db".to_string(),
            artifacts_dir: PathBuf::from("/tmp"),
            global_home: PathBuf::from("/tmp"),
            project_context: None,
            project_path: String::new(),
            preset: PresetKind::Sdd,
            default_provider: default,
            agent_providers,
            agent_sandboxes: HashMap::new(),
            controlled_shell: false,
            provider_registry: registry,
            github: None,
        }
    }

    #[test]
    fn test_resolve_provider_returns_default() {
        let default: Arc<dyn LlmProvider> = Arc::new(OllamaProvider::from_env());
        std::env::remove_var("KOKLO_PROVIDER_PM");
        let cfg = make_test_config(default, HashMap::new());
        let p = cfg.resolve_provider_for_agent("pm");
        assert_eq!(p.provider_name(), "ollama");
    }

    #[test]
    fn test_resolve_provider_agent_toml_override() {
        let default: Arc<dyn LlmProvider> = Arc::new(OllamaProvider::new(
            "http://127.0.0.1:11434",
            "default-model",
        ));
        let pm: Arc<dyn LlmProvider> =
            Arc::new(OllamaProvider::new("http://127.0.0.1:11434", "pm-model"));
        let mut agent_providers = HashMap::new();
        agent_providers.insert("pm".to_string(), pm);
        std::env::remove_var("KOKLO_PROVIDER_PM");
        let cfg = make_test_config(default, agent_providers);
        let p = cfg.resolve_provider_for_agent("pm");
        assert_eq!(p.model_name(), Some("pm-model"));
    }

    #[test]
    fn test_resolve_provider_unknown_env_falls_back() {
        let default: Arc<dyn LlmProvider> = Arc::new(OllamaProvider::from_env());
        std::env::set_var("KOKLO_PROVIDER_PM", "unknown-xyz");
        let cfg = make_test_config(default, HashMap::new());
        let p = cfg.resolve_provider_for_agent("pm");
        assert_eq!(p.provider_name(), "ollama");
        std::env::remove_var("KOKLO_PROVIDER_PM");
    }

    #[test]
    fn test_pipeline_config_default_preset_is_sdd() {
        let cfg = make_test_config(Arc::new(OllamaProvider::from_env()), HashMap::new());
        assert_eq!(cfg.preset, PresetKind::Sdd);
    }

    #[test]
    fn test_resolve_provider_in_workspace_keeps_openrouter_provider() {
        let default: Arc<dyn LlmProvider> = Arc::new(OpenRouterProvider::new(
            "sk-or-test".to_string(),
            "anthropic/claude-sonnet-4".to_string(),
            None,
        ));
        let cfg = make_test_config(default, HashMap::new());
        let provider =
            cfg.resolve_provider_for_agent_in_workspace("pm", Phase::Spec, Some(Path::new("/tmp")));
        assert_eq!(provider.provider_name(), "openrouter");
        assert_eq!(provider.model_name(), Some("anthropic/claude-sonnet-4"));
    }

    #[test]
    fn test_resolve_provider_in_workspace_keeps_ollama_provider() {
        let default: Arc<dyn LlmProvider> = Arc::new(OllamaProvider::new(
            "http://127.0.0.1:11434",
            "qwen2.5-coder:7b",
        ));
        let cfg = make_test_config(default, HashMap::new());
        let provider =
            cfg.resolve_provider_for_agent_in_workspace("pm", Phase::Spec, Some(Path::new("/tmp")));
        assert_eq!(provider.provider_name(), "ollama");
        assert_eq!(provider.model_name(), Some("qwen2.5-coder:7b"));
    }

    #[test]
    fn test_default_sandbox_mode_by_phase() {
        assert_eq!(
            default_sandbox_mode_for_phase(Phase::Spec),
            SandboxMode::ReadOnly
        );
        assert_eq!(
            default_sandbox_mode_for_phase(Phase::Implement),
            SandboxMode::WorkspaceWrite
        );
        assert_eq!(
            default_sandbox_mode_for_phase(Phase::Test),
            SandboxMode::WorkspaceWrite
        );
    }

    #[test]
    fn test_parse_sandbox_directive_variants() {
        assert_eq!(
            parse_sandbox_directive("workspace-write"),
            Some(SandboxDirective {
                mode: Some(SandboxMode::WorkspaceWrite),
                controlled: false,
            })
        );
        assert_eq!(
            parse_sandbox_directive("controlled-readonly"),
            Some(SandboxDirective {
                mode: Some(SandboxMode::ReadOnly),
                controlled: true,
            })
        );
        assert_eq!(
            parse_sandbox_directive("controlled"),
            Some(SandboxDirective {
                mode: None,
                controlled: true,
            })
        );
        assert!(parse_sandbox_directive("nonsense").is_none());
    }

    #[test]
    fn test_agent_sandbox_override_is_applied() {
        let mut cfg = make_test_config(Arc::new(OllamaProvider::from_env()), HashMap::new());
        cfg.agent_sandboxes
            .insert("developer".to_string(), "read-only".to_string());

        let directive = cfg.agent_sandbox_directive("developer");
        assert_eq!(directive.mode, Some(SandboxMode::ReadOnly));
        assert!(!directive.controlled);
    }

    #[tokio::test]
    async fn test_run_phase_uses_workspace_project_context_for_non_cli_providers() {
        use std::fs;

        let tmp = tempfile::tempdir().unwrap();
        let global_home = tmp.path().join("global");
        let project_root = tmp.path().join("project");
        let workspace_root = tmp.path().join("workspace");

        fs::create_dir_all(global_home.join("agents")).unwrap();
        fs::create_dir_all(project_root.join(".koklo").join("agents")).unwrap();
        fs::create_dir_all(workspace_root.join(".koklo").join("agents")).unwrap();

        fs::write(global_home.join("agents").join("pm.md"), "global role").unwrap();
        fs::write(
            project_root.join(".koklo").join("agents").join("pm.md"),
            "main-branch role",
        )
        .unwrap();
        fs::write(
            workspace_root.join(".koklo").join("agents").join("pm.md"),
            "workspace role",
        )
        .unwrap();

        let provider = RecordingProvider::new("openrouter", Some("openai/gpt-4o"), "spec output");
        let provider_handle = provider.clone();
        let config = PipelineConfig {
            db_path: "sqlite::memory:".to_string(),
            artifacts_dir: PathBuf::from("docs/planning_artifacts"),
            global_home: global_home.clone(),
            project_context: Some(project_root.join(".koklo")),
            project_path: project_root.to_string_lossy().into_owned(),
            preset: PresetKind::Sdd,
            default_provider: Arc::new(provider),
            agent_providers: HashMap::new(),
            agent_sandboxes: HashMap::new(),
            controlled_shell: false,
            provider_registry: Arc::new(
                ProviderRegistry::build(&PipelineTomlConfig::default()).unwrap(),
            ),
            github: None,
        };
        let storage = Arc::new(SessionManager::in_memory().await.unwrap());
        let session = storage
            .create_session("feature", "sdd", &project_root.to_string_lossy())
            .await
            .unwrap();
        storage
            .update_session_workspace(
                &session.id,
                &workspace_root.to_string_lossy(),
                "koklo/session/test",
            )
            .await
            .unwrap();
        let session = storage.get_session(&session.id).await.unwrap().unwrap();

        let orchestrator = PipelineOrchestrator {
            config,
            storage,
            bus: EventBus::new(16),
            gate_handler: Arc::new(StdinGateHandler),
        };

        let PhaseRunOutcome::Completed { output, .. } = orchestrator
            .run_phase(&session, Phase::Spec, "pm")
            .await
            .unwrap();
        assert_eq!(output, "spec output");

        let messages = provider_handle.recorded_messages();
        assert_eq!(messages.len(), 2);
        assert!(messages[0].content.contains("workspace role"));
        assert!(!messages[0].content.contains("main-branch role"));
        assert!(messages[1]
            .content
            .contains(&format!("Workspace: {}", workspace_root.display())));

        let artifact_path = workspace_root
            .join("docs")
            .join("planning_artifacts")
            .join(format!("{}-spec.md", session.id));
        assert_eq!(fs::read_to_string(artifact_path).unwrap(), "spec output");
    }

    #[tokio::test]
    async fn test_write_memory_log_uses_workspace_project_context() {
        use std::fs;

        let tmp = tempfile::tempdir().unwrap();
        let global_home = tmp.path().join("global");
        let project_root = tmp.path().join("project");
        let workspace_root = tmp.path().join("workspace");

        fs::create_dir_all(&global_home).unwrap();
        fs::create_dir_all(project_root.join(".koklo")).unwrap();
        fs::create_dir_all(workspace_root.join(".koklo")).unwrap();

        let config = PipelineConfig {
            db_path: "sqlite::memory:".to_string(),
            artifacts_dir: PathBuf::from("docs/planning_artifacts"),
            global_home: global_home.clone(),
            project_context: Some(project_root.join(".koklo")),
            project_path: project_root.to_string_lossy().into_owned(),
            preset: PresetKind::Sdd,
            default_provider: Arc::new(OllamaProvider::new(
                "http://127.0.0.1:11434",
                "qwen2.5-coder:7b",
            )),
            agent_providers: HashMap::new(),
            agent_sandboxes: HashMap::new(),
            controlled_shell: false,
            provider_registry: Arc::new(
                ProviderRegistry::build(&PipelineTomlConfig::default()).unwrap(),
            ),
            github: None,
        };
        let storage = Arc::new(SessionManager::in_memory().await.unwrap());
        let session = storage
            .create_session("feature", "sdd", &project_root.to_string_lossy())
            .await
            .unwrap();
        storage
            .update_session_workspace(
                &session.id,
                &workspace_root.to_string_lossy(),
                "koklo/session/test",
            )
            .await
            .unwrap();
        let session = storage.get_session(&session.id).await.unwrap().unwrap();

        let orchestrator = PipelineOrchestrator {
            config,
            storage,
            bus: EventBus::new(16),
            gate_handler: Arc::new(StdinGateHandler),
        };

        orchestrator
            .write_memory_log(&session, PresetKind::Sdd, Utc::now())
            .await
            .unwrap();

        let today = chrono::Local::now().format("%Y-%m-%d").to_string();
        let workspace_memory = workspace_root
            .join(".koklo")
            .join("memories")
            .join(format!("{today}.md"));
        assert!(workspace_memory.exists());
        assert!(!global_home
            .join("memories")
            .join(format!("{today}.md"))
            .exists());
    }

    #[test]
    fn test_create_git_workspace_creates_dedicated_branch() {
        use std::fs;

        let tmp = tempfile::tempdir().unwrap();
        let repo_root = tmp.path().join("repo");
        let global_home = tmp.path().join("global-home");
        fs::create_dir_all(&repo_root).unwrap();
        fs::create_dir_all(&global_home).unwrap();

        run_test_git(&repo_root, &["init"]).unwrap();
        run_test_git(&repo_root, &["config", "user.email", "koklo@example.test"]).unwrap();
        run_test_git(&repo_root, &["config", "user.name", "Koklo Test"]).unwrap();
        fs::write(repo_root.join("README.md"), "hello").unwrap();
        run_test_git(&repo_root, &["add", "README.md"]).unwrap();
        run_test_git(&repo_root, &["commit", "-m", "init"]).unwrap();

        let config = PipelineConfig {
            db_path: "sqlite::memory:".to_string(),
            artifacts_dir: PathBuf::from("docs/planning_artifacts"),
            global_home: global_home.clone(),
            project_context: Some(repo_root.join(".koklo")),
            project_path: repo_root.to_string_lossy().into_owned(),
            preset: PresetKind::Sdd,
            default_provider: Arc::new(OllamaProvider::new(
                "http://127.0.0.1:11434",
                "qwen2.5-coder:7b",
            )),
            agent_providers: HashMap::new(),
            agent_sandboxes: HashMap::new(),
            controlled_shell: false,
            provider_registry: Arc::new(
                ProviderRegistry::build(&PipelineTomlConfig::default()).unwrap(),
            ),
            github: None,
        };
        let orchestrator = PipelineOrchestrator {
            config,
            storage: Arc::new(
                tokio::runtime::Runtime::new()
                    .unwrap()
                    .block_on(SessionManager::in_memory())
                    .unwrap(),
            ),
            bus: EventBus::new(16),
            gate_handler: Arc::new(StdinGateHandler),
        };

        let workspace = orchestrator
            .create_git_workspace("session-123", &repo_root)
            .unwrap()
            .unwrap();

        assert_eq!(workspace.branch, "koklo/session/session-123");
        assert!(workspace.root.exists());

        let branch_name = String::from_utf8(
            run_test_git(&workspace.root, &["rev-parse", "--abbrev-ref", "HEAD"])
                .unwrap()
                .stdout,
        )
        .unwrap();
        assert_eq!(branch_name.trim(), "koklo/session/session-123");
    }

    fn run_test_git(dir: &Path, args: &[&str]) -> Result<std::process::Output> {
        Ok(Command::new("git").arg("-C").arg(dir).args(args).output()?)
    }

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(0), "0s");
        assert_eq!(format_duration(45), "45s");
        assert_eq!(format_duration(60), "1m 0s");
        assert_eq!(format_duration(125), "2m 5s");
    }
}
