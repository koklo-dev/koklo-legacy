//! Koklo CLI — autonomous AI development pipeline.
//!
//! # Usage
//! ```text
//! koklo [OPTIONS] <COMMAND>
//!
//! Core:
//!   init          Initialize Koklo in the current project
//!   run           Run a workflow pipeline
//!   session       Manage pipeline sessions
//!   agent         Manage and run built-in agents
//!   workflow      List and inspect workflow presets
//!   config        View project configuration
//!   artifacts     Browse pipeline artifacts
//!   provider      Manage LLM providers
//!   monitor       Live TUI dashboard
//!   context       Manage context files
//!
//! Aliases (backward-compat):
//!   status        → session list / session show <id>
//!   resume        → session resume <id>
//! ```

mod home_dirs;
mod md_render;
mod monitor;

use anyhow::Result;
use clap::{Parser, Subcommand};
use koklo_events::GateChannel;
use koklo_providers::registry::build_provider;
use koklo_providers::{
    has_secret, load_secrets_into_env, resolve_secret, ClaudeCodeCliProvider, LlmProvider,
    OllamaProvider, OpenRouterProvider, PipelineTomlConfig, ProviderRegistry, ProviderTomlEntry,
};
use koklo_workflow_engine::{
    presets::{phases_for_preset, PresetKind},
    GateHandler, GithubConfig, PipelineConfig, PipelineOrchestrator, TuiGateHandler,
};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

// ── CLI structure ─────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(
    name = "koklo",
    about = "Autonomous AI development pipeline",
    long_about = "Koklo — spec-driven autonomous development.\n\n\
        Run `koklo workflow list` to see available presets.\n\
        Run `koklo agent list` to see available agents.\n\
        Run `koklo init` to initialise a new project.",
    version
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialise Koklo in the current project (creates .koklo/pipeline.toml).
    ///
    /// Examples:
    ///   koklo init
    ///   koklo init --preset bmad --yes
    ///   koklo .
    Init {
        /// Path to initialise (default: current directory).
        #[arg(value_name = "PATH", default_value = ".")]
        path: PathBuf,

        /// Workflow preset to use for this project.
        #[arg(long, default_value = "sdd", value_parser = parse_preset)]
        preset: PresetKind,

        /// Skip interactive prompts and use defaults.
        #[arg(long, short = 'y')]
        yes: bool,
    },

    /// Run a workflow pipeline.
    ///
    /// Examples:
    ///   koklo run feature "Auth JWT"
    ///   koklo run --preset bmad feature "Add OAuth2"
    ///   koklo run --preset speckit feature "Refactor storage"
    ///   koklo run --preset light task "Fix typo in README"
    ///   koklo run --no-tui feature "Auth JWT"
    Run {
        /// Workflow preset to use.
        #[arg(long, default_value = "sdd", value_parser = parse_preset)]
        preset: PresetKind,

        /// Pipeline type (currently: feature, task, bug).
        #[arg(value_name = "TYPE")]
        pipeline_type: String,

        /// Title / description of the work item.
        #[arg(value_name = "TITLE")]
        title: String,

        /// Disable TUI; use stdin for gates (CI, scripting).
        #[arg(long)]
        no_tui: bool,
    },

    /// Manage pipeline sessions.
    ///
    /// Examples:
    ///   koklo session list
    ///   koklo session show <id>
    ///   koklo session resume <id>
    #[command(subcommand)]
    Session(SessionCommands),

    /// Manage and run built-in agents.
    ///
    /// Examples:
    ///   koklo agent list
    ///   koklo agent show security
    ///   koklo agent run pm --input "Add user authentication"
    #[command(subcommand)]
    Agent(AgentCommands),

    /// List and inspect workflow presets.
    ///
    /// Examples:
    ///   koklo workflow list
    ///   koklo workflow show bmad
    #[command(subcommand)]
    Workflow(WorkflowCommands),

    /// View project configuration.
    ///
    /// Examples:
    ///   koklo config show
    ///   koklo config init --preset sdd --yes
    #[command(subcommand)]
    Config(ConfigCommands),

    /// Browse pipeline artifacts.
    ///
    /// Examples:
    ///   koklo artifacts list <session-id>
    ///   koklo artifacts show <session-id> spec
    #[command(subcommand)]
    Artifacts(ArtifactsCommands),

    /// Manage LLM providers.
    ///
    /// Examples:
    ///   koklo provider list
    ///   koklo provider test ollama
    #[command(subcommand)]
    Provider(ProviderCommands),

    // ── Backward-compatible aliases ───────────────────────────────────────────
    /// [alias] Show status of pipeline sessions (same as `session list/show`).
    Status {
        /// Session ID to inspect (omit for all sessions).
        session_id: Option<String>,
    },

    /// [alias] Resume a paused or failed session (same as `session resume`).
    Resume {
        /// Session ID to resume.
        session_id: String,
    },

    /// Live TUI dashboard showing what agents are doing.
    ///
    /// Examples:
    ///   koklo monitor
    ///   koklo monitor --session <id>
    ///   koklo monitor --follow <id>
    ///   koklo monitor --project .
    Monitor {
        /// Focus on a specific session from launch (prefix match).
        #[arg(long)]
        session: Option<String>,

        /// Plain text stream mode — no TUI, good for CI/scripting.
        #[arg(long, value_name = "SESSION_ID")]
        follow: Option<String>,

        /// Filter sessions to the given project directory (use `.` for current dir).
        #[arg(long, value_name = "PROJECT_DIR")]
        project: Option<String>,
    },

    /// Manage project context files (.koklo/USER.md, MEMORY.md, memories/).
    ///
    /// Examples:
    ///   koklo context show
    ///   koklo context init
    #[command(subcommand)]
    Context(ContextCommands),

    // ── Future stubs ──────────────────────────────────────────────────────────
    /// [coming soon] Integrated ticket system — Phase 5.
    Tickets,
    /// [coming soon] Multi-provider deployment — Phase 10.
    Deploy,
    /// [coming soon] Cloud collaboration sync — Phase 12.
    Sync,
    /// [coming soon] Git constellation visualisation — Phase 9.
    Constellation,
    /// [coming soon] Agent marketplace — Phase 11.
    Marketplace,
    /// [coming soon] Voice input — Phase 8.
    Voice,
    /// [coming soon] IDE bridge — Phase 7.
    Ide,
}

// ── session subcommands ───────────────────────────────────────────────────────

#[derive(Subcommand)]
enum SessionCommands {
    /// List all pipeline sessions.
    List,
    /// Show details of a specific session.
    Show {
        /// Session ID to inspect.
        id: String,
    },
    /// Resume a paused or failed session.
    Resume {
        /// Session ID to resume.
        id: String,
    },
    /// Mark a session as paused in storage.
    Pause {
        /// Session ID to pause.
        id: String,
    },
    /// Cancel a session in storage.
    Cancel {
        /// Session ID to cancel.
        id: String,
    },
    /// Rerun a session from a specific phase.
    Rerun {
        /// Session ID to rerun.
        id: String,
        /// Phase name to restart from (e.g. spec, plan, implement).
        #[arg(long)]
        from: Option<String>,
        /// Restart from the first failed phase.
        #[arg(long)]
        failed: bool,
    },
}

// ── agent subcommands ─────────────────────────────────────────────────────────

#[derive(Subcommand)]
enum AgentCommands {
    /// List all built-in agents.
    List,
    /// Show the system prompt for an agent.
    Show {
        /// Agent name (e.g. pm, architect, security).
        name: String,
    },
    /// Run an agent with the given input.
    Run {
        /// Agent name.
        name: String,
        /// Input prompt (reads from stdin if omitted).
        #[arg(long)]
        input: Option<String>,
    },
}

// ── workflow subcommands ──────────────────────────────────────────────────────

#[derive(Subcommand)]
enum WorkflowCommands {
    /// List all workflow presets.
    List,
    /// Show the phase sequence for a preset.
    Show {
        /// Preset name (sdd, bmad, speckit, light, custom).
        preset: String,
    },
}

// ── config subcommands ────────────────────────────────────────────────────────

#[derive(Subcommand)]
enum ConfigCommands {
    /// Show the current project configuration.
    Show,
    /// Create or update .koklo/pipeline.toml.
    Init {
        /// Workflow preset to configure.
        #[arg(long, default_value = "sdd", value_parser = parse_preset)]
        preset: PresetKind,

        /// Skip interactive prompts.
        #[arg(long, short = 'y')]
        yes: bool,
    },
}

// ── artifacts subcommands ─────────────────────────────────────────────────────

#[derive(Subcommand)]
enum ArtifactsCommands {
    /// List artifacts for a session.
    List {
        /// Session ID.
        session_id: String,
    },
    /// Show the content of a specific phase artifact.
    Show {
        /// Session ID.
        session_id: String,
        /// Phase name (e.g. spec, plan, implement).
        phase: String,
    },
}

// ── provider subcommands ──────────────────────────────────────────────────────

#[derive(Subcommand)]
enum ProviderCommands {
    /// List configured providers.
    List,
    /// Test connectivity to a provider.
    Test {
        /// Provider name (e.g. ollama, openrouter).
        name: String,
    },
    /// Add or update a provider in the config.
    ///
    /// Examples:
    ///   koklo provider add openrouter
    ///   koklo provider add openrouter --model "anthropic/claude-opus-4-6"
    ///   koklo provider add ollama --model qwen2.5-coder:7b --project
    Add {
        /// Provider name (openrouter, ollama, claude-code, codex).
        name: String,
        /// Model name (uses smart default if omitted).
        #[arg(long)]
        model: Option<String>,
        /// Env var holding the API key (uses smart default if omitted).
        #[arg(long)]
        key_env: Option<String>,
        /// Base URL (uses smart default if omitted).
        #[arg(long)]
        base_url: Option<String>,
        /// Write to project .koklo/pipeline.toml instead of global ~/.koklo/config.toml.
        #[arg(long)]
        project: bool,
    },
    /// Remove a provider from the config.
    Remove {
        /// Provider name to remove.
        name: String,
        /// Remove from project config instead of global config.
        #[arg(long)]
        project: bool,
    },
    /// Set the default provider.
    ///
    /// Examples:
    ///   koklo provider set-default claude-code
    ///   koklo provider set-default ollama --project
    SetDefault {
        /// Provider name to set as default.
        name: String,
        /// Write to project config instead of global config.
        #[arg(long)]
        project: bool,
    },
    /// Show API usage for configured providers.
    ///
    /// Examples:
    ///   koklo provider usage
    ///   koklo provider usage openrouter
    Usage {
        /// Show usage for a specific provider only.
        name: Option<String>,
    },
}

// ── context subcommands ───────────────────────────────────────────────────────

#[derive(Subcommand)]
enum ContextCommands {
    /// List which context files exist and preview each one.
    Show,
    /// Create .koklo/USER.md interactively.
    Init,
}

// ── entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let _ = home_dirs::ensure_home();
    load_secrets_into_env();

    let cli = Cli::parse();

    match cli.command {
        Commands::Init { path, preset, yes } => cmd_init(&path, preset, yes).await?,
        Commands::Run {
            preset,
            pipeline_type,
            title,
            no_tui,
        } => cmd_run(preset, &pipeline_type, &title, no_tui).await?,

        Commands::Session(sub) => match sub {
            SessionCommands::List => cmd_session_list().await?,
            SessionCommands::Show { id } => cmd_session_show(&id).await?,
            SessionCommands::Resume { id } => cmd_session_resume(&id).await?,
            SessionCommands::Pause { id } => cmd_session_pause(&id).await?,
            SessionCommands::Cancel { id } => cmd_session_cancel(&id).await?,
            SessionCommands::Rerun { id, from, failed } => {
                cmd_session_rerun(&id, from.as_deref(), failed).await?
            }
        },

        Commands::Agent(sub) => match sub {
            AgentCommands::List => cmd_agent_list().await?,
            AgentCommands::Show { name } => cmd_agent_show(&name).await?,
            AgentCommands::Run { name, input } => cmd_agent_run(&name, input).await?,
        },

        Commands::Workflow(sub) => match sub {
            WorkflowCommands::List => cmd_workflow_list(),
            WorkflowCommands::Show { preset } => cmd_workflow_show(&preset)?,
        },

        Commands::Config(sub) => match sub {
            ConfigCommands::Show => cmd_config_show().await?,
            ConfigCommands::Init { preset, yes } => cmd_config_init(preset, yes).await?,
        },

        Commands::Artifacts(sub) => match sub {
            ArtifactsCommands::List { session_id } => cmd_artifacts_list(&session_id).await?,
            ArtifactsCommands::Show { session_id, phase } => {
                cmd_artifacts_show(&session_id, &phase).await?
            }
        },

        Commands::Provider(sub) => match sub {
            ProviderCommands::List => cmd_provider_list().await?,
            ProviderCommands::Test { name } => cmd_provider_test(&name).await?,
            ProviderCommands::Add {
                name,
                model,
                key_env,
                base_url,
                project,
            } => cmd_provider_add(&name, model, key_env, base_url, project).await?,
            ProviderCommands::Remove { name, project } => {
                cmd_provider_remove(&name, project).await?
            }
            ProviderCommands::SetDefault { name, project } => {
                cmd_provider_set_default(&name, project).await?
            }
            ProviderCommands::Usage { name } => cmd_provider_usage(name).await?,
        },

        Commands::Monitor {
            session,
            follow,
            project,
        } => cmd_monitor(session, follow, project).await?,

        Commands::Context(sub) => match sub {
            ContextCommands::Show => cmd_context_show().await?,
            ContextCommands::Init => cmd_context_init().await?,
        },

        // Backward-compat aliases
        Commands::Status { session_id } => match session_id {
            Some(id) => cmd_session_show(&id).await?,
            None => cmd_session_list().await?,
        },
        Commands::Resume { session_id } => cmd_session_resume(&session_id).await?,

        // Future stubs — informative message, no crash.
        Commands::Tickets => {
            eprintln!(
                "Tickets: coming in Phase 5 (Integrated Ticketing). \
                 See roadmap at https://github.com/koklo-dev/koklo"
            );
        }
        Commands::Deploy => {
            eprintln!(
                "Deploy: coming in Phase 10 (Multi-provider Deployment). \
                 See roadmap at https://github.com/koklo-dev/koklo"
            );
        }
        Commands::Sync => {
            eprintln!(
                "Sync: coming in Phase 12 (Cloud Collaboration). \
                 See roadmap at https://github.com/koklo-dev/koklo"
            );
        }
        Commands::Constellation => {
            eprintln!(
                "Constellation: coming in Phase 9 (Git Visualisation). \
                 See roadmap at https://github.com/koklo-dev/koklo"
            );
        }
        Commands::Marketplace => {
            eprintln!(
                "Marketplace: coming in Phase 11 (Agent Marketplace). \
                 See roadmap at https://github.com/koklo-dev/koklo"
            );
        }
        Commands::Voice => {
            eprintln!(
                "Voice: coming in Phase 8 (Voice Input). \
                 See roadmap at https://github.com/koklo-dev/koklo"
            );
        }
        Commands::Ide => {
            eprintln!(
                "IDE Bridge: coming in Phase 7 (IDE Integration). \
                 See roadmap at https://github.com/koklo-dev/koklo"
            );
        }
    }

    Ok(())
}

// ── preset parser (for clap value_parser) ────────────────────────────────────

fn parse_preset(s: &str) -> Result<PresetKind, String> {
    PresetKind::parse(s).ok_or_else(|| {
        format!(
            "Unknown preset '{}'. Valid: sdd, bmad, speckit, light, custom",
            s
        )
    })
}

// ── orchestrator construction ─────────────────────────────────────────────────

/// Walk up from `cwd` to find the directory containing `.koklo/`.
/// Falls back to `cwd` if not found.
fn find_project_root() -> Result<PathBuf> {
    let mut dir = std::env::current_dir()?;
    loop {
        if dir.join(".koklo").exists() {
            return Ok(dir);
        }
        if !dir.pop() {
            return Ok(std::env::current_dir()?);
        }
    }
}

/// Select the default provider.
///
/// Priority:
/// 0. `toml_default` field from merged config (highest priority)
/// 1. `KOKLO_PROVIDER` env var → registry lookup
/// 2. `OPENROUTER_API_KEY` set → openrouter
/// 3. OllamaProvider (fallback)
fn determine_default_provider(
    registry: &ProviderRegistry,
    toml_default: Option<&str>,
) -> Result<Arc<dyn LlmProvider>> {
    // 0. TOML default_provider (merged pipeline config)
    if let Some(name) = toml_default {
        if let Some(p) = registry.get(name) {
            tracing::info!(
                "Default provider: '{}' (from config default_provider)",
                name
            );
            return Ok(p);
        }
        tracing::warn!(
            "Config default_provider='{}' not in registry, falling back",
            name
        );
    }

    // 1. KOKLO_PROVIDER env var
    if let Ok(name) = std::env::var("KOKLO_PROVIDER") {
        if let Some(p) = registry.get(&name) {
            tracing::info!("Default provider: '{}' (from KOKLO_PROVIDER)", name);
            return Ok(p);
        }
        tracing::warn!("KOKLO_PROVIDER='{}' not in registry, falling back", name);
    }

    // 2. OPENROUTER_API_KEY set → openrouter
    if let Some(api_key) = resolve_secret("OPENROUTER_API_KEY") {
        let p: Arc<dyn LlmProvider> = match registry.get("openrouter") {
            Some(p) => p,
            None => Arc::new(OpenRouterProvider::new(
                api_key,
                "openai/gpt-4o".to_string(),
                None,
            )),
        };
        tracing::info!("Default provider: openrouter (OPENROUTER_API_KEY)");
        return Ok(p);
    }

    // 3. Ollama fallback
    let p: Arc<dyn LlmProvider> = match registry.get("ollama") {
        Some(p) => p,
        None => Arc::new(OllamaProvider::from_env()),
    };
    tracing::info!("Default provider: ollama (fallback)");
    Ok(p)
}

/// Build a `PipelineOrchestrator` from `$KOKLO_HOME/config.toml` + `.koklo/pipeline.toml`
/// (project overrides global) + environment, using the given preset.
async fn build_orchestrator(
    project_root_override: Option<PathBuf>,
    preset_override: Option<PresetKind>,
) -> Result<PipelineOrchestrator> {
    let project_root = match project_root_override {
        Some(path) => path,
        None => find_project_root()?,
    };
    tracing::debug!("Project root: {}", project_root.display());

    let global = home_dirs::load_global_config();
    let project = PipelineTomlConfig::load_from_project_root(&project_root)?;
    let merged = global.merge(project);

    let registry = Arc::new(ProviderRegistry::build(&merged)?);

    let mut agent_providers: HashMap<String, Arc<dyn LlmProvider>> = HashMap::new();
    let mut agent_sandboxes: HashMap<String, String> = HashMap::new();
    for (agent_name, agent_cfg) in &merged.agents {
        if let Some(ref provider_name) = agent_cfg.provider {
            if let Some(p) = registry.get(provider_name) {
                agent_providers.insert(agent_name.clone(), p);
            }
        }
        if let Some(ref sandbox_name) = agent_cfg.sandbox {
            agent_sandboxes.insert(agent_name.clone(), sandbox_name.clone());
        }
    }

    let default_provider =
        determine_default_provider(&registry, merged.pipeline.default_provider.as_deref())?;

    // Preset resolution: explicit override > merged TOML default > SDD
    let preset = preset_override.unwrap_or_else(|| {
        merged
            .workflow
            .preset
            .as_deref()
            .and_then(PresetKind::parse)
            .unwrap_or_default()
    });

    let global_home = home_dirs::koklo_home();
    let project_context_dir = project_root.join(".koklo");
    let project_context = if project_context_dir.exists() {
        Some(project_context_dir)
    } else {
        None
    };

    let config = PipelineConfig {
        db_path: home_dirs::koklo_db_path(),
        artifacts_dir: PathBuf::from(
            merged
                .pipeline
                .artifacts_dir
                .as_deref()
                .unwrap_or("docs/planning_artifacts"),
        ),
        global_home,
        project_context,
        project_path: project_root.to_string_lossy().into_owned(),
        preset,
        default_provider,
        agent_providers,
        agent_sandboxes,
        controlled_shell: std::env::var("KOKLO_CONTROLLED_SHELL")
            .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
            .unwrap_or(false),
        provider_registry: registry,
        github: GithubConfig::from_env(),
    };

    PipelineOrchestrator::new(config).await
}

/// Open the global koklo database (`$KOKLO_HOME/koklo.db`).
async fn open_storage() -> Result<koklo_storage::SessionManager> {
    let db_path = home_dirs::koklo_db_path();
    koklo_storage::SessionManager::open(&db_path).await
}

/// Returns the global agents directory (`$KOKLO_HOME/agents/`).
fn agents_dir() -> PathBuf {
    home_dirs::koklo_home().join("agents")
}

// ── command handlers ──────────────────────────────────────────────────────────

/// `koklo init [PATH] [--preset P] [--yes]`
async fn cmd_init(path: &PathBuf, preset: PresetKind, yes: bool) -> Result<()> {
    let target = if path == &PathBuf::from(".") {
        std::env::current_dir()?
    } else {
        path.clone()
    };

    println!("Initializing koklo...\n");

    // ── Step 1: ensure global home ──────────────────────────────────────────
    let global_home = home_dirs::ensure_home()?;
    println!("Global home: {}/", global_home.display());
    println!("  config.toml    ✓");
    println!("  USER.md        ✓  (edit to tell agents who you are)");
    println!("  koklo.db       will be created on first run");

    // ── Step 2: create project .koklo/ ──────────────────────────────────────
    let koklo_dir = target.join(".koklo");
    let toml_path = koklo_dir.join("pipeline.toml");

    if toml_path.exists() && !yes {
        println!("\nProject: {}", target.display());
        println!("  .koklo/pipeline.toml   already exists");
        println!("\nUse `koklo config init` to reconfigure.");
        return Ok(());
    }

    // Detect project stack for preset suggestion
    let detected_preset = detect_stack_preset(&target);
    let chosen_preset = if !yes && detected_preset != preset {
        println!(
            "\nDetected stack suggests '{}' preset. You specified '{}'. Using '{}'.",
            detected_preset.as_str(),
            preset.as_str(),
            preset.as_str()
        );
        preset
    } else {
        preset
    };

    if !yes {
        println!(
            "\nProject: {}\nPreset:  {} — {}\nCreate .koklo/pipeline.toml? [Y/n] ",
            target.display(),
            chosen_preset.as_str(),
            chosen_preset.display_name()
        );
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        let trimmed = input.trim().to_lowercase();
        if trimmed == "n" || trimmed == "no" {
            println!("Aborted.");
            return Ok(());
        }
    }

    std::fs::create_dir_all(&koklo_dir)?;
    write_default_pipeline_toml(&toml_path, chosen_preset)?;

    // Create PROJECT.md template if it doesn't exist.
    let project_md = koklo_dir.join("PROJECT.md");
    if !project_md.exists() {
        std::fs::write(
            &project_md,
            "# Project Constitution\n\n\
             <!-- Describe this project: tech stack, conventions, goals. -->\n\
             <!-- This file is injected into every agent prompt for this project. -->\n",
        )?;
    }

    println!("\nProject: {}", target.display());
    println!("  .koklo/pipeline.toml   ✓ created");
    println!("  .koklo/PROJECT.md      ✓ created  (edit to add project constitution)");
    println!("\nRun `koklo run feature \"your feature\"` to start.");
    Ok(())
}

/// Detect the likely project stack and return a suggested preset.
fn detect_stack_preset(dir: &Path) -> PresetKind {
    if dir.join("Cargo.toml").exists() {
        return PresetKind::Sdd; // Rust → SDD
    }
    if dir.join("package.json").exists() {
        return PresetKind::SpecKit; // Node/TS → Spec Kit
    }
    if dir.join("pyproject.toml").exists() || dir.join("setup.py").exists() {
        return PresetKind::Sdd; // Python → SDD
    }
    if dir.join("go.mod").exists() {
        return PresetKind::Sdd; // Go → SDD
    }
    PresetKind::Sdd // default
}

/// Write a minimal `.koklo/pipeline.toml` for this project.
///
/// The DB path and agents directory are always global (`$KOKLO_HOME/`) and are
/// not stored in the project config.
fn write_default_pipeline_toml(path: &PathBuf, preset: PresetKind) -> Result<()> {
    let content = format!(
        r#"[pipeline]
artifacts_dir = "docs/planning_artifacts"

[workflow]
preset = "{preset}"

# Provider overrides are optional — global $KOKLO_HOME/config.toml is used by default.
# [providers.openrouter]
# model = "anthropic/claude-opus-4-6"
"#,
        preset = preset.as_str()
    );
    std::fs::write(path, content)?;
    Ok(())
}

/// `koklo run [--preset P] [--no-tui] <type> <title>`
async fn cmd_run(preset: PresetKind, pipeline_type: &str, title: &str, no_tui: bool) -> Result<()> {
    match pipeline_type {
        "feature" | "task" | "bug" => {}
        other => {
            anyhow::bail!(
                "Unknown pipeline type '{}'. Supported: feature, task, bug",
                other
            );
        }
    }

    if no_tui || std::env::var("CI").is_ok() {
        // Original behavior: stdin gates, no TUI
        let orchestrator = build_orchestrator(None, Some(preset)).await?;
        let session_id = orchestrator.run_feature_with_preset(title, preset).await?;
        let storage = open_storage().await?;
        if let Some(session) = storage.get_session(&session_id).await? {
            println!(
                "\nPipeline complete — session: {}\nWorkspace: {}\nBranch: {}",
                session_id,
                session.workspace_path,
                if session.workspace_branch.is_empty() {
                    "(shared project tree)"
                } else {
                    &session.workspace_branch
                }
            );
        } else {
            println!("\nPipeline complete — session: {}", session_id);
        }
        return Ok(());
    }

    // TUI mode
    let gate_channel = GateChannel::new();
    let tui_gate_channel = gate_channel.clone_handle();

    let orch = {
        let gate_handler: Arc<dyn GateHandler> = Arc::new(TuiGateHandler::new(gate_channel));
        build_orchestrator_with_gate(None, Some(preset), gate_handler).await?
    };

    let event_rx = orch.event_bus().subscribe(); // subscribe BEFORE spawn
    let storage = orch.storage_handle();

    // TUI owns stdout; suppress raw agent streaming and route everything
    // through the event bus/monitor instead.
    koklo_agent_runtime::set_stdout_streaming_enabled(false);

    let title_owned = title.to_string();
    let pipeline =
        tokio::spawn(async move { orch.run_feature_with_preset(&title_owned, preset).await });

    // TUI blocks until user quits
    let preset_phase_names: Vec<String> = koklo_workflow_engine::presets::phases_for_preset(preset)
        .into_iter()
        .map(|(phase, _)| phase.to_string())
        .collect();
    monitor::run_integrated_tui(
        storage,
        Some(event_rx),
        Some(tui_gate_channel),
        preset_phase_names,
    )
    .await?;

    // Wait for pipeline to finish
    match pipeline.await {
        Ok(Ok(session_id)) => {
            println!("\nPipeline complete — session: {}", session_id);
        }
        Ok(Err(e)) => {
            eprintln!("\nPipeline error: {}", e);
        }
        Err(e) => {
            eprintln!("\nPipeline task panicked: {}", e);
        }
    }

    Ok(())
}

/// Build a `PipelineOrchestrator` with a custom gate handler.
async fn build_orchestrator_with_gate(
    project_root_override: Option<PathBuf>,
    preset_override: Option<PresetKind>,
    gate_handler: Arc<dyn GateHandler>,
) -> Result<PipelineOrchestrator> {
    let project_root = match project_root_override {
        Some(path) => path,
        None => find_project_root()?,
    };
    tracing::debug!("Project root: {}", project_root.display());

    let global = home_dirs::load_global_config();
    let project = PipelineTomlConfig::load_from_project_root(&project_root)?;
    let merged = global.merge(project);

    let registry = Arc::new(ProviderRegistry::build(&merged)?);

    let mut agent_providers: HashMap<String, Arc<dyn LlmProvider>> = HashMap::new();
    let mut agent_sandboxes: HashMap<String, String> = HashMap::new();
    for (agent_name, agent_cfg) in &merged.agents {
        if let Some(ref provider_name) = agent_cfg.provider {
            if let Some(p) = registry.get(provider_name) {
                agent_providers.insert(agent_name.clone(), p);
            }
        }
        if let Some(ref sandbox_name) = agent_cfg.sandbox {
            agent_sandboxes.insert(agent_name.clone(), sandbox_name.clone());
        }
    }

    let default_provider =
        determine_default_provider(&registry, merged.pipeline.default_provider.as_deref())?;

    let preset = preset_override.unwrap_or_else(|| {
        merged
            .workflow
            .preset
            .as_deref()
            .and_then(PresetKind::parse)
            .unwrap_or_default()
    });

    let global_home = home_dirs::koklo_home();
    let project_context_dir = project_root.join(".koklo");
    let project_context = if project_context_dir.exists() {
        Some(project_context_dir)
    } else {
        None
    };

    let config = PipelineConfig {
        db_path: home_dirs::koklo_db_path(),
        artifacts_dir: PathBuf::from(
            merged
                .pipeline
                .artifacts_dir
                .as_deref()
                .unwrap_or("docs/planning_artifacts"),
        ),
        global_home,
        project_context,
        project_path: project_root.to_string_lossy().into_owned(),
        preset,
        default_provider,
        agent_providers,
        agent_sandboxes,
        controlled_shell: std::env::var("KOKLO_CONTROLLED_SHELL")
            .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
            .unwrap_or(false),
        provider_registry: registry,
        github: GithubConfig::from_env(),
    };

    PipelineOrchestrator::new_with_gate(config, gate_handler).await
}

/// `koklo session list`
async fn cmd_session_list() -> Result<()> {
    let storage = open_storage().await?;
    let sessions = storage.list_sessions().await?;
    if sessions.is_empty() {
        println!("No sessions found.");
    } else {
        println!(
            "{:<38} {:<8} {:<24} {:<10} CONTROL",
            "SESSION ID", "PRESET", "FEATURE", "STATUS"
        );
        println!("{}", "-".repeat(104));
        for s in sessions {
            println!(
                "{:<38} {:<8} {:<24} {:<10} {}",
                s.id,
                s.preset,
                s.feature_title,
                s.status,
                s.control_action.as_deref().unwrap_or("-")
            );
        }
    }
    Ok(())
}

/// `koklo session show <id>`
async fn cmd_session_show(id: &str) -> Result<()> {
    let storage = open_storage().await?;
    match storage.get_session(id).await? {
        Some(s) => {
            println!("Session:  {}", s.id);
            println!("Feature:  {}", s.feature_title);
            println!("Preset:   {}", s.preset);
            println!("Status:   {}", s.status);
            println!(
                "Control:  {}",
                s.control_action.as_deref().unwrap_or("(none)")
            );
            if let Some(requested_at) = &s.control_requested_at {
                println!("Queued:   {}", requested_at);
            }
            println!("Project:  {}", s.project_path);
            println!("Workspace: {}", s.workspace_path);
            println!(
                "Branch:   {}",
                if s.workspace_branch.is_empty() {
                    "(shared project tree)"
                } else {
                    &s.workspace_branch
                }
            );
            println!("Created:  {}", s.created_at);
            println!("Updated:  {}", s.updated_at);
            println!();
            let phases = storage.get_phases_for_session(id).await?;
            if phases.is_empty() {
                println!("No phases recorded.");
            } else {
                println!("{:<14} {:<12} COMPLETED", "PHASE", "STATUS");
                println!("{}", "-".repeat(50));
                for p in phases {
                    println!(
                        "{:<14} {:<12} {}",
                        p.phase,
                        p.status,
                        p.completed_at.as_deref().unwrap_or("-")
                    );
                }
            }
        }
        None => println!("Session not found: {}", id),
    }
    Ok(())
}

/// `koklo session resume <id>`
async fn cmd_session_resume(id: &str) -> Result<()> {
    let storage = open_storage().await?;
    let session = storage
        .get_session(id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Session not found: {}", id))?;
    let orchestrator = build_orchestrator(Some(PathBuf::from(&session.project_path)), None).await?;
    orchestrator.resume(id).await?;
    let session = storage
        .get_session(id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Session not found after resume: {}", id))?;
    println!(
        "\nSession {} resumed and completed.\nWorkspace: {}\nBranch: {}",
        id,
        session.workspace_path,
        if session.workspace_branch.is_empty() {
            "(shared project tree)"
        } else {
            &session.workspace_branch
        }
    );
    Ok(())
}

/// `koklo session pause <id>`
async fn cmd_session_pause(id: &str) -> Result<()> {
    let storage = open_storage().await?;
    let session = storage
        .get_session(id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Session not found: {}", id))?;
    if session.status == "completed" || session.status == "canceled" {
        anyhow::bail!(
            "Session {} cannot be paused from status '{}'",
            id,
            session.status
        );
    }
    if session.status == "running" {
        storage.request_session_control(id, "pause").await?;
        println!(
            "Pause requested for session {}. It will stop after the current phase or gate boundary.",
            id
        );
    } else {
        storage.clear_session_control(id).await?;
        storage.update_session_status(id, "paused").await?;
        println!("Session {} marked as paused.", id);
    }
    Ok(())
}

/// `koklo session cancel <id>`
async fn cmd_session_cancel(id: &str) -> Result<()> {
    let storage = open_storage().await?;
    let session = storage
        .get_session(id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Session not found: {}", id))?;
    if session.status == "completed" {
        anyhow::bail!("Session {} is already completed and cannot be canceled", id);
    }
    if session.status == "running" {
        storage.request_session_control(id, "cancel").await?;
        println!(
            "Cancel requested for session {}. It will stop after the current phase or gate boundary.",
            id
        );
    } else {
        storage.clear_session_control(id).await?;
        storage.update_session_status(id, "canceled").await?;
        println!("Session {} marked as canceled.", id);
    }
    Ok(())
}

/// `koklo session rerun <id> [--from <phase> | --failed]`
async fn cmd_session_rerun(id: &str, from: Option<&str>, failed: bool) -> Result<()> {
    if from.is_some() == failed {
        anyhow::bail!("Choose exactly one of --from <phase> or --failed");
    }

    let storage = open_storage().await?;
    let session = storage
        .get_session(id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Session not found: {}", id))?;
    let orchestrator = build_orchestrator(Some(PathBuf::from(&session.project_path)), None).await?;

    if failed {
        orchestrator.rerun_failed(id).await?;
        println!("Session {} rerun from its first failed phase.", id);
    } else {
        let phase = from.expect("validated above");
        orchestrator.rerun_from_phase(id, phase).await?;
        println!("Session {} rerun from phase '{}'.", id, phase);
    }

    let session = storage
        .get_session(id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Session not found after rerun: {}", id))?;
    println!(
        "Workspace: {}\nBranch: {}",
        session.workspace_path,
        if session.workspace_branch.is_empty() {
            "(shared project tree)"
        } else {
            &session.workspace_branch
        }
    );
    Ok(())
}

/// `koklo agent list`
async fn cmd_agent_list() -> Result<()> {
    // Collect unique agent names across all presets.
    let mut seen = std::collections::BTreeSet::new();
    for &kind in PresetKind::all() {
        for (_, name) in phases_for_preset(kind) {
            seen.insert(name);
        }
    }
    let dir = agents_dir();
    println!("{:<22} SYSTEM PROMPT", "AGENT");
    println!("{}", "-".repeat(60));
    for name in &seen {
        let prompt_path = dir.join(format!("{}.md", name));
        let status = if prompt_path.exists() {
            prompt_path.to_string_lossy().into_owned()
        } else {
            "(prompt file not found)".to_string()
        };
        println!("{:<22} {}", name, status);
    }
    Ok(())
}

/// `koklo agent show <name>` — print the fully assembled system prompt (all layers stacked).
async fn cmd_agent_show(name: &str) -> Result<()> {
    use koklo_agent_runtime::{build_system_prompt, AgentConfig};
    use koklo_events::Phase;

    let global_home = home_dirs::koklo_home();
    let project_root = find_project_root()?;
    let project_context_dir = project_root.join(".koklo");
    let project_context = if project_context_dir.exists() {
        Some(project_context_dir)
    } else {
        None
    };

    let config = AgentConfig {
        name: name.to_string(),
        phase: Phase::Spec, // placeholder — doesn't affect prompt assembly
        agent_slug: name.to_string(),
        timeout_secs: 0,
        global_home,
        project_context,
    };

    let prompt = build_system_prompt(&config)?;
    println!("{}", prompt);
    Ok(())
}

/// `koklo agent run <name> [--input <text>]`
async fn cmd_agent_run(name: &str, input: Option<String>) -> Result<()> {
    let prompt = match input {
        Some(p) => p,
        None => {
            println!("Enter input (Ctrl+D to finish):");
            let mut buf = String::new();
            loop {
                let mut line = String::new();
                let n = std::io::stdin().read_line(&mut line)?;
                if n == 0 {
                    break;
                }
                buf.push_str(&line);
            }
            buf
        }
    };

    // Build a minimal orchestrator to get the provider.
    let project_root = find_project_root()?;
    let global = home_dirs::load_global_config();
    let project_cfg = PipelineTomlConfig::load_from_project_root(&project_root)?;
    let merged = global.merge(project_cfg);
    let registry = Arc::new(ProviderRegistry::build(&merged)?);
    let provider =
        determine_default_provider(&registry, merged.pipeline.default_provider.as_deref())?;

    let dir = agents_dir();
    let system_prompt_file = dir.join(format!("{}.md", name));
    let system_prompt = if system_prompt_file.exists() {
        std::fs::read_to_string(&system_prompt_file)?
    } else {
        format!(
            "You are the {} agent for the koklo AI development pipeline.",
            name
        )
    };

    println!("Running agent '{}'...\n", name);
    use koklo_providers::Message;
    let messages = vec![Message::system(system_prompt), Message::user(prompt)];
    let mut output = String::new();
    let (_text, _usage) = provider
        .complete_stream(messages, &mut |chunk| {
            if !chunk.text.is_empty() {
                print!("{}", chunk.text);
                output.push_str(&chunk.text);
            }
        })
        .await?;
    println!();
    Ok(())
}

/// `koklo workflow list`
fn cmd_workflow_list() {
    println!("{:<10} {:<30} {:<8} REFERENCE", "PRESET", "NAME", "PHASES");
    println!("{}", "-".repeat(75));
    for &kind in PresetKind::all() {
        let phases = phases_for_preset(kind);
        let url = kind.reference_url().unwrap_or("");
        println!(
            "{:<10} {:<30} {:<8} {}",
            kind.as_str(),
            kind.display_name(),
            phases.len(),
            url
        );
    }
}

/// `koklo workflow show <preset>`
fn cmd_workflow_show(preset_str: &str) -> Result<()> {
    let kind = PresetKind::parse(preset_str).ok_or_else(|| {
        anyhow::anyhow!(
            "Unknown preset '{}'. Run `koklo workflow list`.",
            preset_str
        )
    })?;
    let phases = phases_for_preset(kind);
    println!("{} — {}", kind.display_name(), kind.description());
    if let Some(url) = kind.reference_url() {
        println!("Reference: {}", url);
    }
    println!();
    let phase_names: Vec<String> = phases
        .iter()
        .map(|(p, agent)| format!("{} ({})", p, agent))
        .collect();
    println!("{}", phase_names.join(" → "));
    Ok(())
}

/// `koklo config show`
async fn cmd_config_show() -> Result<()> {
    // Global config
    let global_path = home_dirs::koklo_home().join("config.toml");
    println!("# Global: {}", global_path.display());
    if global_path.exists() {
        println!("{}", std::fs::read_to_string(&global_path)?);
    } else {
        println!("(not found — run `koklo init` to create)\n");
    }

    // Project config
    let project_root = find_project_root()?;
    let toml_path = project_root.join(".koklo").join("pipeline.toml");
    println!("# Project: {}", toml_path.display());
    if toml_path.exists() {
        println!("{}", std::fs::read_to_string(&toml_path)?);
    } else {
        println!("(not found — run `koklo init` to create)\n");
    }
    Ok(())
}

/// `koklo config init [--preset P] [--yes]`
async fn cmd_config_init(preset: PresetKind, yes: bool) -> Result<()> {
    let project_root = find_project_root()?;
    cmd_init(&project_root, preset, yes).await
}

/// `koklo artifacts list <session-id>`
async fn cmd_artifacts_list(session_id: &str) -> Result<()> {
    let storage = open_storage().await?;
    let artifacts = storage.list_artifacts(session_id).await?;
    if artifacts.is_empty() {
        println!("No artifacts recorded for session {}.", session_id);
    } else {
        println!("{:<14} {:<12} PATH", "PHASE", "SIZE");
        println!("{}", "-".repeat(70));
        for a in artifacts {
            println!("{:<14} {:<12} {}", a.phase, a.size_bytes, a.path);
        }
    }
    Ok(())
}

/// `koklo artifacts show <session-id> <phase>`
async fn cmd_artifacts_show(session_id: &str, phase: &str) -> Result<()> {
    let storage = open_storage().await?;
    let artifacts = storage.list_artifacts(session_id).await?;
    let artifact = artifacts
        .into_iter()
        .find(|a| a.phase == phase)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "No artifact for phase '{}' in session '{}'",
                phase,
                session_id
            )
        })?;
    let content = std::fs::read_to_string(&artifact.path)
        .map_err(|e| anyhow::anyhow!("Cannot read {}: {}", artifact.path, e))?;
    println!("{}", content);
    Ok(())
}

/// `koklo provider list`
async fn cmd_provider_list() -> Result<()> {
    let global = home_dirs::load_global_config();
    let project_root = find_project_root()?;
    let project = PipelineTomlConfig::load_from_project_root(&project_root)?;
    let merged = global.clone().merge(project.clone());

    if merged.providers.is_empty() {
        println!("No providers configured.");
        println!("Run `koklo provider add <name>` or edit $KOKLO_HOME/config.toml.");
        return Ok(());
    }

    let default_name = merged.pipeline.default_provider.as_deref();

    println!("{:<22} {:<30} {:<10} STATUS", "NAME", "MODEL", "SOURCE");
    println!("{}", "─".repeat(75));
    let mut names: Vec<&String> = merged.providers.keys().collect();
    names.sort();
    for name in names {
        let entry = &merged.providers[name];
        let source = if project.providers.contains_key(name) {
            "project"
        } else {
            "global"
        };
        let model = entry.model.as_deref().unwrap_or("-");
        let is_default = default_name == Some(name.as_str());
        let display_name = if is_default {
            format!("{} *", name)
        } else {
            name.clone()
        };
        let status = if let Some(ref key_env) = entry.api_key_env {
            if has_secret(key_env) {
                "configured"
            } else {
                "missing key"
            }
        } else {
            "configured"
        };
        println!(
            "{:<22} {:<30} {:<10} {}",
            display_name, model, source, status
        );
    }
    if default_name.is_some() {
        println!();
        println!(
            "  * = default provider  |  global = $KOKLO_HOME/config.toml  |  project = .koklo/pipeline.toml"
        );
    }
    Ok(())
}

/// `koklo monitor [--session <id>] [--follow <id>] [--project <dir>]`
async fn cmd_monitor(
    session: Option<String>,
    follow: Option<String>,
    project: Option<String>,
) -> Result<()> {
    let storage = std::sync::Arc::new(open_storage().await?);
    let (session_filter, follow_mode) = if let Some(id) = follow {
        (Some(id), true)
    } else {
        (session, false)
    };

    // Resolve --project . to an absolute path.
    let project_filter = if let Some(ref p) = project {
        let resolved = if p == "." {
            find_project_root().unwrap_or_else(|_| std::env::current_dir().unwrap())
        } else {
            PathBuf::from(p)
                .canonicalize()
                .unwrap_or_else(|_| PathBuf::from(p))
        };
        Some(resolved.to_string_lossy().into_owned())
    } else {
        None
    };

    monitor::run_monitor(session_filter, follow_mode, project_filter, storage).await
}

/// `koklo context show`
async fn cmd_context_show() -> Result<()> {
    let global_home = home_dirs::koklo_home();
    let project_root = find_project_root()?;
    let koklo_dir = project_root.join(".koklo");

    // ── Global context ──────────────────────────────────────────────────────
    println!("Global context: {}/", global_home.display());
    for (file, desc) in &[
        ("USER.md", "Who the user is"),
        ("MEMORY.md", "Long-term memory"),
    ] {
        let path = global_home.join(file);
        if path.exists() {
            let content = std::fs::read_to_string(&path)?;
            let first_line = content.lines().next().unwrap_or("(empty)");
            println!("  {} — {} ✓", file, desc);
            println!("    {}", first_line);
        } else {
            println!("  {} — {} (not found)", file, desc);
        }
    }
    let global_memories = global_home.join("memories");
    if global_memories.exists() {
        let count = std::fs::read_dir(&global_memories)
            .map(|d| d.count())
            .unwrap_or(0);
        println!("  memories/ ({} files)", count);
    } else {
        println!("  memories/ (no logs yet)");
    }

    // ── Project context ─────────────────────────────────────────────────────
    println!();
    if koklo_dir.exists() {
        println!("Project context: {}/", koklo_dir.display());
        for (file, desc) in &[
            ("PROJECT.md", "Project constitution"),
            ("MEMORY.md", "Project memory"),
        ] {
            let path = koklo_dir.join(file);
            if path.exists() {
                let content = std::fs::read_to_string(&path)?;
                let first_line = content.lines().next().unwrap_or("(empty)");
                println!("  {} — {} ✓", file, desc);
                println!("    {}", first_line);
            } else {
                println!("  {} — {} (not found)", file, desc);
            }
        }
        let proj_memories = koklo_dir.join("memories");
        if proj_memories.exists() {
            let mut entries: Vec<_> = std::fs::read_dir(&proj_memories)?
                .filter_map(|e| e.ok())
                .collect();
            entries.sort_by_key(|e| e.file_name());
            println!("  memories/ ({} files)", entries.len());
            for entry in entries.iter().rev().take(3) {
                println!("    {}", entry.file_name().to_string_lossy());
            }
            if entries.len() > 3 {
                println!("    ... and {} more", entries.len() - 3);
            }
        } else {
            println!("  memories/ (no project session logs yet)");
        }
    } else {
        println!(
            "Project context: (none — no .koklo/ in {})",
            project_root.display()
        );
        println!("  Run `koklo init` to create one.");
    }

    Ok(())
}

/// `koklo context init`
async fn cmd_context_init() -> Result<()> {
    let project_root = find_project_root()?;
    let koklo_dir = project_root.join(".koklo");
    std::fs::create_dir_all(&koklo_dir)?;

    let user_md = koklo_dir.join("USER.md");
    if user_md.exists() {
        println!("USER.md already exists: {}", user_md.display());
        println!("Edit it directly to update your user context.");
        return Ok(());
    }

    println!("Creating .koklo/USER.md");
    println!(
        "This file is injected into every agent prompt so agents know who they're working with."
    );
    println!();

    println!("Your name (or handle): ");
    let mut name = String::new();
    std::io::stdin().read_line(&mut name)?;
    let name = name.trim().to_string();

    println!("Your role/title (e.g. 'Senior Rust Engineer', 'indie hacker'): ");
    let mut role = String::new();
    std::io::stdin().read_line(&mut role)?;
    let role = role.trim().to_string();

    println!("Your main stack/languages (e.g. 'Rust, TypeScript, Python'): ");
    let mut stack = String::new();
    std::io::stdin().read_line(&mut stack)?;
    let stack = stack.trim().to_string();

    let content = format!(
        "# User Context\n\nName: {}\nRole: {}\nStack: {}\n\n\
         ## Preferences\n\n\
         - Prefer concise, direct explanations\n\
         - Show me the code, not just the theory\n\
         - Flag trade-offs explicitly\n",
        if name.is_empty() { "Unknown" } else { &name },
        if role.is_empty() { "Developer" } else { &role },
        if stack.is_empty() {
            "Not specified"
        } else {
            &stack
        },
    );

    std::fs::write(&user_md, &content)?;
    println!("\nCreated: {}", user_md.display());
    println!("Edit this file anytime to update what agents know about you.");

    // Also create empty MEMORY.md if it doesn't exist
    let memory_md = koklo_dir.join("MEMORY.md");
    if !memory_md.exists() {
        std::fs::write(
            &memory_md,
            "# Project Memory\n\n\
             Add hand-curated notes here. This file is injected into every agent prompt.\n\
             Keep it concise — agents read this on every pipeline run.\n",
        )?;
        println!("Created: {}", memory_md.display());
    }

    Ok(())
}

/// `koklo provider test <name>`
async fn cmd_provider_test(name: &str) -> Result<()> {
    let global = home_dirs::load_global_config();
    let project_root = find_project_root()?;
    let project = PipelineTomlConfig::load_from_project_root(&project_root)?;
    let merged = global.merge(project);
    let smoke_dir = if name == "claude-code" {
        Some(tempfile::tempdir()?)
    } else {
        None
    };
    let provider: Arc<dyn LlmProvider> = if let Some(dir) = &smoke_dir {
        Arc::new(ClaudeCodeCliProvider::with_working_dir(
            dir.path().to_path_buf(),
        )?)
    } else {
        let entry = merged
            .providers
            .get(name)
            .ok_or_else(|| anyhow::anyhow!("Provider '{}' is not configured.", name))?;
        let mut smoke_entry = entry.clone();
        if let Some(smoke_model) = &entry.smoke_model {
            smoke_entry.model = Some(smoke_model.clone());
        }
        build_provider(name, &smoke_entry)?
    };

    println!("Testing provider '{}'...", name);
    use koklo_providers::Message;
    let messages = vec![Message::user("Reply with exactly: OK".to_string())];
    match provider
        .complete_stream(messages, &mut |chunk| {
            if !chunk.text.is_empty() {
                print!("{}", chunk.text);
            }
        })
        .await
    {
        Ok(_) => {
            println!("\nProvider '{}': OK", name);
        }
        Err(e) => {
            eprintln!("\nProvider '{}' failed: {}", name, e);
        }
    }
    Ok(())
}

/// `koklo provider add <name> [--model M] [--key-env K] [--base-url U] [--project]`
async fn cmd_provider_add(
    name: &str,
    model: Option<String>,
    key_env: Option<String>,
    base_url: Option<String>,
    project: bool,
) -> Result<()> {
    // Guard: --key-env takes an env var NAME (e.g. OPENROUTER_API_KEY), not the key value.
    if let Some(ref k) = key_env {
        if looks_like_api_key(k) {
            anyhow::bail!(
                "'{}' looks like an API key value, not an env var name.\n\
                 --key-env expects the NAME of the environment variable that holds the key.\n\
                 \n\
                 Usage:\n\
                 \n\
                   export OPENROUTER_API_KEY='{}'\n\
                   koklo provider add {} [--key-env OPENROUTER_API_KEY]\n\
                 \n\
                 Or use a custom var name:\n\
                 \n\
                   export MY_KEY='{}'\n\
                   koklo provider add {} --key-env MY_KEY",
                k,
                k,
                name,
                k,
                name
            );
        }
    }

    // Guard: --key-env takes an env var NAME (e.g. OPENROUTER_API_KEY), not the key value.
    if let Some(ref k) = key_env {
        if looks_like_api_key(k) {
            anyhow::bail!(
                "'{}' looks like an API key value, not an env var name.\n\
                 --key-env expects the NAME of the environment variable that holds the key.\n\
                 \n\
                 Usage:\n\
                 \n\
                   export OPENROUTER_API_KEY='{}'\n\
                   koklo provider add {} [--key-env OPENROUTER_API_KEY]\n\
                 \n\
                 Or use a custom var name:\n\
                 \n\
                   export MY_KEY='{}'\n\
                   koklo provider add {} --key-env MY_KEY",
                k,
                k,
                name,
                k,
                name
            );
        }
    }

    // Smart defaults per known provider name
    let (default_key_env, default_model, default_smoke_model, default_base_url): (
        Option<&str>,
        Option<&str>,
        Option<&str>,
        Option<&str>,
    ) = match name {
        "openrouter" => (
            Some("OPENROUTER_API_KEY"),
            Some("openai/gpt-4o"),
            Some("google/gemma-3-4b-it:free"),
            None,
        ),
        "ollama" => (None, Some("llama3.2"), None, Some("http://localhost:11434")),
        "claude-code" | "codex" => (None, None, None, None),
        _ => (None, None, None, None),
    };

    let entry = ProviderTomlEntry {
        api_key_env: key_env.or_else(|| default_key_env.map(String::from)),
        model: model.or_else(|| default_model.map(String::from)),
        smoke_model: default_smoke_model.map(String::from),
        base_url: base_url.or_else(|| default_base_url.map(String::from)),
        ..Default::default()
    };

    let (config_path, mut config) = load_writable_config(project)?;

    config.providers.insert(name.to_string(), entry);

    write_config(&config_path, &config)?;
    println!("Added provider '{}' to {}", name, config_path.display());
    Ok(())
}

/// Returns true if `s` looks like an API key value rather than an env var name.
/// Env var names are typically UPPER_SNAKE_CASE; keys often start with known
/// prefixes (sk-, sk-or-, pk-, ...) or contain lowercase letters.
fn looks_like_api_key(s: &str) -> bool {
    let key_prefixes = ["sk-", "pk-", "ak-", "key-", "Bearer "];
    if key_prefixes.iter().any(|p| s.starts_with(p)) {
        return true;
    }
    // Env var names are [A-Z0-9_] only; anything with lowercase or other chars is suspicious
    s.chars().any(|c| c.is_lowercase())
}

/// `koklo provider remove <name> [--project]`
async fn cmd_provider_remove(name: &str, project: bool) -> Result<()> {
    let (config_path, mut config) = load_writable_config(project)?;

    if config.providers.remove(name).is_none() {
        println!("Provider '{}' not found in config.", name);
        return Ok(());
    }

    write_config(&config_path, &config)?;
    println!("Removed provider '{}' from {}", name, config_path.display());
    Ok(())
}

/// `koklo provider set-default <name> [--project]`
async fn cmd_provider_set_default(name: &str, project: bool) -> Result<()> {
    let (config_path, mut config) = load_writable_config(project)?;
    config.pipeline.default_provider = Some(name.to_string());
    write_config(&config_path, &config)?;
    println!(
        "Default provider set to '{}' in {}",
        name,
        config_path.display()
    );
    Ok(())
}

/// `koklo provider usage [name]`
async fn cmd_provider_usage(name: Option<String>) -> Result<()> {
    let global = home_dirs::load_global_config();
    let project_root = find_project_root()?;
    let project = PipelineTomlConfig::load_from_project_root(&project_root)?;
    let merged = global.merge(project);

    if merged.providers.is_empty() {
        println!("No providers configured. Run `koklo provider add <name>`.");
        return Ok(());
    }

    let names_to_show: Vec<String> = if let Some(n) = name {
        vec![n]
    } else {
        let mut keys: Vec<String> = merged.providers.keys().cloned().collect();
        keys.sort();
        keys
    };

    println!("{:<14} {:<16} {:<12} TIER", "PROVIDER", "USAGE", "LIMIT");
    println!("{}", "─".repeat(55));

    for pname in &names_to_show {
        if pname == "openrouter" {
            if let Some(entry) = merged.providers.get(pname) {
                let key_env = entry.api_key_env.as_deref().unwrap_or("OPENROUTER_API_KEY");
                // Detect misconfiguration: api_key_env was set to the key value, not a var name
                if looks_like_api_key(key_env) {
                    println!(
                        "{:<14} misconfigured — api_key_env contains a key value, not a var name",
                        pname
                    );
                    println!("  Fix:  koklo provider remove openrouter");
                    println!("        export OPENROUTER_API_KEY='<your-key>'");
                    println!("        koklo provider add openrouter");
                    continue;
                }
                match std::env::var(key_env) {
                    Ok(api_key) => match fetch_openrouter_usage(&api_key).await {
                        Ok(info) => {
                            let usage = format!("${:.2}", info.usage);
                            let limit = info
                                .limit
                                .map(|l| format!("${:.2}", l))
                                .unwrap_or_else(|| "unlimited".to_string());
                            let tier = if info.is_free_tier { "free" } else { "paid" };
                            println!("{:<14} {:<16} {:<12} {}", pname, usage, limit, tier);
                        }
                        Err(e) => {
                            println!("{:<14} error: {}", pname, e);
                        }
                    },
                    Err(_) => {
                        println!("{:<14} env var {} is not set", pname, key_env);
                        println!("  Fix:  export {}='<your-key>'", key_env);
                    }
                }
            } else {
                println!("{:<14} not configured", pname);
            }
        } else {
            println!("{:<14} local — no usage data", pname);
        }
    }
    Ok(())
}

struct OpenRouterKeyInfo {
    usage: f64,
    limit: Option<f64>,
    is_free_tier: bool,
}

async fn fetch_openrouter_usage(api_key: &str) -> Result<OpenRouterKeyInfo> {
    let client = reqwest::Client::new();
    let resp = client
        .get("https://openrouter.ai/api/v1/auth/key")
        .header("Authorization", format!("Bearer {}", api_key))
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("OpenRouter request failed: {}", e))?;

    let json: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| anyhow::anyhow!("OpenRouter response parse failed: {}", e))?;
    let data = &json["data"];

    Ok(OpenRouterKeyInfo {
        usage: data["usage"].as_f64().unwrap_or(0.0),
        limit: data["limit"].as_f64(),
        is_free_tier: data["is_free_tier"].as_bool().unwrap_or(true),
    })
}

/// Load the config file and its path for write operations.
/// If `project` is true, uses `.koklo/pipeline.toml`; otherwise `$KOKLO_HOME/config.toml`.
fn load_writable_config(project: bool) -> Result<(PathBuf, PipelineTomlConfig)> {
    if project {
        let project_root = find_project_root()?;
        let path = project_root.join(".koklo").join("pipeline.toml");
        let config = PipelineTomlConfig::load_from_project_root(&project_root)?;
        Ok((path, config))
    } else {
        let path = home_dirs::koklo_home().join("config.toml");
        let config = home_dirs::load_global_config();
        Ok((path, config))
    }
}

/// Serialize a `PipelineTomlConfig` and write it to `path`.
fn write_config(path: &PathBuf, config: &PipelineTomlConfig) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let toml_str = toml::to_string_pretty(config)
        .map_err(|e| anyhow::anyhow!("Failed to serialize config: {}", e))?;
    std::fs::write(path, toml_str)?;
    Ok(())
}
