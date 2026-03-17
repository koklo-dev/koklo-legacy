//! `koklo monitor` — live TUI dashboard for pipeline activity.
//!
//! Polls the SQLite database every 500 ms.  Two display modes:
//! - Default: ratatui TUI with sessions + phases + log panels
//! - `--follow`: plain text stream (for CI / scripting)

use anyhow::Result;
use koklo_events::{CostDisplay, GateChannel, GateDisplay, GateResponse, PipelineEvent};
use koklo_storage::{AgentLogRecord, PhaseRecord, Session, SessionManager, SessionUsageSummary};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Clear, List, ListItem, Paragraph, Row, Table},
    Frame, Terminal,
};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, oneshot};

use crossterm::{
    event::{self, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};

#[derive(PartialEq, Clone)]
enum Panel {
    Sessions,
    Phases,
    Log,
}

#[derive(PartialEq, Clone, Debug)]
enum TuiMode {
    Live,
    GateOverlay,
    Summary,
    CommandPrompt,
    HelpOverlay,
}

const SPINNER: &[&str] = &["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];

#[derive(Debug, PartialEq, Eq)]
enum MonitorCommand {
    Help,
    Status,
    Live,
    Summary,
    Approve,
    Reject,
    Pause,
    Cancel,
    Refresh,
    FocusSessions,
    FocusPhases,
    FocusLogs,
    Session(String),
    Phase(String),
}

fn parse_monitor_command(input: &str) -> Result<MonitorCommand, String> {
    let trimmed = input.trim();
    let trimmed = trimmed
        .strip_prefix('/')
        .or_else(|| trimmed.strip_prefix(':'))
        .unwrap_or(trimmed)
        .trim();

    if trimmed.is_empty() {
        return Err("Empty command. Try /help.".to_string());
    }

    let mut parts = trimmed.split_whitespace();
    let name = parts.next().unwrap_or_default().to_ascii_lowercase();
    let arg = parts.collect::<Vec<_>>().join(" ");

    match name.as_str() {
        "help" => Ok(MonitorCommand::Help),
        "status" => Ok(MonitorCommand::Status),
        "live" => Ok(MonitorCommand::Live),
        "summary" => Ok(MonitorCommand::Summary),
        "approve" => Ok(MonitorCommand::Approve),
        "reject" => Ok(MonitorCommand::Reject),
        "pause" => Ok(MonitorCommand::Pause),
        "cancel" => Ok(MonitorCommand::Cancel),
        "refresh" => Ok(MonitorCommand::Refresh),
        "sessions" => Ok(MonitorCommand::FocusSessions),
        "phases" => Ok(MonitorCommand::FocusPhases),
        "logs" | "log" => Ok(MonitorCommand::FocusLogs),
        "session" => {
            if arg.is_empty() {
                Err("Usage: /session <id-prefix|title-fragment>".to_string())
            } else {
                Ok(MonitorCommand::Session(arg))
            }
        }
        "phase" => {
            if arg.is_empty() {
                Err("Usage: /phase <phase-name>".to_string())
            } else {
                Ok(MonitorCommand::Phase(arg))
            }
        }
        _ => Err(format!("Unknown command '/{}'. Try /help.", name)),
    }
}

/// Live-updating TUI monitor application.
pub struct MonitorApp {
    sessions: Vec<Session>,
    phases: Vec<PhaseRecord>,
    logs: Vec<AgentLogRecord>,
    selected_session: usize,
    last_seq: i64,
    focus: Panel,
    storage: Arc<SessionManager>,
    /// When `Some`, only sessions from this project path are shown.
    project_filter: Option<String>,
    /// Incremented on every tick to drive spinner animation.
    tick_count: usize,
    /// Index of the phase selected in the Phases panel. `None` = follow live phase.
    selected_phase: Option<usize>,
    // New fields for integrated TUI mode:
    event_rx: Option<broadcast::Receiver<PipelineEvent>>,
    gate_channel: Option<GateChannel>,
    pending_gate_display: Option<GateDisplay>,
    pending_gate_responder: Option<oneshot::Sender<GateResponse>>,
    running_tokens: u64,
    running_cost: Option<f64>,
    has_subscription_cost: bool,
    mode: TuiMode,
    session_usage: Option<SessionUsageSummary>,
    live_session_id: Option<String>,
    /// Preset phase names in order — used to pre-populate the phase panel as "pending".
    preset_phase_names: Vec<String>,
    /// Buffer for incomplete lines from streaming AgentLog chunks.
    /// SSE providers emit tiny tokens (e.g. "Hello", ",", " world") that don't
    /// end with newlines.  We accumulate here until we see `\n`, then flush as
    /// a complete log record.
    line_buffer: String,
    /// Session ID associated with the current line buffer.
    line_buffer_session: String,
    /// Slash-command input buffer.
    command_input: String,
    /// Last command/result message shown in the status bar.
    status_message: Option<String>,
}

impl MonitorApp {
    pub async fn new(
        session_filter: Option<&str>,
        project_filter: Option<String>,
        storage: Arc<SessionManager>,
    ) -> Result<Self> {
        let sessions = Self::load_sessions(&storage, project_filter.as_deref()).await?;
        let selected_session = if let Some(filter) = session_filter {
            sessions
                .iter()
                .position(|s| s.id.starts_with(filter))
                .unwrap_or(0)
        } else {
            0
        };

        let (phases, logs) = if !sessions.is_empty() {
            let sid = &sessions[selected_session].id;
            let p = storage.get_phases_for_session(sid).await?;
            let l = storage.get_agent_logs_for_session(sid).await?;
            (p, l)
        } else {
            (vec![], vec![])
        };

        let last_seq = logs.last().map(|l| l.seq).unwrap_or(0);

        Ok(Self {
            sessions,
            phases,
            logs,
            selected_session,
            last_seq,
            focus: Panel::Sessions,
            storage,
            project_filter,
            tick_count: 0,
            selected_phase: None,
            event_rx: None,
            gate_channel: None,
            pending_gate_display: None,
            pending_gate_responder: None,
            running_tokens: 0,
            running_cost: None,
            has_subscription_cost: false,
            mode: TuiMode::Live,
            session_usage: None,
            live_session_id: None,
            preset_phase_names: vec![],
            line_buffer: String::new(),
            line_buffer_session: String::new(),
            command_input: String::new(),
            status_message: None,
        })
    }

    pub async fn new_integrated(
        storage: Arc<SessionManager>,
        event_rx: Option<broadcast::Receiver<PipelineEvent>>,
        gate_channel: Option<GateChannel>,
        session_filter: Option<&str>,
        project_filter: Option<String>,
        preset_phases: Vec<String>,
    ) -> Result<Self> {
        let sessions = Self::load_sessions(&storage, project_filter.as_deref()).await?;
        let selected_session = if let Some(filter) = session_filter {
            sessions
                .iter()
                .position(|s| s.id.starts_with(filter))
                .unwrap_or(0)
        } else {
            0
        };
        let (phases, logs) = if !sessions.is_empty() {
            let sid = &sessions[selected_session].id;
            let p = storage.get_phases_for_session(sid).await?;
            let l = storage.get_agent_logs_for_session(sid).await?;
            (p, l)
        } else {
            (vec![], vec![])
        };
        let last_seq = logs.last().map(|l| l.seq).unwrap_or(0);
        Ok(Self {
            sessions,
            phases,
            logs,
            selected_session,
            last_seq,
            focus: Panel::Sessions,
            storage,
            project_filter,
            tick_count: 0,
            selected_phase: None,
            event_rx,
            gate_channel,
            pending_gate_display: None,
            pending_gate_responder: None,
            running_tokens: 0,
            running_cost: None,
            has_subscription_cost: false,
            mode: TuiMode::Live,
            session_usage: None,
            live_session_id: None,
            preset_phase_names: preset_phases,
            line_buffer: String::new(),
            line_buffer_session: String::new(),
            command_input: String::new(),
            status_message: None,
        })
    }

    pub(crate) async fn load_sessions(
        storage: &SessionManager,
        project_filter: Option<&str>,
    ) -> Result<Vec<Session>> {
        if let Some(path) = project_filter {
            storage.list_sessions_for_project(path).await
        } else {
            storage.list_sessions().await
        }
    }

    /// Poll the DB and event bus for new data. Returns `true` if anything changed.
    pub async fn tick(&mut self) -> Result<bool> {
        self.tick_count = self.tick_count.wrapping_add(1);
        let mut changed = false;

        // Drain event bus if in integrated mode (non-blocking)
        // Collect events first to avoid double-mutable-borrow issue.
        let events: Vec<PipelineEvent> = if let Some(ref mut rx) = self.event_rx {
            let mut collected = Vec::new();
            loop {
                match rx.try_recv() {
                    Ok(event) => collected.push(event),
                    Err(broadcast::error::TryRecvError::Empty) => break,
                    Err(_) => break, // lagged or closed
                }
            }
            collected
        } else {
            Vec::new()
        };
        for event in events {
            self.handle_pipeline_event(event).await?;
            changed = true;
        }

        // Check for pending gate
        if self.mode == TuiMode::Live {
            if let Some(ref ch) = self.gate_channel {
                if ch.has_pending() {
                    if let Some(req) = ch.take_pending() {
                        let koklo_events::GateRequest { display, responder } = req;
                        self.pending_gate_display = Some(display);
                        self.pending_gate_responder = Some(responder);
                        self.mode = TuiMode::GateOverlay;
                        changed = true;
                    }
                }
            }
        }

        // Poll DB every tick
        let new_sessions =
            Self::load_sessions(&self.storage, self.project_filter.as_deref()).await?;
        if new_sessions.len() != self.sessions.len() {
            changed = true;
        }
        self.sessions = new_sessions;

        if self.selected_session >= self.sessions.len() && !self.sessions.is_empty() {
            self.selected_session = self.sessions.len() - 1;
        }

        if self.sessions.is_empty() {
            return Ok(changed);
        }

        let sid = self.sessions[self.selected_session].id.clone();
        let new_phases = self.storage.get_phases_for_session(&sid).await?;
        if new_phases.len() != self.phases.len() {
            changed = true;
        }
        if self.preset_phase_names.is_empty() {
            self.phases = new_phases;
        } else {
            use std::collections::HashMap;
            let db_map: HashMap<String, PhaseRecord> = new_phases
                .into_iter()
                .map(|p| (p.phase.clone(), p))
                .collect();
            self.phases = self
                .preset_phase_names
                .iter()
                .map(|name| {
                    db_map.get(name).cloned().unwrap_or_else(|| PhaseRecord {
                        id: format!("pending-{}", name),
                        session_id: self.live_session_id.clone().unwrap_or_default(),
                        phase: name.clone(),
                        status: "pending".to_string(),
                        started_at: None,
                        completed_at: None,
                        error: None,
                    })
                })
                .collect();
        }

        let new_logs = self
            .storage
            .get_agent_logs_since(&sid, self.last_seq)
            .await?;
        if !new_logs.is_empty() {
            self.last_seq = new_logs.last().map(|l| l.seq).unwrap_or(self.last_seq);
            self.logs.extend(new_logs);
            changed = true;
        }

        Ok(changed)
    }

    async fn handle_pipeline_event(&mut self, event: PipelineEvent) -> Result<()> {
        match event {
            PipelineEvent::AgentLog {
                phase: _,
                session_id,
                message,
            } => {
                if self.live_session_id.is_none() {
                    self.live_session_id = Some(session_id.clone());
                }
                // Buffer incomplete lines.  SSE providers emit tiny tokens
                // (e.g. "Hello", ",", " world") without newlines.  We
                // accumulate until we see '\n', then flush complete lines as
                // log records.
                if self.line_buffer_session != session_id {
                    // New session — flush any leftover buffer.
                    if !self.line_buffer.is_empty() {
                        let flushed = std::mem::take(&mut self.line_buffer);
                        self.logs.push(AgentLogRecord {
                            id: uuid_simple(),
                            session_id: std::mem::take(&mut self.line_buffer_session),
                            phase: String::new(),
                            agent_name: "agent".to_string(),
                            message: flushed,
                            seq: self.last_seq + 1,
                            created_at: chrono::Utc::now().to_rfc3339(),
                        });
                        self.last_seq += 1;
                    }
                    self.line_buffer_session = session_id.clone();
                }

                self.line_buffer.push_str(&message);

                // Flush all complete lines from the buffer.
                while let Some(nl) = self.line_buffer.find('\n') {
                    let line: String = self.line_buffer.drain(..=nl).collect();
                    self.logs.push(AgentLogRecord {
                        id: uuid_simple(),
                        session_id: session_id.clone(),
                        phase: String::new(),
                        agent_name: "agent".to_string(),
                        message: line,
                        seq: self.last_seq + 1,
                        created_at: chrono::Utc::now().to_rfc3339(),
                    });
                    self.last_seq += 1;
                }
            }
            PipelineEvent::UsageUpdate {
                prompt_tokens,
                completion_tokens,
                cost,
                ..
            } => {
                self.running_tokens += (prompt_tokens + completion_tokens) as u64;
                match &cost {
                    Some(CostDisplay::Usd(v)) => {
                        *self.running_cost.get_or_insert(0.0) += v;
                    }
                    Some(CostDisplay::Subscription) => {
                        self.has_subscription_cost = true;
                    }
                    _ => {}
                }
            }
            PipelineEvent::SessionCompleted { session_id } => {
                // Flush any remaining buffered text.
                if !self.line_buffer.is_empty() {
                    let flushed = std::mem::take(&mut self.line_buffer);
                    self.logs.push(AgentLogRecord {
                        id: uuid_simple(),
                        session_id: std::mem::take(&mut self.line_buffer_session),
                        phase: String::new(),
                        agent_name: "agent".to_string(),
                        message: flushed,
                        seq: self.last_seq + 1,
                        created_at: chrono::Utc::now().to_rfc3339(),
                    });
                    self.last_seq += 1;
                }
                if let Ok(usage) = self.storage.get_session_usage_summary(&session_id).await {
                    self.session_usage = Some(usage);
                }
                self.mode = TuiMode::Summary;
            }
            PipelineEvent::PhaseStarted { phase, session_id } => {
                if self.live_session_id.is_none() {
                    self.live_session_id = Some(session_id.clone());
                }
                if let Some(p) = self
                    .phases
                    .iter_mut()
                    .find(|p| p.phase == phase.to_string())
                {
                    p.status = "running".to_string();
                    p.started_at = Some(chrono::Utc::now().to_rfc3339());
                    p.session_id = session_id;
                }
            }
            PipelineEvent::PhaseCompleted { phase, session_id } => {
                // Flush buffered text before marking phase complete.
                if !self.line_buffer.is_empty() {
                    let flushed = std::mem::take(&mut self.line_buffer);
                    self.logs.push(AgentLogRecord {
                        id: uuid_simple(),
                        session_id,
                        phase: String::new(),
                        agent_name: "agent".to_string(),
                        message: flushed,
                        seq: self.last_seq + 1,
                        created_at: chrono::Utc::now().to_rfc3339(),
                    });
                    self.last_seq += 1;
                }
                if let Some(p) = self
                    .phases
                    .iter_mut()
                    .find(|p| p.phase == phase.to_string())
                {
                    p.status = "completed".to_string();
                    p.completed_at = Some(chrono::Utc::now().to_rfc3339());
                }
            }
            PipelineEvent::PhaseFailed { phase, .. } => {
                if let Some(p) = self
                    .phases
                    .iter_mut()
                    .find(|p| p.phase == phase.to_string())
                {
                    p.status = "failed".to_string();
                    p.completed_at = Some(chrono::Utc::now().to_rfc3339());
                }
            }
            PipelineEvent::ToolCall {
                tool_name,
                input_summary,
                session_id,
                ..
            } => {
                if self.live_session_id.is_none() {
                    self.live_session_id = Some(session_id.clone());
                }
                self.logs.push(AgentLogRecord {
                    id: uuid_simple(),
                    session_id,
                    phase: String::new(),
                    agent_name: "tool".to_string(),
                    message: format!("⚙  {}  {}", tool_name, input_summary),
                    seq: self.last_seq + 1,
                    created_at: chrono::Utc::now().to_rfc3339(),
                });
                self.last_seq += 1;
            }
            PipelineEvent::ToolResult {
                tool_name,
                output_summary,
                session_id,
                ..
            } => {
                if self.live_session_id.is_none() {
                    self.live_session_id = Some(session_id.clone());
                }
                self.logs.push(AgentLogRecord {
                    id: uuid_simple(),
                    session_id,
                    phase: String::new(),
                    agent_name: "tool".to_string(),
                    message: format!("  ↳  {} — {}", tool_name, output_summary),
                    seq: self.last_seq + 1,
                    created_at: chrono::Utc::now().to_rfc3339(),
                });
                self.last_seq += 1;
            }
            _ => {}
        }
        Ok(())
    }

    pub fn respond_gate(&mut self, response: GateResponse) {
        let status_message = match &response {
            GateResponse::Approve => "Gate approved".to_string(),
            GateResponse::Reject => "Gate rejected".to_string(),
            GateResponse::Edit(path) => format!("Gate sent back for edit: {}", path.display()),
        };
        if let Some(responder) = self.pending_gate_responder.take() {
            let _ = responder.send(response);
        }
        self.pending_gate_display = None;
        self.mode = TuiMode::Live;
        self.status_message = Some(status_message);
    }

    pub fn render(&self, frame: &mut Frame) {
        match self.mode {
            TuiMode::Summary => self.render_summary(frame),
            TuiMode::HelpOverlay => {
                self.render_live_shell(frame);
                self.render_help_overlay(frame);
            }
            TuiMode::CommandPrompt => self.render_live_shell(frame),
            _ => {
                self.render_live_shell(frame);

                if self.mode == TuiMode::GateOverlay {
                    self.render_gate_overlay(frame);
                }
            }
        }
    }

    fn render_live_shell(&self, frame: &mut Frame) {
        let area = frame.size();

        let outer = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(1), Constraint::Length(1)])
            .split(area);

        let main_area = outer[0];
        let status_area = outer[1];

        let cols = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(28), Constraint::Percentage(72)])
            .split(main_area);

        let left_rows = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Percentage(28),
                Constraint::Length(3),
                Constraint::Length(7),
                Constraint::Min(8),
            ])
            .split(cols[0]);

        self.render_sessions(frame, left_rows[0]);
        self.render_command_prompt(frame, left_rows[1]);
        self.render_session_details(frame, left_rows[2]);
        self.render_phases(frame, left_rows[3]);
        self.render_logs(frame, cols[1]);
        self.render_statusbar(frame, status_area);
    }

    fn render_sessions(&self, frame: &mut Frame, area: Rect) {
        let border_style = if self.focus == Panel::Sessions {
            Style::default().fg(Color::Yellow)
        } else {
            Style::default()
        };

        let items: Vec<ListItem> = self
            .sessions
            .iter()
            .enumerate()
            .map(|(i, s)| {
                let icon = status_icon(&s.status);
                let short_id = short_id(&s.id);
                let short_title = truncate(&s.feature_title, 14);
                let style = if i == self.selected_session {
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                };
                ListItem::new(format!(
                    "{} {}  {}  {}",
                    icon, short_id, short_title, s.status
                ))
                .style(style)
            })
            .collect();

        let list = List::new(items).block(
            Block::default()
                .borders(Borders::ALL)
                .title("SESSIONS")
                .border_style(border_style),
        );
        frame.render_widget(list, area);
    }

    fn render_phases(&self, frame: &mut Frame, area: Rect) {
        let border_style = if self.focus == Panel::Phases {
            Style::default().fg(Color::Yellow)
        } else {
            Style::default()
        };

        let session_label = self
            .sessions
            .get(self.selected_session)
            .map(|s| short_id(&s.id))
            .unwrap_or_else(|| "—".to_string());

        let spinner_frame = SPINNER[self.tick_count % SPINNER.len()];
        let items: Vec<ListItem> = self
            .phases
            .iter()
            .enumerate()
            .map(|(i, p)| {
                let icon = if p.status == "running" {
                    spinner_frame
                } else {
                    status_icon(&p.status)
                };
                let dur = phase_dur_str(&p.started_at, &p.completed_at);
                let selected = self.selected_phase == Some(i);
                let style = if selected {
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                };
                ListItem::new(format!("{} {}{}", icon, p.phase, dur)).style(style)
            })
            .collect();

        let list = List::new(items).block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!("PHASES — {}", session_label))
                .border_style(border_style),
        );
        frame.render_widget(list, area);
    }

    fn render_session_details(&self, frame: &mut Frame, area: Rect) {
        let session = self.sessions.get(self.selected_session);

        let content = if let Some(s) = session {
            let branch = if s.workspace_branch.is_empty() {
                "main workspace"
            } else {
                s.workspace_branch.as_str()
            };
            vec![
                Line::from(vec![
                    Span::styled("Status: ", Style::default().add_modifier(Modifier::BOLD)),
                    Span::raw(&s.status),
                ]),
                Line::from(vec![
                    Span::styled("Control: ", Style::default().add_modifier(Modifier::BOLD)),
                    Span::raw(s.control_action.as_deref().unwrap_or("none")),
                ]),
                Line::from(vec![
                    Span::styled("Preset: ", Style::default().add_modifier(Modifier::BOLD)),
                    Span::raw(&s.preset),
                ]),
                Line::from(vec![
                    Span::styled("Branch: ", Style::default().add_modifier(Modifier::BOLD)),
                    Span::raw(branch),
                ]),
                Line::from(vec![
                    Span::styled("Worktree: ", Style::default().add_modifier(Modifier::BOLD)),
                    Span::raw(truncate_path(
                        &s.workspace_path,
                        area.width.saturating_sub(12) as usize,
                    )),
                ]),
                Line::from(vec![
                    Span::styled("Project: ", Style::default().add_modifier(Modifier::BOLD)),
                    Span::raw(truncate_path(
                        &s.project_path,
                        area.width.saturating_sub(11) as usize,
                    )),
                ]),
            ]
        } else {
            vec![Line::from("No session selected")]
        };

        let para = Paragraph::new(Text::from(content))
            .block(Block::default().borders(Borders::ALL).title("DETAILS"));
        frame.render_widget(para, area);
    }

    fn render_logs(&self, frame: &mut Frame, area: Rect) {
        let border_style = if self.focus == Panel::Log {
            Style::default().fg(Color::Yellow)
        } else {
            Style::default()
        };

        let session_label = self
            .sessions
            .get(self.selected_session)
            .map(|s| short_id(&s.id))
            .unwrap_or_else(|| "—".to_string());

        // Determine which phase logs to show.
        let (display_phase, is_live) = if let Some(i) = self.selected_phase {
            // Explicit phase selection from the Phases panel.
            let name = self.phases.get(i).map(|p| p.phase.as_str()).unwrap_or("—");
            let running = self
                .phases
                .get(i)
                .map(|p| p.status == "running")
                .unwrap_or(false);
            (name, running)
        } else {
            // Default: follow the live running phase (or last completed).
            let running = self.phases.iter().find(|p| p.status == "running");
            let phase = running.or_else(|| self.phases.last());
            let name = phase.map(|p| p.phase.as_str()).unwrap_or("—");
            (name, running.is_some())
        };

        let filtered_logs: Vec<&AgentLogRecord> = self
            .logs
            .iter()
            .filter(|l| l.phase == display_phase || l.phase.is_empty())
            .collect();

        let agent_name = filtered_logs
            .last()
            .map(|l| l.agent_name.as_str())
            .unwrap_or("—");

        let spinner_frame = SPINNER[self.tick_count % SPINNER.len()];
        let phase_label = if is_live {
            format!("{} {}", spinner_frame, display_phase)
        } else {
            display_phase.to_string()
        };

        let title = format!(
            "{}  ·  session {}  ·  {}",
            agent_name, session_label, phase_label
        );

        // Build styled lines: tool call/result lines get distinct dim colours.
        //
        // SSE-based providers (Ollama, OpenRouter) emit one token per log record
        // (e.g. "Hello", ",", " world").  We concatenate consecutive "agent" log
        // records into a single text blob, then split by newlines for display.
        let visible_height = area.height.saturating_sub(2) as usize;

        let mut all_styled: Vec<Line> = Vec::new();
        let mut agent_buf = String::new();

        let flush_agent_buf = |buf: &mut String, out: &mut Vec<Line>| {
            if !buf.is_empty() {
                let text = std::mem::take(buf);
                out.append(&mut crate::md_render::markdown_to_lines(&text));
            }
        };

        for log in &filtered_logs {
            if log.agent_name == "tool" {
                // Flush any buffered agent text before rendering tool events.
                flush_agent_buf(&mut agent_buf, &mut all_styled);
                for raw_line in log.message.lines() {
                    let style = if raw_line.starts_with("⚙") {
                        Style::default()
                            .fg(Color::Yellow)
                            .add_modifier(Modifier::DIM)
                    } else {
                        Style::default()
                            .fg(Color::Green)
                            .add_modifier(Modifier::DIM)
                    };
                    all_styled.push(Line::from(Span::styled(raw_line.to_string(), style)));
                }
            } else {
                // Accumulate agent text — may be a full paragraph or a single token.
                agent_buf.push_str(&log.message);
            }
        }
        flush_agent_buf(&mut agent_buf, &mut all_styled);

        // Scroll to bottom.
        let start = all_styled.len().saturating_sub(visible_height);
        let display_lines: Vec<Line> = all_styled[start..].to_vec();

        let para = Paragraph::new(Text::from(display_lines))
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(title)
                    .border_style(border_style),
            )
            .wrap(ratatui::widgets::Wrap { trim: false });
        frame.render_widget(para, area);
    }

    fn render_statusbar(&self, frame: &mut Frame, area: Rect) {
        let cost_part = if self.has_subscription_cost {
            format!("Tokens: {} | via subscription", self.running_tokens)
        } else if let Some(cost) = self.running_cost {
            format!("Tokens: {} | Cost: ${:.4}", self.running_tokens, cost)
        } else if self.running_tokens > 0 {
            format!("Tokens: {}", self.running_tokens)
        } else {
            String::new()
        };

        let nav_text = match self.mode {
            TuiMode::GateOverlay => "[Y] approve  [N] reject",
            TuiMode::Summary => "[q] quit  [Tab] live",
            TuiMode::CommandPrompt => "[Enter] run command  [Esc] close",
            TuiMode::HelpOverlay => "[Esc] close  [/] command prompt",
            TuiMode::Live => match self.focus {
                Panel::Sessions => {
                    "[q] quit  [↑↓] select session  [p] pause  [c] cancel  [Tab] next panel  [r] refresh"
                }
                Panel::Phases => {
                    "[q] quit  [↑↓] select phase  [p] pause  [c] cancel  [Esc] live view  [Tab] next panel"
                }
                Panel::Log => "[q] quit  [p] pause  [c] cancel  [Tab] next panel  [r] refresh",
            },
        };

        let full_text = if cost_part.is_empty() {
            nav_text.to_string()
        } else {
            format!("{}  |  {}", nav_text, cost_part)
        };

        let full_text = if let Some(control) = self
            .sessions
            .get(self.selected_session)
            .and_then(|session| session.control_action.as_deref())
        {
            format!("{}  |  Control queued: {}", full_text, control)
        } else {
            full_text
        };

        let full_text = if let Some(msg) = &self.status_message {
            format!("{}  |  {}", full_text, msg)
        } else {
            full_text
        };

        let para = Paragraph::new(full_text).style(Style::default().fg(Color::DarkGray));
        frame.render_widget(para, area);
    }

    fn render_command_prompt(&self, frame: &mut Frame, area: Rect) {
        let (text, border_style) = if self.mode == TuiMode::CommandPrompt {
            (
                if self.command_input.is_empty() {
                    "/".to_string()
                } else {
                    self.command_input.clone()
                },
                Style::default().fg(Color::Yellow),
            )
        } else {
            (
                "Press / to enter a command".to_string(),
                Style::default().fg(Color::DarkGray),
            )
        };
        let para = Paragraph::new(text).block(
            Block::default()
                .borders(Borders::ALL)
                .title("COMMAND")
                .border_style(border_style),
        );
        frame.render_widget(para, area);
    }

    fn render_help_overlay(&self, frame: &mut Frame) {
        let area = centered_rect(frame.size(), 72, 16);
        frame.render_widget(Clear, area);
        let content = vec![
            Line::from("/help      Show this command list"),
            Line::from("/status    Show the current session/focus summary"),
            Line::from("/live      Return to the live dashboard"),
            Line::from("/summary   Show the session usage summary"),
            Line::from("/approve   Approve the pending gate"),
            Line::from("/reject    Reject the pending gate"),
            Line::from("/pause     Pause the selected session"),
            Line::from("/cancel    Cancel the selected session"),
            Line::from("/refresh   Force a tick/refresh"),
            Line::from("/sessions  Focus the sessions panel"),
            Line::from("/phases    Focus the phases panel"),
            Line::from("/logs      Focus the logs panel"),
            Line::from("/session <id-prefix|title-fragment>"),
            Line::from("/phase <phase-name-prefix>"),
            Line::from(""),
            Line::from("[Esc] close  [/] command prompt"),
        ];
        let para = Paragraph::new(Text::from(content))
            .block(Block::default().borders(Borders::ALL).title("Commands"));
        frame.render_widget(para, area);
    }

    fn render_gate_overlay(&self, frame: &mut Frame) {
        let area = frame.size();
        let width = (area.width * 60 / 100).max(50);
        let height = 10u16;
        let x = (area.width.saturating_sub(width)) / 2;
        let y = (area.height.saturating_sub(height)) / 2;
        let overlay_area = Rect::new(x, y, width, height);

        frame.render_widget(Clear, overlay_area);

        let display = self.pending_gate_display.as_ref();
        let content = if let Some(d) = display {
            let usage_str = if let Some(u) = &d.usage {
                format!(
                    "Tokens: {} prompt + {} completion",
                    u.prompt_tokens, u.completion_tokens
                )
            } else {
                "Tokens: —".to_string()
            };
            let cost_str = if let Some(c) = &d.cost {
                match c {
                    CostDisplay::Usd(v) => format!("Cost: ${:.4}", v),
                    CostDisplay::Subscription => "Cost: via subscription".to_string(),
                    CostDisplay::Free => "Cost: free".to_string(),
                }
            } else {
                "Cost: —".to_string()
            };
            format!(
                "GATE: Phase '{}' complete\n\n  {}\n  {}\n\n  [Y] Approve   [N] Reject",
                d.phase, usage_str, cost_str
            )
        } else {
            "Gate pending…".to_string()
        };

        let para = Paragraph::new(content)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Gate Review")
                    .style(Style::default().fg(Color::Yellow)),
            )
            .style(Style::default());
        frame.render_widget(para, overlay_area);
    }

    fn render_summary(&self, frame: &mut Frame) {
        let area = frame.size();

        let header = Row::new(vec!["Phase", "Tokens", "Cost"])
            .style(Style::default().add_modifier(Modifier::BOLD))
            .bottom_margin(1);

        let mut rows: Vec<Row> = Vec::new();
        let mut total_tokens = 0u64;
        let mut total_cost: Option<f64> = None;

        if let Some(u) = &self.session_usage {
            for phase in &u.phases {
                let tokens = phase.prompt_tokens + phase.completion_tokens;
                total_tokens += tokens as u64;
                let cost_str = match phase.cost_usd {
                    Some(c) => {
                        *total_cost.get_or_insert(0.0) += c;
                        format!("${:.4}", c)
                    }
                    None => "—".to_string(),
                };
                rows.push(Row::new(vec![
                    phase.phase.clone(),
                    tokens.to_string(),
                    cost_str,
                ]));
            }
            rows.push(Row::new(vec![
                "".to_string(),
                "".to_string(),
                "".to_string(),
            ]));
            rows.push(
                Row::new(vec![
                    "TOTAL".to_string(),
                    total_tokens.to_string(),
                    total_cost
                        .map(|c| format!("${:.4}", c))
                        .unwrap_or_else(|| "—".to_string()),
                ])
                .style(Style::default().add_modifier(Modifier::BOLD)),
            );
        }

        rows.push(Row::new(vec![
            "".to_string(),
            "".to_string(),
            "[q] quit  [Tab] live".to_string(),
        ]));

        let widths = [
            Constraint::Length(18),
            Constraint::Length(12),
            Constraint::Min(20),
        ];
        let table = Table::new(rows, widths).header(header).block(
            Block::default().borders(Borders::ALL).title(format!(
                "SESSION SUMMARY — {}",
                self.live_session_id.as_deref().unwrap_or("—")
            )),
        );

        frame.render_widget(table, area);
    }

    fn select_prev(&mut self) {
        if self.selected_session > 0 {
            self.selected_session -= 1;
            self.reset_for_session();
        }
    }

    fn select_next(&mut self) {
        if self.selected_session + 1 < self.sessions.len() {
            self.selected_session += 1;
            self.reset_for_session();
        }
    }

    pub fn toggle_focus(&mut self) {
        self.focus = match self.focus {
            Panel::Sessions => Panel::Phases,
            Panel::Phases => Panel::Log,
            Panel::Log => Panel::Sessions,
        };
    }

    pub fn handle_up(&mut self) {
        match self.focus {
            Panel::Sessions => self.select_prev(),
            Panel::Phases => self.phase_prev(),
            Panel::Log => {}
        }
    }

    pub fn handle_down(&mut self) {
        match self.focus {
            Panel::Sessions => self.select_next(),
            Panel::Phases => self.phase_next(),
            Panel::Log => {}
        }
    }

    fn phase_prev(&mut self) {
        match self.selected_phase {
            Some(0) | None => {}
            Some(i) => self.selected_phase = Some(i - 1),
        }
    }

    fn phase_next(&mut self) {
        let max = self.phases.len().saturating_sub(1);
        self.selected_phase = Some(match self.selected_phase {
            None => 0,
            Some(i) if i < max => i + 1,
            Some(i) => i,
        });
    }

    fn reset_for_session(&mut self) {
        self.logs.clear();
        self.phases.clear();
        self.last_seq = 0;
        self.selected_phase = None;
        self.session_usage = None;
    }

    async fn request_selected_session_control(&mut self, action: &str) -> Result<()> {
        let Some(session) = self.sessions.get_mut(self.selected_session) else {
            return Ok(());
        };

        match session.status.as_str() {
            "completed" | "canceled" => return Ok(()),
            "running" => {
                self.storage
                    .request_session_control(&session.id, action)
                    .await?;
                session.control_action = Some(action.to_string());
                session.control_requested_at = Some(chrono::Utc::now().to_rfc3339());
            }
            _ => {
                let next_status = match action {
                    "pause" => "paused",
                    "cancel" => "canceled",
                    _ => &session.status,
                };
                self.storage.clear_session_control(&session.id).await?;
                self.storage
                    .update_session_status(&session.id, next_status)
                    .await?;
                session.control_action = None;
                session.control_requested_at = None;
                session.status = next_status.to_string();
            }
        }

        self.status_message = Some(format!(
            "Session {} {}",
            short_id(&session.id),
            match action {
                "pause" => "pause requested",
                "cancel" => "cancel requested",
                other => other,
            }
        ));

        Ok(())
    }

    fn open_command_prompt(&mut self, prefill: &str) {
        self.command_input = prefill.to_string();
        self.mode = TuiMode::CommandPrompt;
    }

    fn set_focus(&mut self, panel: Panel) {
        self.focus = panel;
        self.mode = TuiMode::Live;
    }

    fn status_snapshot(&self) -> String {
        let session = self.sessions.get(self.selected_session);
        let session_label = session
            .map(|s| format!("{} ({})", short_id(&s.id), s.status))
            .unwrap_or_else(|| "none".to_string());
        let focus = match self.focus {
            Panel::Sessions => "sessions",
            Panel::Phases => "phases",
            Panel::Log => "logs",
        };
        let phase = if let Some(index) = self.selected_phase {
            self.phases
                .get(index)
                .map(|p| p.phase.clone())
                .unwrap_or_else(|| "live".to_string())
        } else {
            "live".to_string()
        };
        format!("Session: {} | Focus: {} | Phase: {}", session_label, focus, phase)
    }

    fn select_session_by_query(&mut self, query: &str) -> Result<String, String> {
        let needle = query.to_ascii_lowercase();
        let Some((index, session_id, feature_title)) =
            self.sessions.iter().enumerate().find_map(|(index, session)| {
                let matches = session.id.to_ascii_lowercase().starts_with(&needle)
                    || session
                        .feature_title
                        .to_ascii_lowercase()
                        .contains(&needle);
                matches.then(|| (index, session.id.clone(), session.feature_title.clone()))
            })
        else {
            return Err(format!("No session matches '{}'", query));
        };

        self.selected_session = index;
        self.reset_for_session();
        Ok(format!(
            "Selected session {} ({})",
            short_id(&session_id),
            truncate(&feature_title, 24)
        ))
    }

    fn select_phase_by_query(&mut self, query: &str) -> Result<String, String> {
        let needle = query.to_ascii_lowercase();
        let Some((index, phase)) = self.phases.iter().enumerate().find(|(_, phase)| {
            phase.phase.to_ascii_lowercase().starts_with(&needle)
        }) else {
            return Err(format!("No phase matches '{}'", query));
        };

        self.selected_phase = Some(index);
        self.focus = Panel::Phases;
        self.mode = TuiMode::Live;
        Ok(format!("Selected phase {}", phase.phase))
    }

    async fn execute_command(&mut self, command: MonitorCommand) -> Result<String, String> {
        match command {
            MonitorCommand::Help => {
                self.mode = TuiMode::HelpOverlay;
                Ok("Help opened".to_string())
            }
            MonitorCommand::Status => {
                self.mode = TuiMode::Live;
                Ok(self.status_snapshot())
            }
            MonitorCommand::Live => {
                self.mode = TuiMode::Live;
                Ok("Returned to live dashboard".to_string())
            }
            MonitorCommand::Summary => {
                if self.session_usage.is_some() {
                    self.mode = TuiMode::Summary;
                    Ok("Summary opened".to_string())
                } else {
                    self.mode = TuiMode::Live;
                    Err("No session summary available yet".to_string())
                }
            }
            MonitorCommand::Approve => {
                if self.pending_gate_responder.is_some() {
                    self.respond_gate(GateResponse::Approve);
                    Ok("Gate approved".to_string())
                } else {
                    Err("No pending gate to approve".to_string())
                }
            }
            MonitorCommand::Reject => {
                if self.pending_gate_responder.is_some() {
                    self.respond_gate(GateResponse::Reject);
                    Ok("Gate rejected".to_string())
                } else {
                    Err("No pending gate to reject".to_string())
                }
            }
            MonitorCommand::Pause => {
                self.mode = TuiMode::Live;
                self.request_selected_session_control("pause")
                    .await
                    .map_err(|e| e.to_string())?;
                Ok("Pause requested".to_string())
            }
            MonitorCommand::Cancel => {
                self.mode = TuiMode::Live;
                self.request_selected_session_control("cancel")
                    .await
                    .map_err(|e| e.to_string())?;
                Ok("Cancel requested".to_string())
            }
            MonitorCommand::Refresh => {
                self.mode = TuiMode::Live;
                self.tick().await.map_err(|e| e.to_string())?;
                Ok("Refreshed".to_string())
            }
            MonitorCommand::FocusSessions => {
                self.set_focus(Panel::Sessions);
                Ok("Focused sessions".to_string())
            }
            MonitorCommand::FocusPhases => {
                self.set_focus(Panel::Phases);
                Ok("Focused phases".to_string())
            }
            MonitorCommand::FocusLogs => {
                self.set_focus(Panel::Log);
                Ok("Focused logs".to_string())
            }
            MonitorCommand::Session(query) => self.select_session_by_query(&query),
            MonitorCommand::Phase(query) => self.select_phase_by_query(&query),
        }
    }

    async fn submit_command(&mut self) {
        let raw = std::mem::take(&mut self.command_input);
        match parse_monitor_command(&raw) {
            Ok(command) => match self.execute_command(command).await {
                Ok(message) => self.status_message = Some(message),
                Err(err) => {
                    self.mode = TuiMode::Live;
                    self.status_message = Some(err);
                }
            },
            Err(err) => {
                self.mode = TuiMode::Live;
                self.status_message = Some(err);
            }
        }
    }
}

// ── Public entry points ────────────────────────────────────────────────────────

/// Run `koklo monitor`. Dispatches to TUI or follow mode.
pub async fn run_monitor(
    session_filter: Option<String>,
    follow: bool,
    project_filter: Option<String>,
    storage: Arc<SessionManager>,
) -> Result<()> {
    if follow {
        run_follow_mode(session_filter, project_filter, storage).await
    } else {
        run_tui_mode(session_filter, project_filter, storage).await
    }
}

/// Run the integrated TUI for `koklo run` with a live event bus and gate channel.
pub async fn run_integrated_tui(
    storage: Arc<SessionManager>,
    event_rx: Option<broadcast::Receiver<PipelineEvent>>,
    gate_channel: Option<GateChannel>,
    preset_phases: Vec<String>,
) -> Result<()> {
    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut app =
        MonitorApp::new_integrated(storage, event_rx, gate_channel, None, None, preset_phases)
            .await?;
    let result = tui_integrated_event_loop(&mut terminal, &mut app).await;

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    result
}

// ── Follow mode ────────────────────────────────────────────────────────────────

async fn run_follow_mode(
    session_filter: Option<String>,
    project_filter: Option<String>,
    storage: Arc<SessionManager>,
) -> Result<()> {
    let sessions = MonitorApp::load_sessions(&storage, project_filter.as_deref()).await?;
    let session = if let Some(ref filter) = session_filter {
        sessions
            .iter()
            .find(|s| s.id.starts_with(filter.as_str()))
            .ok_or_else(|| anyhow::anyhow!("Session not found: {}", filter))?
            .clone()
    } else {
        sessions
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("No sessions found"))?
    };

    println!(
        "Following session: {} ({})",
        session.id, session.feature_title
    );

    let mut last_seq: i64 = 0;
    let tick = Duration::from_millis(500);

    loop {
        tokio::time::sleep(tick).await;

        let logs = storage.get_agent_logs_since(&session.id, last_seq).await?;
        for log in &logs {
            let time = log.created_at.get(11..19).unwrap_or("??:??:??");
            println!("[{}] [{}] {}", time, log.agent_name, log.message);
        }
        if let Some(last) = logs.last() {
            last_seq = last.seq;
        }

        if let Some(s) = storage.get_session(&session.id).await? {
            if s.status == "completed" || s.status == "failed" {
                println!("\nSession {} — {}", short_id(&s.id), s.status);
                break;
            }
        }
    }

    Ok(())
}

// ── TUI mode ───────────────────────────────────────────────────────────────────

async fn run_tui_mode(
    session_filter: Option<String>,
    project_filter: Option<String>,
    storage: Arc<SessionManager>,
) -> Result<()> {
    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut app = MonitorApp::new(session_filter.as_deref(), project_filter, storage).await?;
    let result = tui_event_loop(&mut terminal, &mut app).await;

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    result
}

async fn tui_event_loop(
    terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
    app: &mut MonitorApp,
) -> Result<()> {
    let tick_rate = Duration::from_millis(500);
    let mut last_tick = Instant::now();

    loop {
        terminal.draw(|f| app.render(f))?;

        let timeout = tick_rate
            .checked_sub(last_tick.elapsed())
            .unwrap_or(Duration::ZERO);

        if event::poll(timeout)? {
            if let Event::Key(key) = event::read()? {
                match key.code {
                    KeyCode::Char('q') | KeyCode::Char('Q') => break,
                    KeyCode::Up => app.handle_up(),
                    KeyCode::Down => app.handle_down(),
                    KeyCode::Tab => app.toggle_focus(),
                    // Esc in Phases panel clears phase selection (back to live view).
                    KeyCode::Esc => app.selected_phase = None,
                    KeyCode::Char('p') | KeyCode::Char('P') => {
                        app.request_selected_session_control("pause").await?;
                    }
                    KeyCode::Char('c') | KeyCode::Char('C') => {
                        app.request_selected_session_control("cancel").await?;
                    }
                    KeyCode::Char('r') => {
                        app.tick().await?;
                    }
                    _ => {}
                }
            }
        }

        if last_tick.elapsed() >= tick_rate {
            app.tick().await?;
            last_tick = Instant::now();
        }
    }

    Ok(())
}

async fn tui_integrated_event_loop(
    terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
    app: &mut MonitorApp,
) -> Result<()> {
    let tick_rate = Duration::from_millis(50); // faster for event bus draining
    let mut last_tick = Instant::now();

    loop {
        terminal.draw(|f| app.render(f))?;

        let timeout = tick_rate
            .checked_sub(last_tick.elapsed())
            .unwrap_or(Duration::ZERO);

        if event::poll(timeout)? {
            if let Event::Key(key) = event::read()? {
                match app.mode.clone() {
                    TuiMode::GateOverlay => match key.code {
                        KeyCode::Char('y') | KeyCode::Char('Y') => {
                            app.respond_gate(GateResponse::Approve);
                        }
                        KeyCode::Char('n') | KeyCode::Char('N') => {
                            app.respond_gate(GateResponse::Reject);
                        }
                        _ => {}
                    },
                    TuiMode::Summary => match key.code {
                        KeyCode::Char('q') | KeyCode::Char('Q') => break,
                        KeyCode::Tab => app.mode = TuiMode::Live,
                        KeyCode::Char('/') | KeyCode::Char(':') => app.open_command_prompt("/"),
                        KeyCode::Esc => app.mode = TuiMode::Live,
                        _ => {}
                    },
                    TuiMode::HelpOverlay => match key.code {
                        KeyCode::Esc => app.mode = TuiMode::Live,
                        KeyCode::Char('/') | KeyCode::Char(':') => app.open_command_prompt("/"),
                        _ => {}
                    },
                    TuiMode::CommandPrompt => match key.code {
                        KeyCode::Esc => {
                            app.command_input.clear();
                            app.mode = TuiMode::Live;
                        }
                        KeyCode::Backspace => {
                            app.command_input.pop();
                        }
                        KeyCode::Enter => app.submit_command().await,
                        KeyCode::Char(ch) => app.command_input.push(ch),
                        _ => {}
                    },
                    TuiMode::Live => match key.code {
                        KeyCode::Char('q') | KeyCode::Char('Q') => break,
                        KeyCode::Up => app.handle_up(),
                        KeyCode::Down => app.handle_down(),
                        KeyCode::Tab => app.toggle_focus(),
                        KeyCode::Esc => app.selected_phase = None,
                        KeyCode::Char('p') | KeyCode::Char('P') => {
                            app.request_selected_session_control("pause").await?;
                        }
                        KeyCode::Char('c') | KeyCode::Char('C') => {
                            app.request_selected_session_control("cancel").await?;
                        }
                        KeyCode::Char('r') => {
                            app.tick().await?;
                        }
                        KeyCode::Char('/') | KeyCode::Char(':') => app.open_command_prompt("/"),
                        _ => {}
                    },
                }
            }
        }

        if last_tick.elapsed() >= tick_rate {
            app.tick().await?;
            last_tick = Instant::now();
        }
    }

    Ok(())
}

// ── Helpers ────────────────────────────────────────────────────────────────────

fn status_icon(status: &str) -> &'static str {
    match status {
        "running" => "●",
        "completed" => "✓",
        "failed" => "✗",
        "paused" => "⏸",
        "canceled" => "■",
        "pending" => "·",
        _ => "○",
    }
}

fn short_id(id: &str) -> String {
    id[..6.min(id.len())].to_string()
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max.saturating_sub(1)])
    }
}

fn truncate_path(path: &str, max: usize) -> String {
    if path.chars().count() <= max {
        return path.to_string();
    }
    if max <= 3 {
        return "...".to_string();
    }

    let keep = (max.saturating_sub(3)) / 2;
    let start: String = path.chars().take(keep).collect();
    let end: String = path
        .chars()
        .rev()
        .take(keep)
        .collect::<String>()
        .chars()
        .rev()
        .collect();
    format!("{}...{}", start, end)
}

fn phase_dur_str(started_at: &Option<String>, completed_at: &Option<String>) -> String {
    if let (Some(start), Some(end)) = (started_at, completed_at) {
        if let (Ok(s), Ok(e)) = (
            chrono::DateTime::parse_from_rfc3339(start),
            chrono::DateTime::parse_from_rfc3339(end),
        ) {
            let secs = (e - s).num_seconds();
            return format!("  {}s", secs);
        }
    }
    String::new()
}

fn uuid_simple() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    format!("{}-{}", t.as_millis(), t.subsec_nanos())
}

fn centered_rect(area: Rect, width_pct: u16, height: u16) -> Rect {
    let width = (area.width * width_pct / 100).max(32);
    let x = (area.width.saturating_sub(width)) / 2;
    let y = (area.height.saturating_sub(height)) / 2;
    Rect::new(x, y, width, height)
}

#[cfg(test)]
mod tests {
    use super::{parse_monitor_command, MonitorCommand};

    #[test]
    fn parse_simple_command() {
        assert_eq!(parse_monitor_command("/status"), Ok(MonitorCommand::Status));
    }

    #[test]
    fn parse_command_with_argument() {
        assert_eq!(
            parse_monitor_command("/session auth"),
            Ok(MonitorCommand::Session("auth".to_string()))
        );
    }

    #[test]
    fn reject_unknown_command() {
        assert!(parse_monitor_command("/nope").is_err());
    }
}
