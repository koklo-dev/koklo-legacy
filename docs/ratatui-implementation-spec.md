# Koklo RataTUI Implementation Spec

## Goal

Turn `koklo monitor` and the integrated `koklo run` TUI into an operator console, not just a live log viewer.

The target interaction model is:

1. Observe live pipeline state.
2. Operate the session from inside the TUI.
3. Review work before and after gates.
4. Resume or fork work without leaving the current context.

## Ranked Delivery Plan

### 1. Command Layer

Status: implemented in the first slice.

Add a slash-command layer so the TUI is directly operable.

Commands:

- `/help`
- `/status`
- `/live`
- `/summary`
- `/approve`
- `/reject`
- `/pause`
- `/cancel`
- `/refresh`
- `/sessions`
- `/phases`
- `/logs`
- `/session <id-prefix|title-fragment>`
- `/phase <phase-name-prefix>`

Acceptance criteria:

- Pressing `/` or `:` opens a command prompt.
- Commands execute without leaving the TUI.
- Command results are shown in the status bar.
- The help overlay is available from `/help`.

### 2. Review and Diff Surface

Status: next.

Add an explicit review mode for the current session/worktree.

Commands:

- `/diff`
- `/review`
- `/artifacts`
- `/open <artifact>`

UI:

- Diff list panel.
- File-level diff viewer.
- Phase artifact picker.
- Gate overlay can link to diff/review mode.

Acceptance criteria:

- User can inspect what changed before approving a gate.
- Review mode works for both integrated runs and historical sessions.

### 3. Resume and Fork

Status: blocked on wiring an orchestrator-capable control handle into the TUI.

Commands:

- `/resume`
- `/fork`
- `/new`

Requirements:

- `MonitorApp` must receive more than `SessionManager`; it needs a resumable action layer.
- Historical sessions need a session action abstraction that can call orchestrator resume flows.

Acceptance criteria:

- Paused sessions can be resumed from inside the TUI.
- A new exploratory branch/worktree can be created from a session context.

### 4. Worktree-Native Sessions

Status: next after review mode.

Commands:

- `/worktree`
- `/workspace`
- `/handoff`

UI:

- Session details panel shows worktree mode clearly.
- Review mode labels local tree vs isolated worktree.
- Handoff flow from integrated run to desktop app.

### 5. Background Jobs and Notifications

Status: later.

Commands:

- `/jobs`
- `/notifications`
- `/terminal`

UI:

- Background job list.
- Notification tray/toast area.
- Per-command execution history.

## State Model

The TUI state should be separated into four concerns.

### 1. Data State

Backed by storage and event bus.

- `sessions`
- `phases`
- `logs`
- `session_usage`
- `running_tokens`
- `running_cost`

### 2. View State

Controls what is visible.

- `focus`
- `mode`
- `selected_session`
- `selected_phase`

Current modes:

- `Live`
- `GateOverlay`
- `Summary`
- `CommandPrompt`
- `HelpOverlay`

### 3. Command State

Controls command entry and feedback.

- `command_input`
- `status_message`

Future additions:

- `command_history`
- `last_successful_command`
- `notifications`

### 4. Action State

Tracks what operations the TUI can trigger.

Current:

- gate approval/rejection
- session pause/cancel
- refresh
- selection/focus changes

Future:

- resume
- fork
- diff/review actions
- artifact open/select

## Screen Model

### Live Dashboard

Existing primary screen.

Panels:

- sessions
- details
- phases
- logs
- status bar

### Command Prompt

Overlay on top of live dashboard.

Purpose:

- fast in-context operation
- discoverable command entry

### Help Overlay

Overlay listing supported commands and shortcuts.

### Gate Overlay

Existing modal.

Should evolve to include:

- phase summary
- changed files count
- direct jump to review mode

### Summary Screen

Existing usage summary.

Should evolve to include:

- per-phase outcome summary
- links to artifacts
- link back into review mode

### Planned Review Screen

Not implemented yet.

Panels:

- changed files
- diff body
- artifact references
- action bar

## Rust Module Split

Recommended target split for `apps/cli/src/monitor/`:

- `mod.rs`
  Entry points, terminal bootstrap, public API.
- `app.rs`
  `MonitorApp`, high-level state container.
- `commands.rs`
  slash-command parsing, command execution dispatch.
- `events.rs`
  event-bus handling and storage refresh logic.
- `render.rs`
  top-level render orchestration.
- `screens/live.rs`
  sessions/details/phases/logs layout.
- `screens/overlay.rs`
  gate, help, command prompt overlays.
- `screens/summary.rs`
  summary table rendering.
- `state.rs`
  enums and view-state helpers.
- `selectors.rs`
  session and phase lookup helpers.

Recommended split order:

1. extract `commands.rs`
2. extract `state.rs`
3. extract `render.rs`
4. extract `events.rs`

This order keeps behavior stable while reducing the current size of `monitor.rs`.

## First Implemented Slice

Implemented in `apps/cli/src/monitor.rs`:

- command prompt opened by `/` or `:`
- command parser
- command execution for navigation and session control
- help overlay
- status-bar command feedback
- parser unit tests

## Explicit Non-Goals For This Slice

- diff viewer
- resume orchestration from monitor
- worktree handoff UI
- desktop parity

Those remain the next-ranked slices because they require new action plumbing, not just TUI rendering changes.
