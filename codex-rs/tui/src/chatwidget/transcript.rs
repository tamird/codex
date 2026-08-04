//! Transcript and active-cell bookkeeping for `ChatWidget`.

use super::HistoryCell;
use super::HistoryRenderMode;
use crate::history_cell::StreamingAgentTailCell;
use crate::history_cell::StreamingPlanTailCell;
use crate::terminal_hyperlinks::HyperlinkLine;
use crate::terminal_hyperlinks::HyperlinkParagraph;
use ratatui::style::Style;
use std::cell::Cell;
use std::cell::Ref;
use std::cell::RefCell;

/// Identifies the render state that determines an active cell's viewport height.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ActiveCellLayoutCacheKey {
    pub(super) cell_identity: usize,
    pub(super) revision: u64,
    pub(super) width: u16,
    pub(super) render_mode: HistoryRenderMode,
    pub(super) syntax_theme_revision: u64,
}

/// Retains the active cell's semantic and actual wrapped heights independently.
///
/// History cells may override their desired height, so it cannot be substituted for the rendered
/// row count used to keep overflowing content anchored to the bottom of the viewport.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ActiveCellLayoutCache {
    pub(super) key: ActiveCellLayoutCacheKey,
    pub(super) desired_height: Option<u16>,
    pub(super) rendered_height: Option<usize>,
}

#[derive(Debug, Eq, PartialEq)]
struct ActiveCellRenderKey {
    width: u16,
    revision: u64,
    animation_tick: Option<u64>,
    render_mode: HistoryRenderMode,
    syntax_theme_revision: u64,
}

#[derive(Default)]
pub(super) struct ActiveCellRender {
    key: Option<ActiveCellRenderKey>,
    pub(super) desired_height: u16,
    pub(super) paragraph_height: usize,
    pub(super) lines: Vec<HyperlinkLine>,
}

#[derive(Default)]
pub(super) struct TranscriptState {
    pub(super) active_cell: Option<Box<dyn HistoryCell>>,
    /// Monotonic-ish counter used to invalidate transcript overlay caching.
    pub(super) active_cell_revision: u64,
    /// One bounded entry shared by layout and paint across unchanged active-cell frames.
    pub(super) active_cell_layout: Cell<Option<ActiveCellLayoutCache>>,
    /// Reuses one exact active-cell layout across consecutive unchanged viewport frames.
    active_cell_render: RefCell<ActiveCellRender>,
    /// Markdown of the most recently completed agent response for whole-response copying.
    pub(super) last_agent_markdown: Option<String>,
    /// Original source of that response, before display sanitization, for exact block copying.
    pub(super) last_agent_source: Option<String>,
    pub(super) last_completed_agent_message: Option<(String, String)>,
    /// Raw markdown of the most recently completed proposed plan.
    pub(super) latest_proposed_plan_markdown: Option<String>,
    /// Whether this turn already produced a copyable response.
    pub(super) saw_copy_source_this_turn: bool,
    /// Whether the next streamed assistant content should be preceded by a final message separator.
    pub(super) needs_final_message_separator: bool,
    /// Whether the current turn performed "work" (exec commands, MCP tool calls, patch applications).
    pub(super) had_work_activity: bool,
    /// Whether the current turn emitted a plan update.
    pub(super) saw_plan_update_this_turn: bool,
    /// Whether the current turn emitted a proposed plan item that has not been superseded by a
    /// later steer.
    pub(super) saw_plan_item_this_turn: bool,
    /// Latest `update_plan` checklist task counts for terminal-title rendering.
    pub(super) last_plan_progress: Option<(usize, usize)>,
    /// Incremental buffer for streamed plan content.
    pub(super) plan_delta_buffer: String,
    /// True while a plan item is streaming.
    pub(super) plan_item_active: bool,
}

impl TranscriptState {
    pub(super) fn new(active_cell: Option<Box<dyn HistoryCell>>) -> Self {
        Self {
            active_cell,
            ..Self::default()
        }
    }

    pub(super) fn bump_active_cell_revision(&mut self) {
        // Wrapping avoids overflow; wraparound would require 2^64 bumps and at
        // worst causes a one-time cache-key collision.
        self.active_cell_revision = self.active_cell_revision.wrapping_add(1);
        self.active_cell_layout.set(None);
    }

    /// Remove the active cell and invalidate its layout before its address can be reused.
    pub(super) fn take_active_cell(&mut self) -> Option<Box<dyn HistoryCell>> {
        let active_cell = self.active_cell.take();
        if active_cell.is_some() {
            self.active_cell_layout.set(None);
            *self.active_cell_render.get_mut() = ActiveCellRender::default();
        }
        active_cell
    }

    pub(super) fn render_active_cell(
        &self,
        cell: &dyn HistoryCell,
        width: u16,
        render_mode: HistoryRenderMode,
    ) -> Ref<'_, ActiveCellRender> {
        let key = ActiveCellRenderKey {
            width,
            revision: self.active_cell_revision,
            animation_tick: cell.transcript_animation_tick(),
            render_mode,
            syntax_theme_revision: crate::render::highlight::syntax_theme_revision(),
        };
        {
            let mut cached = self.active_cell_render.borrow_mut();
            if cached.key.as_ref() != Some(&key) {
                let lines = cell.display_hyperlink_lines(width);
                let paragraph_height =
                    HyperlinkParagraph::new(&lines, Style::default()).line_count(width);
                let desired_height = if cell.as_any().is::<StreamingAgentTailCell>()
                    || cell.as_any().is::<StreamingPlanTailCell>()
                {
                    paragraph_height.try_into().unwrap_or(0)
                } else {
                    HistoryCell::desired_height(cell, width)
                };
                *cached = ActiveCellRender {
                    key: Some(key),
                    desired_height,
                    paragraph_height,
                    lines,
                };
            }
        }
        self.active_cell_render.borrow()
    }

    pub(super) fn record_agent_markdown(&mut self, markdown: String, source: String) {
        self.last_agent_markdown = Some(markdown);
        self.last_agent_source = Some(source);
        self.saw_copy_source_this_turn = true;
    }

    pub(super) fn reset_copy_history(&mut self) {
        self.last_agent_markdown = None;
        self.last_agent_source = None;
        self.saw_copy_source_this_turn = false;
    }

    pub(super) fn reset_turn_flags(&mut self) {
        self.saw_copy_source_this_turn = false;
        self.last_completed_agent_message = None;
        self.saw_plan_update_this_turn = false;
        self.saw_plan_item_this_turn = false;
        self.had_work_activity = false;
        self.latest_proposed_plan_markdown = None;
        self.plan_delta_buffer.clear();
        self.plan_item_active = false;
    }
}

#[cfg(test)]
mod tests {
    use crate::chatwidget::tests::make_chatwidget_manual_with_sender;
    use crate::render::renderable::Renderable;
    use pretty_assertions::assert_eq;
    use ratatui::buffer::Buffer;
    use ratatui::layout::Rect;
    use ratatui::text::Line;

    use super::*;

    #[test]
    fn active_cell_revision_wraps() {
        let mut state = TranscriptState {
            active_cell_revision: u64::MAX,
            ..TranscriptState::default()
        };

        state.bump_active_cell_revision();

        assert_eq!(state.active_cell_revision, 0);
    }

    #[tokio::test]
    async fn streaming_tails_reuse_rendered_lines_without_animation_ticks() {
        let lines = vec![HyperlinkLine::new(Line::from("visible stream tail"))];
        let cells: [Box<dyn HistoryCell>; 2] = [
            Box::new(StreamingAgentTailCell::new(
                lines.clone(),
                /*is_first_line*/ true,
            )),
            Box::new(StreamingPlanTailCell::new(
                lines, /*is_stream_continuation*/ false,
            )),
        ];
        for cell in cells {
            assert_eq!(cell.transcript_animation_tick(), None);
            let (mut widget, _sender, _events, _operations) =
                make_chatwidget_manual_with_sender().await;
            widget.transcript.active_cell = Some(cell);
            widget.transcript.bump_active_cell_revision();
            let area = Rect::new(
                /*x*/ 0, /*y*/ 0, /*width*/ 40, /*height*/ 10,
            );
            let mut first = Buffer::empty(area);
            widget.as_renderable().render(area, &mut first);
            let first_lines = {
                let cached = widget.transcript.active_cell_render.borrow();
                assert!(
                    cached.key.is_some(),
                    "the real streaming tail must use the cache"
                );
                assert!(!cached.lines.is_empty());
                cached.lines.as_ptr()
            };

            let mut second = Buffer::empty(area);
            widget.as_renderable().render(area, &mut second);
            assert_eq!(first, second);
            assert_eq!(
                widget.transcript.active_cell_render.borrow().lines.as_ptr(),
                first_lines,
                "unchanged streaming tails must reuse their rendered lines",
            );
        }
    }
}
