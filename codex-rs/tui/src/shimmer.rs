use std::sync::OnceLock;
use std::time::Duration;
use std::time::Instant;

use ratatui::style::Color;
use ratatui::style::Modifier;
use ratatui::style::Style;
use ratatui::text::Span;

use crate::color::blend;
use crate::terminal_palette::default_bg;
use crate::terminal_palette::default_fg;

static PROCESS_START: OnceLock<Instant> = OnceLock::new();
const SHIMMER_PADDING: usize = 10;
const SHIMMER_BAND_HALF_WIDTH: usize = 5;
const SHIMMER_SWEEP: Duration = Duration::from_secs(2);

fn elapsed_since_start() -> Duration {
    let start = PROCESS_START.get_or_init(Instant::now);
    start.elapsed()
}

pub(super) fn next_shimmer_change_in(text: &str) -> Option<Duration> {
    next_shimmer_change_after(elapsed_since_start(), text.chars().count())
}

pub(super) fn next_shimmer_change_after(elapsed: Duration, char_count: usize) -> Option<Duration> {
    if char_count == 0 {
        return None;
    }

    let period = char_count as u128 + (SHIMMER_PADDING * 2) as u128;
    let sweep_nanos = SHIMMER_SWEEP.as_nanos();
    let phase_nanos = elapsed.as_nanos() % sweep_nanos;
    let bucket = phase_nanos * period / sweep_nanos;
    let first_visible_bucket = (SHIMMER_PADDING - SHIMMER_BAND_HALF_WIDTH + 1) as u128;
    let last_visible_bucket =
        char_count as u128 + (SHIMMER_PADDING + SHIMMER_BAND_HALF_WIDTH - 1) as u128;
    let next_bucket = if bucket < first_visible_bucket {
        first_visible_bucket
    } else if bucket < last_visible_bucket {
        bucket + 1
    } else {
        period + first_visible_bucket
    };
    let next_boundary_nanos = (next_bucket * sweep_nanos).div_ceil(period);
    let remaining_nanos = next_boundary_nanos - phase_nanos;
    // The next visible boundary is at most one two-second sweep away.
    Some(Duration::from_nanos(remaining_nanos as u64))
}

pub(super) fn shimmer_spans(text: &str) -> Vec<Span<'static>> {
    let chars: Vec<char> = text.chars().collect();
    if chars.is_empty() {
        return Vec::new();
    }
    // Use time-based sweep synchronized to process start.
    let period = chars.len() as u128 + (SHIMMER_PADDING * 2) as u128;
    let phase_nanos = elapsed_since_start().as_nanos() % SHIMMER_SWEEP.as_nanos();
    let pos = phase_nanos * period / SHIMMER_SWEEP.as_nanos();
    let has_true_color = supports_color::on_cached(supports_color::Stream::Stdout)
        .map(|level| level.has_16m)
        .unwrap_or(false);
    let band_half_width = SHIMMER_BAND_HALF_WIDTH as f32;

    let mut spans: Vec<Span<'static>> = Vec::with_capacity(chars.len());
    let base_color = default_fg().unwrap_or((128, 128, 128));
    let highlight_color = default_bg().unwrap_or((255, 255, 255));
    for (i, ch) in chars.iter().enumerate() {
        let i_pos = i as u128 + SHIMMER_PADDING as u128;
        let dist = i_pos.abs_diff(pos) as f32;

        let t = if dist <= band_half_width {
            let x = std::f32::consts::PI * (dist / band_half_width);
            0.5 * (1.0 + x.cos())
        } else {
            0.0
        };
        let style = if has_true_color {
            let highlight = t.clamp(0.0, 1.0);
            let (r, g, b) = blend(highlight_color, base_color, highlight * 0.9);
            // Allow custom RGB colors, as the implementation is thoughtfully
            // adjusting the level of the default foreground color.
            #[allow(clippy::disallowed_methods)]
            {
                Style::default()
                    .fg(Color::Rgb(r, g, b))
                    .add_modifier(Modifier::BOLD)
            }
        } else {
            color_for_level(t)
        };
        spans.push(Span::styled(ch.to_string(), style));
    }
    spans
}

fn color_for_level(intensity: f32) -> Style {
    // Tune fallback styling so the shimmer band reads even without RGB support.
    if intensity < 0.2 {
        Style::default().add_modifier(Modifier::DIM)
    } else if intensity < 0.6 {
        Style::default()
    } else {
        Style::default().add_modifier(Modifier::BOLD)
    }
}
