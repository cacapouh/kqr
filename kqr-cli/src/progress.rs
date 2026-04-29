//! Progress reporter abstraction.
//!
//! TTY: spins one line on stderr (overwritten in place via indicatif).
//! Non-TTY: emits a `[progress] ...` line every `--progress-interval`.
//! Disabled: no output.

use std::time::{Duration, Instant};

use indicatif::{ProgressBar, ProgressStyle};

pub trait Reporter: Send {
    /// Called after each consumed message; reporters may rate-limit internally.
    fn record(&mut self, messages: u64, bytes: u64);
    /// Called once at end.
    fn finish(&mut self, total_messages: u64, total_bytes: u64, elapsed: Duration);
}

pub struct NoopReporter;
impl Reporter for NoopReporter {
    fn record(&mut self, _: u64, _: u64) {}
    fn finish(&mut self, _: u64, _: u64, _: Duration) {}
}

pub struct SpinnerReporter {
    bar: ProgressBar,
    started: Instant,
}

impl SpinnerReporter {
    pub fn new(label: &str) -> Self {
        let bar = ProgressBar::new_spinner();
        bar.set_style(
            ProgressStyle::with_template("{spinner} {prefix} {msg}")
                .unwrap()
                .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]),
        );
        bar.set_prefix(format!("[{label}]"));
        bar.enable_steady_tick(Duration::from_millis(100));
        Self {
            bar,
            started: Instant::now(),
        }
    }
}

impl Reporter for SpinnerReporter {
    fn record(&mut self, messages: u64, bytes: u64) {
        self.bar.set_message(format!(
            "{} msgs · {} ({:.1?})",
            messages,
            human_bytes(bytes),
            self.started.elapsed()
        ));
    }
    fn finish(&mut self, total_messages: u64, total_bytes: u64, elapsed: Duration) {
        self.bar.finish_with_message(format!(
            "{} msgs · {} in {:.1?}",
            total_messages,
            human_bytes(total_bytes),
            elapsed
        ));
    }
}

pub struct PeriodicReporter {
    interval: Duration,
    last_emit: Instant,
    started: Instant,
    label: String,
}

impl PeriodicReporter {
    pub fn new(label: &str, interval: Duration) -> Self {
        Self {
            interval,
            last_emit: Instant::now(),
            started: Instant::now(),
            label: label.to_string(),
        }
    }
}

impl Reporter for PeriodicReporter {
    fn record(&mut self, messages: u64, bytes: u64) {
        if self.last_emit.elapsed() >= self.interval {
            eprintln!(
                "[{}] {} msgs · {} ({:.1?})",
                self.label,
                messages,
                human_bytes(bytes),
                self.started.elapsed()
            );
            self.last_emit = Instant::now();
        }
    }
    fn finish(&mut self, total_messages: u64, total_bytes: u64, elapsed: Duration) {
        eprintln!(
            "[{}] done · {} msgs · {} in {:.1?}",
            self.label,
            total_messages,
            human_bytes(total_bytes),
            elapsed
        );
    }
}

/// Build a reporter according to flags + TTY detection.
pub fn build(label: &str, enabled: bool, interval: Duration) -> Box<dyn Reporter> {
    if !enabled {
        return Box::new(NoopReporter);
    }
    if is_stderr_tty() {
        Box::new(SpinnerReporter::new(label))
    } else {
        Box::new(PeriodicReporter::new(label, interval))
    }
}

fn is_stderr_tty() -> bool {
    use std::io::IsTerminal;
    std::io::stderr().is_terminal()
}

fn human_bytes(n: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut v = n as f64;
    let mut idx = 0;
    while v >= 1024.0 && idx + 1 < UNITS.len() {
        v /= 1024.0;
        idx += 1;
    }
    if idx == 0 {
        format!("{} {}", n, UNITS[0])
    } else {
        format!("{:.1} {}", v, UNITS[idx])
    }
}
