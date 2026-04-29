//! Time-window specifications for bounded Kafka reads.
//!
//! Maps the user-facing `--last / --since / --from / --to / --offset` flags
//! into a small enum that [`super::KafkaSource::consume`] can resolve into
//! per-partition offsets.

use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::error::{Error, Result};

/// Bounded read window.
#[derive(Debug, Clone)]
pub enum TimeWindow {
    /// `[now - duration, now]`. Resolved at consume time so the upper bound
    /// is the high watermark observed when the consumer connects.
    Last(Duration),
    /// Absolute time range. `to: None` means "no upper bound" — read until
    /// each partition's high watermark.
    Range {
        from: DateTime<Utc>,
        to: Option<DateTime<Utc>>,
    },
    /// Read from earliest or latest, capped at `limit` messages total.
    Offset { start: OffsetStart, limit: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OffsetStart {
    Earliest,
    Latest,
}

impl TimeWindow {
    /// Default applied when no window flag is given (`--last 10m`).
    pub const DEFAULT_LAST: Duration = Duration::from_secs(600);

    pub fn default_last() -> Self {
        TimeWindow::Last(Self::DEFAULT_LAST)
    }
}

/// Mirror of the CLI flag set, decoupled from `clap` so this lives in core.
///
/// Each `&str` is the raw user input; resolution happens in
/// [`WindowParse::resolve`].
#[derive(Debug, Default, Clone, Copy)]
pub struct WindowParse<'a> {
    pub last: Option<&'a str>,
    pub since: Option<&'a str>,
    pub from: Option<&'a str>,
    pub to: Option<&'a str>,
    pub offset: Option<OffsetStart>,
    pub limit: Option<u64>,
}

impl<'a> WindowParse<'a> {
    pub fn resolve(self) -> Result<TimeWindow> {
        let lower_count = [
            self.last.is_some(),
            self.since.is_some(),
            self.from.is_some(),
            self.offset.is_some(),
        ]
        .iter()
        .filter(|x| **x)
        .count();
        if lower_count > 1 {
            return Err(Error::WindowConflict);
        }

        // --last <duration>
        if let Some(s) = self.last {
            if self.to.is_some() {
                return Err(Error::WindowConflict);
            }
            let d = humantime::parse_duration(s)
                .map_err(|e| Error::WindowDuration(s.to_string(), e))?;
            return Ok(TimeWindow::Last(d));
        }

        // --since <duration|rfc3339>
        if let Some(s) = self.since {
            let from = if let Ok(d) = humantime::parse_duration(s) {
                Utc::now()
                    - chrono::Duration::from_std(d).unwrap_or_else(|_| chrono::Duration::seconds(0))
            } else {
                parse_rfc3339(s)?
            };
            let to = self.to.map(parse_rfc3339).transpose()?;
            check_range(&from, to.as_ref())?;
            return Ok(TimeWindow::Range { from, to });
        }

        // --from <rfc3339> [--to <rfc3339>]
        if let Some(s) = self.from {
            let from = parse_rfc3339(s)?;
            let to = self.to.map(parse_rfc3339).transpose()?;
            check_range(&from, to.as_ref())?;
            return Ok(TimeWindow::Range { from, to });
        }

        // --offset earliest|latest --limit N
        if let Some(off) = self.offset {
            let limit = self.limit.ok_or(Error::WindowOffsetWithoutLimit)?;
            return Ok(TimeWindow::Offset { start: off, limit });
        }

        // --to alone
        if self.to.is_some() {
            return Err(Error::WindowToWithoutFrom);
        }

        // Nothing set → default --last 10m.
        Ok(TimeWindow::default_last())
    }
}

fn parse_rfc3339(s: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .map(|d| d.with_timezone(&Utc))
        .map_err(|e| Error::WindowTimestamp(s.to_string(), e))
}

fn check_range(from: &DateTime<Utc>, to: Option<&DateTime<Utc>>) -> Result<()> {
    if let Some(t) = to {
        if t <= from {
            return Err(Error::WindowToBeforeFrom);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn last_default_when_no_flags() {
        let w = WindowParse::default().resolve().unwrap();
        match w {
            TimeWindow::Last(d) => assert_eq!(d, TimeWindow::DEFAULT_LAST),
            _ => panic!("expected Last(default)"),
        }
    }

    #[test]
    fn last_parses_duration() {
        let w = WindowParse {
            last: Some("30m"),
            ..Default::default()
        }
        .resolve()
        .unwrap();
        match w {
            TimeWindow::Last(d) => assert_eq!(d, Duration::from_secs(1800)),
            _ => panic!(),
        }
    }

    #[test]
    fn since_duration_to_range() {
        let before = Utc::now();
        let w = WindowParse {
            since: Some("1m"),
            ..Default::default()
        }
        .resolve()
        .unwrap();
        match w {
            TimeWindow::Range { from, to } => {
                assert!(to.is_none());
                assert!(from <= before);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn from_to_absolute() {
        let w = WindowParse {
            from: Some("2026-01-02T00:00:00Z"),
            to: Some("2026-01-02T01:00:00Z"),
            ..Default::default()
        }
        .resolve()
        .unwrap();
        match w {
            TimeWindow::Range { from, to } => {
                assert_eq!(from.timestamp(), 1767312000);
                assert_eq!(to.unwrap().timestamp(), 1767315600);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn offset_with_limit() {
        let w = WindowParse {
            offset: Some(OffsetStart::Earliest),
            limit: Some(50),
            ..Default::default()
        }
        .resolve()
        .unwrap();
        match w {
            TimeWindow::Offset { start, limit } => {
                assert_eq!(start, OffsetStart::Earliest);
                assert_eq!(limit, 50);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn offset_without_limit_errors() {
        let err = WindowParse {
            offset: Some(OffsetStart::Latest),
            ..Default::default()
        }
        .resolve()
        .unwrap_err();
        assert!(matches!(err, Error::WindowOffsetWithoutLimit));
    }

    #[test]
    fn conflicting_flags_error() {
        let err = WindowParse {
            last: Some("10m"),
            from: Some("2026-01-02T00:00:00Z"),
            ..Default::default()
        }
        .resolve()
        .unwrap_err();
        assert!(matches!(err, Error::WindowConflict));
    }

    #[test]
    fn to_alone_errors() {
        let err = WindowParse {
            to: Some("2026-01-02T00:00:00Z"),
            ..Default::default()
        }
        .resolve()
        .unwrap_err();
        assert!(matches!(err, Error::WindowToWithoutFrom));
    }

    #[test]
    fn to_before_from_errors() {
        let err = WindowParse {
            from: Some("2026-01-02T01:00:00Z"),
            to: Some("2026-01-02T00:00:00Z"),
            ..Default::default()
        }
        .resolve()
        .unwrap_err();
        assert!(matches!(err, Error::WindowToBeforeFrom));
    }

    #[test]
    fn invalid_rfc3339_errors() {
        let err = WindowParse {
            from: Some("not a date"),
            ..Default::default()
        }
        .resolve()
        .unwrap_err();
        assert!(matches!(err, Error::WindowTimestamp(_, _)));
    }

    #[test]
    fn invalid_duration_errors() {
        let err = WindowParse {
            last: Some("forever and ever"),
            ..Default::default()
        }
        .resolve()
        .unwrap_err();
        assert!(matches!(err, Error::WindowDuration(_, _)));
    }
}
