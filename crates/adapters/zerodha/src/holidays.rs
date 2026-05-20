// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  See LICENSE for full text.
// -------------------------------------------------------------------------------------------------

//! NSE / BSE trading-day gate (spec §5/Phase-7 §6).
//!
//! The reconciler's default 1 s `/orders` poll cadence over off-hours would burn ~57 600
//! requests/day per closed market, tripping Kite's rate limit and emitting `429` storms on
//! every public holiday. The gate in [`should_poll_orders`] pauses polling outside trading
//! hours, on weekends, and on the calendar entries below.
//!
//! # Calendar source
//!
//! The list of NSE/BSE holidays is **checked in** rather than fetched at runtime — Kite doesn't
//! expose holiday data via the Connect API, and a one-line annual update is cheaper than a
//! scraper. Refresh from the NSE annual list before the new financial year:
//!
//! - <https://www.nseindia.com/resources/exchange-communication-holidays>
//! - <https://www.bseindia.com/static/markets/marketinfo/listholi.aspx>
//!
//! The two exchanges' calendars are essentially identical (BSE adds Bakri-Id occasionally;
//! that diverges rarely enough to ignore for v1).
//!
//! # Day-flow timing (IST)
//!
//! ```text
//! 00:00 – 08:00    polling paused (overnight)
//! 08:00 – 09:15    polling resumed (pre-open polling catches AMO acceptances)
//! 09:15 – 15:30    market open, polling active
//! 15:30 – 16:00    polling active (catches MIS auto-square at 15:20)
//! 16:00 – 24:00    polling paused (post-close)
//! ```
//!
//! On weekends and holidays polling is paused for the full 24 h.

use chrono::{Datelike, NaiveDate, TimeZone, Timelike, Utc, Weekday};
use chrono_tz::Asia::Kolkata;

/// Latest hour (IST) at which we keep polling after market open.
pub const POLL_RESUME_HOUR_IST: u32 = 8;

/// Hour (IST) past which we pause polling for the day.
pub const POLL_PAUSE_HOUR_IST: u32 = 16;

/// Known NSE / BSE market holidays for 2026 (full-day equity + derivatives close).
///
/// Source: NSE annual holiday list (2025-12 publication, updated yearly).
///
/// **This must be refreshed annually** before April 1st of each new financial year. A handful
/// of muhurat trading sessions on Diwali are intentionally excluded (they're not full trading
/// days and the spec's reconciler is tolerant of one missed cadence).
pub const NSE_BSE_HOLIDAYS_2026: &[(i32, u32, u32)] = &[
    // (year, month, day)
    (2026, 1, 26),   // Republic Day (Mon)
    (2026, 2, 17),   // Maha Shivratri (Tue)
    (2026, 3, 3),    // Holi (Tue) – approximate; verify NSE list
    (2026, 3, 31),   // Eid-ul-Fitr (Tue) – verify
    (2026, 4, 3),    // Good Friday (Fri)
    (2026, 4, 14),   // Dr. B.R. Ambedkar Jayanti (Tue)
    (2026, 5, 1),    // Maharashtra Day (Fri)
    (2026, 5, 27),   // Buddha Purnima (Wed) – verify
    (2026, 6, 17),   // Eid-ul-Adha / Bakri Eid (Wed) – verify
    (2026, 8, 15),   // Independence Day (Sat — falls on weekend, listed for completeness)
    (2026, 8, 27),   // Ganesh Chaturthi (Thu) – verify
    (2026, 10, 2),   // Mahatma Gandhi Jayanti (Fri)
    (2026, 10, 21),  // Dussehra (Wed) – approximate
    (2026, 11, 9),   // Diwali Balipratipada (Mon) – approximate
    (2026, 11, 25),  // Guru Nanak Jayanti (Wed) – approximate
    (2026, 12, 25),  // Christmas (Fri)
];

/// Whether `date` is on the holiday calendar.
#[must_use]
pub fn is_market_holiday(date: NaiveDate) -> bool {
    NSE_BSE_HOLIDAYS_2026
        .iter()
        .any(|(y, m, d)| *y == date.year() && *m == date.month() && *d == date.day())
}

/// Whether `date` is a normal trading day (weekday + not on the holiday calendar).
#[must_use]
pub fn is_trading_day(date: NaiveDate) -> bool {
    !matches!(date.weekday(), Weekday::Sat | Weekday::Sun) && !is_market_holiday(date)
}

/// Whether the `/orders` reconciler should poll right now.
///
/// `now_utc` is the wall clock. The function converts to IST internally and applies the
/// trading-day + intraday-window gate per the module docs.
#[must_use]
pub fn should_poll_orders(now_utc: chrono::DateTime<Utc>) -> bool {
    let now_ist = now_utc.with_timezone(&Kolkata);
    let date = now_ist.date_naive();
    if !is_trading_day(date) {
        return false;
    }
    let hour = now_ist.hour();
    (POLL_RESUME_HOUR_IST..POLL_PAUSE_HOUR_IST).contains(&hour)
}

/// Skip directly to the next trading day's [`POLL_RESUME_HOUR_IST`] boundary.
///
/// Useful for the reconciler's sleep loop when [`should_poll_orders`] returns `false` — sleep
/// once until the next polling window rather than wake every minute to re-check.
///
/// # Panics
///
/// Panics if the calendar date arithmetic over-runs `NaiveDate`'s representable range (i.e.
/// beyond year ±262 144) or if `Asia/Kolkata` ever stops resolving — both compile-time
/// invariants of the workspace.
#[must_use]
pub fn next_poll_window(now_utc: chrono::DateTime<Utc>) -> chrono::DateTime<Utc> {
    let now_ist = now_utc.with_timezone(&Kolkata);
    let mut candidate_date = now_ist.date_naive();

    // If we're already past today's window or it's a non-trading day, advance.
    let past_today = now_ist.hour() >= POLL_PAUSE_HOUR_IST;
    if past_today || !is_trading_day(candidate_date) {
        candidate_date = candidate_date
            .succ_opt()
            .expect("date arithmetic in-range for the forseeable century");
    }
    while !is_trading_day(candidate_date) {
        candidate_date = candidate_date
            .succ_opt()
            .expect("date arithmetic in-range for the forseeable century");
    }

    let target_naive = candidate_date
        .and_hms_opt(POLL_RESUME_HOUR_IST, 0, 0)
        .expect("POLL_RESUME_HOUR_IST is a valid hour");
    Kolkata
        .from_local_datetime(&target_naive)
        .single()
        .expect("Asia/Kolkata has no DST; this resolves uniquely")
        .with_timezone(&Utc)
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use rstest::rstest;

    use super::*;

    fn ist(year: i32, month: u32, day: u32, hour: u32) -> chrono::DateTime<Utc> {
        Kolkata
            .with_ymd_and_hms(year, month, day, hour, 0, 0)
            .single()
            .unwrap()
            .with_timezone(&Utc)
    }

    #[rstest]
    fn republic_day_is_holiday() {
        assert!(is_market_holiday(NaiveDate::from_ymd_opt(2026, 1, 26).unwrap()));
    }

    #[rstest]
    fn random_weekday_is_not_holiday() {
        assert!(!is_market_holiday(NaiveDate::from_ymd_opt(2026, 5, 20).unwrap()));
    }

    #[rstest]
    fn saturday_is_not_a_trading_day() {
        // 2026-05-23 is a Saturday.
        assert!(!is_trading_day(NaiveDate::from_ymd_opt(2026, 5, 23).unwrap()));
    }

    #[rstest]
    fn weekday_non_holiday_is_a_trading_day() {
        assert!(is_trading_day(NaiveDate::from_ymd_opt(2026, 5, 20).unwrap()));
    }

    #[rstest]
    fn polls_during_market_hours_on_a_weekday() {
        // 2026-05-20 (Wed) at 10:00 IST.
        assert!(should_poll_orders(ist(2026, 5, 20, 10)));
    }

    #[rstest]
    fn does_not_poll_overnight() {
        // 03:00 IST on a weekday — pre-resume window.
        assert!(!should_poll_orders(ist(2026, 5, 20, 3)));
    }

    #[rstest]
    fn does_not_poll_post_pause_hour() {
        // 17:00 IST on a weekday — post 16:00 pause.
        assert!(!should_poll_orders(ist(2026, 5, 20, 17)));
    }

    #[rstest]
    fn does_not_poll_on_weekend() {
        // 11:00 IST on a Saturday.
        assert!(!should_poll_orders(ist(2026, 5, 23, 11)));
    }

    #[rstest]
    fn does_not_poll_on_holiday() {
        // Republic Day at noon.
        assert!(!should_poll_orders(ist(2026, 1, 26, 12)));
    }

    #[rstest]
    fn next_window_skips_a_holiday() {
        // 17:00 IST on Republic Day (Mon 26 Jan 2026). Next trading day is Tue 27 Jan.
        let after_close = ist(2026, 1, 26, 17);
        let next = next_poll_window(after_close);
        let next_ist = next.with_timezone(&Kolkata);
        assert_eq!(next_ist.year(), 2026);
        assert_eq!(next_ist.month(), 1);
        assert_eq!(next_ist.day(), 27);
        assert_eq!(next_ist.hour(), 8);
    }

    #[rstest]
    fn next_window_skips_a_weekend() {
        // 17:00 IST on Friday 22 May 2026; next trading day is Mon 25 May.
        let friday_evening = ist(2026, 5, 22, 17);
        let next = next_poll_window(friday_evening);
        let next_ist = next.with_timezone(&Kolkata);
        assert_eq!(next_ist.day(), 25);
        assert_eq!(next_ist.weekday(), Weekday::Mon);
    }

    #[rstest]
    fn next_window_today_when_pre_resume() {
        // 03:00 IST on a weekday; next window is today at 08:00 IST.
        let early_morning = ist(2026, 5, 20, 3);
        let next = next_poll_window(early_morning);
        let next_ist = next.with_timezone(&Kolkata);
        assert_eq!(next_ist.day(), 20);
        assert_eq!(next_ist.hour(), 8);
    }
}
