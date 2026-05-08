use std::time::Duration;

#[derive(Debug, Clone, Copy)]
pub struct Rusage {
    pub user: Duration,
    pub sys: Duration,
}

impl Rusage {
    #[cfg(unix)]
    pub fn now() -> Self {
        let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
        unsafe {
            libc::getrusage(libc::RUSAGE_SELF, &mut ru);
        }
        Self {
            user: tv_to_duration(ru.ru_utime),
            sys: tv_to_duration(ru.ru_stime),
        }
    }

    // Windows lacks `getrusage`; return zero so the harness still compiles
    // cross-platform. The CPU× column in bench output reads as 0.00× on
    // Windows. Real CPU instrumentation would use GetProcessTimes via
    // `windows-sys`; not worth the dep weight for a dev-only harness.
    #[cfg(not(unix))]
    pub fn now() -> Self {
        Self {
            user: Duration::ZERO,
            sys: Duration::ZERO,
        }
    }

    pub fn diff(&self, before: &Rusage) -> Rusage {
        Rusage {
            user: self.user.saturating_sub(before.user),
            sys: self.sys.saturating_sub(before.sys),
        }
    }
}

#[cfg(unix)]
fn tv_to_duration(tv: libc::timeval) -> Duration {
    Duration::from_secs(tv.tv_sec as u64) + Duration::from_micros(tv.tv_usec as u64)
}
