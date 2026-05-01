use std::time::Duration;

#[derive(Debug, Clone, Copy)]
pub struct Rusage {
    pub user: Duration,
    pub sys: Duration,
}

impl Rusage {
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

    pub fn diff(&self, before: &Rusage) -> Rusage {
        Rusage {
            user: self.user.saturating_sub(before.user),
            sys: self.sys.saturating_sub(before.sys),
        }
    }
}

fn tv_to_duration(tv: libc::timeval) -> Duration {
    Duration::from_secs(tv.tv_sec as u64) + Duration::from_micros(tv.tv_usec as u64)
}
