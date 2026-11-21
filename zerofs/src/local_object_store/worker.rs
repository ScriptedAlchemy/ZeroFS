//! Bounded, reusable workers outside every Tokio runtime.
//!
//! The native object store performs its I/O inline only outside a Tokio
//! context. Keep that cancellation/lock ownership property without creating
//! a new operating-system thread for every object, multipart chunk or listing.

use super::generic;
use object_store::Result;
use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::Duration;
use tokio::sync::{Semaphore, oneshot};

const MAX_WORKERS: usize = 32;
const IDLE_TIMEOUT: Duration = Duration::from_secs(10);
type Job = Box<dyn FnOnce() + Send + 'static>;

#[derive(Default)]
struct State {
    jobs: VecDeque<Job>,
    workers: usize,
    idle: usize,
}

struct Workers {
    admission: Arc<Semaphore>,
    maximum: usize,
    idle_timeout: Duration,
    state: Mutex<State>,
    available: Condvar,
}

impl Workers {
    fn new(maximum: usize, idle_timeout: Duration) -> Arc<Self> {
        assert!(maximum > 0);
        Arc::new(Self {
            admission: Arc::new(Semaphore::new(maximum)),
            maximum,
            idle_timeout,
            state: Mutex::new(State::default()),
            available: Condvar::new(),
        })
    }

    async fn run<T, F>(self: &Arc<Self>, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T> + Send + 'static,
    {
        let permit = self
            .admission
            .clone()
            .acquire_owned()
            .await
            .map_err(generic)?;
        let (sender, receiver) = oneshot::channel();
        // Queueing is synchronous after admission. Cancellation can abandon
        // the response, but the job owns its permit and locks through all I/O.
        self.submit(Box::new(move || {
            let _permit = permit;
            let _ = sender.send(operation());
        }))?;
        receiver
            .await
            .map_err(|_| generic("local storage worker stopped before returning a result"))?
    }

    fn submit(self: &Arc<Self>, job: Job) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        // Already-queued work accounts for idle threads that have been woken
        // but have not acquired this mutex yet. Grow only to meet real demand.
        if state.idle <= state.jobs.len() && state.workers < self.maximum {
            let workers = Arc::clone(self);
            std::thread::Builder::new()
                .name("zerofs-local-store".into())
                .spawn(move || workers.work())
                .map_err(generic)?;
            state.workers += 1;
        }
        state.jobs.push_back(job);
        self.available.notify_one();
        Ok(())
    }

    fn work(&self) {
        loop {
            let job = {
                let mut state = self.state.lock().unwrap();
                loop {
                    if let Some(job) = state.jobs.pop_front() {
                        break job;
                    }
                    state.idle += 1;
                    let (next, timeout) = self
                        .available
                        .wait_timeout(state, self.idle_timeout)
                        .unwrap();
                    state = next;
                    state.idle -= 1;
                    if timeout.timed_out() && state.jobs.is_empty() {
                        // Retirement and submission share the mutex. A job
                        // arriving at the idle deadline cannot be stranded.
                        state.workers -= 1;
                        return;
                    }
                }
            };
            // Match a detached thread's failed-response behavior on panic,
            // while retaining the worker capacity for subsequent operations.
            // User work never executes under the queue mutex.
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(job));
        }
    }
}

pub(super) async fn run_local<T, F>(operation: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
{
    static WORKERS: OnceLock<Arc<Workers>> = OnceLock::new();
    WORKERS
        .get_or_init(|| Workers::new(MAX_WORKERS, IDLE_TIMEOUT))
        .run(operation)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[tokio::test]
    async fn serial_operations_reuse_a_thread_outside_tokio() {
        let workers = Workers::new(1, IDLE_TIMEOUT);
        let first = workers
            .run(|| {
                assert!(tokio::runtime::Handle::try_current().is_err());
                Ok(std::thread::current().id())
            })
            .await
            .unwrap();
        for _ in 0..128 {
            let next = workers
                .run(|| {
                    assert!(tokio::runtime::Handle::try_current().is_err());
                    Ok(std::thread::current().id())
                })
                .await
                .unwrap();
            assert_eq!(next, first);
        }
        assert_eq!(workers.state.lock().unwrap().workers, 1);
    }

    #[tokio::test]
    async fn cancellation_retains_admission_until_native_work_finishes() {
        let workers = Workers::new(1, IDLE_TIMEOUT);
        let (entered, started) = oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let completed = Arc::new(AtomicBool::new(false));
        let operation_completed = Arc::clone(&completed);
        let operation_workers = Arc::clone(&workers);
        let task = tokio::spawn(async move {
            operation_workers
                .run(move || {
                    entered.send(()).unwrap();
                    wait.recv().unwrap();
                    operation_completed.store(true, Ordering::Release);
                    Ok(())
                })
                .await
        });
        started.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(workers.admission.available_permits(), 0);

        let next = workers.run(move || {
            assert!(completed.load(Ordering::Acquire));
            Ok(())
        });
        tokio::pin!(next);
        assert!(futures::poll!(next.as_mut()).is_pending());
        release.send(()).unwrap();
        next.await.unwrap();
    }

    #[tokio::test]
    async fn concurrent_jobs_are_bounded_and_idle_threads_retire() {
        let workers = Workers::new(2, Duration::from_millis(10));
        let mut tasks = Vec::new();
        let mut releases = Vec::new();
        for _ in 0..2 {
            let (entered, started) = oneshot::channel();
            let (release, wait) = std::sync::mpsc::channel();
            let operation_workers = Arc::clone(&workers);
            tasks.push(tokio::spawn(async move {
                operation_workers
                    .run(move || {
                        entered.send(()).unwrap();
                        wait.recv().unwrap();
                        Ok(())
                    })
                    .await
            }));
            started.await.unwrap();
            releases.push(release);
        }
        assert_eq!(workers.state.lock().unwrap().workers, 2);
        let next = workers.run(|| Ok(()));
        tokio::pin!(next);
        assert!(futures::poll!(next.as_mut()).is_pending());
        for release in releases {
            release.send(()).unwrap();
        }
        for task in tasks {
            task.await.unwrap().unwrap();
        }
        next.await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while workers.state.lock().unwrap().workers != 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        // Restart after every native worker has retired.
        assert_eq!(workers.run(|| Ok(7)).await.unwrap(), 7);
    }

    #[tokio::test]
    async fn panicking_operation_releases_capacity_for_following_work() {
        let workers = Workers::new(1, IDLE_TIMEOUT);
        assert!(
            workers
                .run::<(), _>(|| panic!("injected worker failure"))
                .await
                .is_err()
        );
        assert_eq!(workers.run(|| Ok(7)).await.unwrap(), 7);
    }
}
