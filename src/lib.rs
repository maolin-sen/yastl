//! A library to provide a thread pool that can run scoped and unscoped threads.
//!
//!
//! It can be used to spawn threads, that are guaranteed to be finished if the scope ends,
//! and thus make it possible to borrow values from the stack (not requiring the `'static` bound).
//!
//! # Example
//!
//! ```
//! # use yastl::Pool;
//!
//! # fn main() {
//! let pool = Pool::new(4);
//! let mut list = vec![1, 2, 3, 4, 5];
//!
//! pool.scoped(|scope| {
//!
//!     // since the `scope` guarantees that the threads are finished if it drops,
//!     // we can safely borrow `list` inside here.
//!     for x in list.iter_mut() {
//!         scope.execute(move || { *x += 2; });
//!     }
//! });
//!
//! assert_eq!(list, vec![3, 4, 5, 6, 7]);
//! # }
//! ```
#![deny(rust_2018_idioms, missing_docs, broken_intra_doc_links)]

mod wait;
use hwloc2::{Topology, TopologyFlag, ObjectType, TopologyObject, CpuBindFlags};
use wait::{Sentinel, WaitGroup};

mod scope;
pub use scope::Scope;

use flume::{Receiver, Sender};
use std::{sync::{Arc, Mutex}, thread};
use lazy_static::lazy_static;
use anyhow::{Result, format_err};

lazy_static! {
    static ref TOPOLOGY: Mutex<Topology> =
        Mutex::new(Topology::with_flags(vec![TopologyFlag::IncludeDisallowed]).unwrap());
}

#[cfg(not(target_os = "windows"))]
type ThreadId = libc::pthread_t;

#[cfg(target_os = "windows")]
pub type ThreadId = winapi::winnt::HANDLE;

/// Helper method to get the thread id through libc, with current rust stable (1.5.0) its not
/// possible otherwise I think.
#[cfg(not(target_os = "windows"))]
fn get_thread_id() -> ThreadId {
    unsafe { libc::pthread_self() }
}

#[cfg(target_os = "windows")]
fn get_thread_id() -> ThreadId {
    unsafe { kernel32::GetCurrentThread() }
}

fn get_core_by_index(topo: &Topology, index: usize) -> Result<&TopologyObject> {
    let idx = index;

    match topo.objects_with_type(&ObjectType::PU) {
        Ok(all_pus) if idx < all_pus.len() => Ok(all_pus[idx]),
        _e => Err(format_err!("failed to get pu by index {}", idx,)),
    }
}

fn bind_core(core_idx:usize){
    let child_topo = &TOPOLOGY;
    let tid = get_thread_id();
    let mut locked_topo = child_topo.lock().expect("poisoned lock");
    let pu = get_core_by_index(&locked_topo, core_idx)
        .map_err(|err| format_err!("failed to get core at index {}: {:?}", core_idx, err)).unwrap();

    let cpuset = pu
        .complete_cpuset()
        .ok_or_else(|| format_err!("no allowed cpuset for core at index {}", core_idx)).unwrap();
    println!("coreIdx: {:?} allowed cpuset: {:?}", core_idx, cpuset);
    let bind_to = cpuset;

    println!("CoreIndex {:?} binding to {:?}", core_idx, bind_to);
    // Set the binding.
    let result = locked_topo
        .set_cpubind_for_thread(tid, bind_to, CpuBindFlags::CPUBIND_THREAD)
        .map_err(|err| format_err!("failed to bind CPU: {:?}", err));

    if result.is_err() {
        println!("error in bind_core, {:?}", result);
    }
}
/// A structure providing access to a pool of worker threads and a way to spawn jobs.
///
/// It spawns `n` threads at creation and then can be used to spawn scoped threads via
/// [`Pool::scoped()`] or unscoped threads via [`Pool::spawn()`].
#[derive(Clone)]
pub struct Pool {
    inner: Arc<PoolInner>,
    wait: Arc<wait::WaitGroup>,
}

impl Pool {
    /// Create a new `Pool` that will execute it's tasks on `n` worker threads.
    ///
    /// # Panics
    ///
    /// If `n` is zero.
    pub fn new(n: usize,cores:Vec<usize>) -> Self {
        Self::with_config(n, ThreadConfig{
            prefix: None,
            stack_size: None,
            cores: Some(Arc::new(cores))
        })
    }

    /// Create a new `Pool` that will execute it's tasks on `n` worker threads and spawn them using
    /// the given config.
    ///
    /// # Panics
    ///
    /// If `n` is zero.
    pub fn with_config(n: usize, config: ThreadConfig) -> Self {
        assert!(n > 0, "can not create a thread pool with 0 workers");

        let pool = Self {
            inner: Arc::new(PoolInner::with_config(config)),
            wait: Arc::new(WaitGroup::default()),
        };

        for id in 0..n {
            let builder = thread::Builder::new();
            let mut builder = if let Some(prefix) = pool.inner.config.prefix.as_ref() {
                builder.name(format!("{}-{}", prefix, id + 1))
            } else {
                builder.name(format!("worker-thread-{}", id + 1))
            };

            if let Some(stack_size) = pool.inner.config.stack_size {
                builder = builder.stack_size(stack_size);
            }

            let mut cores:Arc<Vec<usize>> = Arc::new(Vec::new());
            if let Some(core) = pool.inner.config.cores.clone() {
                cores = core;
            }

            let this = pool.clone();
            builder
                .spawn(move || {
                    if cores.len() > 0 {
                        bind_core(cores[id]);
                    }
                    this.run_thread()
                })
                .expect("failed to spawn worker thread");
        }

        pool
    }

    /// Spawn an unscoped job onto this thread pool.
    ///
    /// This method doesn't wait until the job finishes.
    pub fn spawn<F: FnOnce() + Send + 'static>(&self, job: F) {
        Scope::forever(self.clone()).execute(job)
    }

    /// Spawn scoped jobs which guarantee to be finished before this method returns and thus allows
    /// to borrow local varaiables.
    pub fn scoped<'scope, F, R>(&self, job: F) -> R
    where
        F: FnOnce(&Scope<'scope>) -> R,
    {
        Scope::forever(self.clone()).zoom(job)
    }

    /// Send a shutdown signal to every worker thread and wait for their termination.
    pub fn shutdown(&self) {
        self.inner
            .msg_tx
            .send(Message::Stop)
            .expect("failed to send message");

        self.wait.join()
    }

    fn run_thread(self) {
        #[cfg(feature = "coz")]
        coz::thread_init();

        let thread_sentinel = Sentinel(Some(self.wait.clone()));

        loop {
            match self.inner.msg_rx.recv() {
                Ok(Message::Stop) => {
                    // the pool only sends one `Stop` message so we duplicate it
                    // to propagate the stops into other threads too
                    self.inner
                        .msg_tx
                        .send(Message::Stop)
                        .expect("failed to send message");

                    thread_sentinel.cancel();
                    break;
                }
                Ok(Message::Job(job, wait)) => {
                    let sentinel = Sentinel(Some(wait.clone()));
                    job.call_box();
                    sentinel.cancel();
                }
                // we break on `Err` because this means that all senders are dropped do there are
                // no more messages
                Err(..) => break,
            }
        }
    }
}

/// Provide configuration parameters to the spawned threads like a name prefix.
pub struct ThreadConfig {
    prefix: Option<String>,
    stack_size: Option<usize>,
    cores:Option<Arc<Vec<usize>>>,
}

impl Default for ThreadConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl ThreadConfig {
    /// Create an empty `ThreadConfig` which can be used to configure thread spawning using it's
    /// builder like methods.
    pub fn new() -> Self {
        Self {
            prefix: None,
            stack_size: None,
            cores: None,
        }
    }

    /// Set a common prefix for the worker thread names.
    ///
    /// The full name is composed like this:
    /// ```ignore
    /// <prefix>-<id>
    /// ```
    ///
    /// The default `prefix` is "worker-thread".
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = Some(prefix.into());
        self
    }

    /// Set that size of the stack of each spawned thread.
    pub fn stack_size(mut self, stack_size: usize) -> Self {
        self.stack_size = Some(stack_size);
        self
    }

    /// Set that size of the stack of each spawned thread.
    pub fn cores(mut self, cores:Vec<usize>) -> Self {
        self.cores = Some(Arc::new(cores));
        self
    }
}

struct PoolInner {
    msg_rx: Receiver<Message>,
    msg_tx: Sender<Message>,
    config: ThreadConfig,
}

impl PoolInner {
    fn with_config(config: ThreadConfig) -> Self {
        let (tx, rx) = flume::unbounded();
        Self {
            msg_rx: rx,
            msg_tx: tx,
            config,
        }
    }
}

/// Messages that are sent to the worker threads.
enum Message {
    Stop,
    Job(Thunk<'static>, Arc<WaitGroup>),
}

trait FnBox {
    fn call_box(self: Box<Self>);
}

impl<F: FnOnce()> FnBox for F {
    fn call_box(self: Box<F>) {
        (*self)()
    }
}

type Thunk<'a> = Box<dyn FnBox + Send + 'a>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::thread::sleep;
    use std::time::Duration;

    struct Canary<'a> {
        drops: DropCounter<'a>,
        expected: usize,
    }

    #[derive(Clone)]
    struct DropCounter<'a>(&'a AtomicUsize);

    impl<'a> Drop for DropCounter<'a> {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl<'a> Drop for Canary<'a> {
        fn drop(&mut self) {
            let drops = self.drops.0.load(Ordering::SeqCst);
            assert_eq!(drops, self.expected);
        }
    }

    #[test]
    fn scope_zoom() {
        let pool = Pool::new(3,Vec::from([0,1,2]));
        let mut outer = 0;

        pool.scoped(|scope| {
            let mut inner = 0;
            scope.zoom(|scope2| scope2.execute(|| inner = 1));
            assert_eq!(inner, 1);
            outer = 1;
        });

        assert_eq!(outer, 1);
    }

    #[test]
    fn scope_recurse() {
        let pool = Pool::new(3,Vec::from([0,1,2]));
        let mut buf = [0, 1, 0, 0];

        pool.scoped(|next| {
            next.recurse(|next| {
                buf[0] += 1;
                buf[1] += 14;

                next.execute(|| {
                    buf[2] = 12;
                    buf[3] = 543;
                });
            });
        });

        assert_eq!(&buf, &[1, 15, 12, 543]);
    }

    #[test]
    fn spawn_doesnt_block() {
        let pool = Pool::new(3,Vec::from([0,1,2]));
        pool.spawn(move || loop {
            sleep(Duration::from_millis(1000))
        });
    }

    #[test]
    fn scope_forever_zoom() {
        let pool = Pool::new(3,Vec::from([0,1,2]));
        let forever = Scope::forever(pool);

        let ran = AtomicBool::new(false);
        forever.zoom(|scope| scope.execute(|| ran.store(true, Ordering::SeqCst)));
        assert!(ran.load(Ordering::SeqCst));
    }

    #[test]
    fn pool_shutdown() {
        let pool = Pool::new(3,Vec::from([0,1,2]));
        pool.shutdown();
    }

    #[test]
    #[should_panic]
    fn task_panic() {
        let pool = Pool::new(3,Vec::from([0,1,2]));
        pool.scoped(|_| panic!());
    }

    #[test]
    #[should_panic]
    fn scoped_execute_panic() {
        let pool = Pool::new(3,Vec::from([0,1,2]));
        pool.scoped(|scope| scope.execute(|| panic!()));
    }

    #[test]
    #[should_panic]
    fn pool_panic() {
        let _pool = Pool::new(3,Vec::from([0,1,2]));
        panic!();
    }

    #[test]
    #[should_panic]
    fn zoomed_scoped_execute_panic() {
        let pool = Pool::new(3,Vec::from([0,1,2]));
        pool.scoped(|scope| scope.zoom(|scope2| scope2.execute(|| panic!())));
    }

    #[test]
    #[should_panic]
    fn recurse_scheduler_panic() {
        let pool = Pool::new(3,Vec::from([0,1,2]));
        pool.scoped(|scope| scope.recurse(|_| panic!()));
    }

    #[test]
    #[should_panic]
    fn recurse_execute_panic() {
        let pool = Pool::new(3,Vec::from([0,1,2]));
        pool.scoped(|scope| scope.recurse(|scope2| scope2.execute(|| panic!())));
    }

    #[test]
    #[should_panic]
    fn scoped_panic_waits_for_all_tasks() {
        let tasks = 50;
        let panicking_task_fraction = 10;
        let panicking_tasks = tasks / panicking_task_fraction;
        let expected_drops = tasks + panicking_tasks;

        let counter = Box::new(AtomicUsize::new(0));
        let drops = DropCounter(&*counter);

        // Actual check occurs on drop of this during unwinding.
        let _canary = Canary {
            drops: drops.clone(),
            expected: expected_drops,
        };

        let pool = Pool::new(3,Vec::from([0,1,2]));

        pool.scoped(|scope| {
            for task in 0..tasks {
                let drop_counter = drops.clone();

                scope.execute(move || {
                    sleep(Duration::from_millis(10));

                    drop::<DropCounter<'_>>(drop_counter);
                });

                if task % panicking_task_fraction == 0 {
                    let drop_counter = drops.clone();

                    scope.execute(move || {
                        // Just make sure we capture it.
                        let _drops = drop_counter;
                        panic!();
                    });
                }
            }
        });
    }

    #[test]
    #[should_panic]
    fn scheduler_panic_waits_for_tasks() {
        let tasks = 50;
        let counter = Box::new(AtomicUsize::new(0));
        let drops = DropCounter(&*counter);

        let _canary = Canary {
            drops: drops.clone(),
            expected: tasks,
        };

        let pool = Pool::new(3,Vec::from([0,1,2]));

        pool.scoped(|scope| {
            for _ in 0..tasks {
                let drop_counter = drops.clone();

                scope.execute(move || {
                    sleep(Duration::from_millis(25));
                    drop::<DropCounter<'_>>(drop_counter);
                });
            }

            panic!();
        });
    }

    #[test]
    fn no_thread_config() {
        let pool = Pool::new(1,Vec::from([0]));

        pool.scoped(|scope| {
            scope.execute(|| {
                assert_eq!(::std::thread::current().name().unwrap(), "worker-thread-1");
            });
        });
    }

    #[test]
    fn with_thread_config() {
        let mut vec = Vec::new();
        vec.insert(0, 0);
        let config = ThreadConfig::new().prefix("pool").cores(vec);


        let pool = Pool::with_config(1, config);

        pool.scoped(|scope| {
            scope.execute(|| {
                assert_eq!(::std::thread::current().name().unwrap(), "pool-1");
            });
        });
    }
}
