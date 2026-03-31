//! OS signal handling for graceful shutdown and hot-reload.
//!
//! Installs `SIGTERM`/`SIGINT` → shutdown and `SIGHUP` → reload handlers that
//! flip `Arc<AtomicBool>` flags owned by the caller.

use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::sync::Arc;

static SHUTDOWN_PTR: AtomicPtr<AtomicBool> = AtomicPtr::new(ptr::null_mut());
static RELOAD_PTR: AtomicPtr<AtomicBool> = AtomicPtr::new(ptr::null_mut());

extern "C" fn handle_shutdown(_sig: libc::c_int) {
    let ptr = SHUTDOWN_PTR.load(Ordering::Relaxed);
    if !ptr.is_null() {
        unsafe { &*ptr }.store(true, Ordering::Release);
    }
}

extern "C" fn handle_reload(_sig: libc::c_int) {
    let ptr = RELOAD_PTR.load(Ordering::Relaxed);
    if !ptr.is_null() {
        unsafe { &*ptr }.store(true, Ordering::Release);
    }
}

fn install(sig: libc::c_int, handler: extern "C" fn(libc::c_int)) {
    unsafe {
        let mut sa: libc::sigaction = std::mem::zeroed();
        sa.sa_sigaction = handler as *const () as usize;
        sa.sa_flags = libc::SA_RESTART;
        libc::sigemptyset(&mut sa.sa_mask);
        libc::sigaction(sig, &sa, ptr::null_mut());
    }
}

/// Install signal handlers and return `(shutdown, reload)` flag pair.
pub fn setup() -> (Arc<AtomicBool>, Arc<AtomicBool>) {
    let shutdown = Arc::new(AtomicBool::new(false));
    SHUTDOWN_PTR.store(Arc::as_ptr(&shutdown) as *mut AtomicBool, Ordering::Release);

    let reload = Arc::new(AtomicBool::new(false));
    RELOAD_PTR.store(Arc::as_ptr(&reload) as *mut AtomicBool, Ordering::Release);

    install(libc::SIGTERM, handle_shutdown);
    install(libc::SIGINT, handle_shutdown);
    install(libc::SIGHUP, handle_reload);

    (shutdown, reload)
}

/// Null out the global flag pointers before the Arcs drop.
pub fn clear() {
    SHUTDOWN_PTR.store(ptr::null_mut(), Ordering::Release);
    RELOAD_PTR.store(ptr::null_mut(), Ordering::Release);
}
