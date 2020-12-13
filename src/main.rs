use anyhow::{anyhow, Result};
use atomic::{AtomicBool, AtomicI64, AtomicU64};
use getopts::Options;
use io::{Read, Write};
use libc::{uid_t, STDERR_FILENO, STDIN_FILENO, STDOUT_FILENO};
use nix::sys::{self, signal};
use nix::unistd::{self, Uid, User, ROOT};
use once_cell::sync::Lazy;
use socket2::{Domain, Socket, Type};
use std::{
    convert::TryFrom,
    env, fs, io, net,
    os::unix::io::IntoRawFd,
    sync::{atomic, Mutex},
};

const USAGE_PREFIX: &str = "Usage: filed [<options>]";

// Compile time constants
const FILED_VERSION: &str = "1.21";

// Default values
const PORT: u16 = 80;
const THREAD_COUNT: u32 = 5;
const BIND_ADDR: &str = "::";
const CACHE_SIZE: u32 = 8209;
const LOG_FILE: &str = "-";

struct ThreadOptions {
    vhosts_enabled: bool,
    fake_newroot: Option<String>,
}

impl Default for ThreadOptions {
    fn default() -> Self {
        ThreadOptions {
            vhosts_enabled: false,
            fake_newroot: None,
        }
    }
}

/* File information */
struct FileInfo {
    path: String,
    fd: Option<fs::File>,
    len: u64,
    lastmod: String,
    lastmod_b: String,
    my_type: String,
    etag: String,
}

impl Default for FileInfo {
    fn default() -> Self {
        Self {
            path: String::new(),
            fd: None,
            lastmod: String::new(),
            lastmod_b: String::new(),
            len: 0,
            my_type: String::new(),
            etag: String::new(),
        }
    }
}

static FILE_CACHE: Lazy<Mutex<Vec<Mutex<FileInfo>>>> = Lazy::new(|| Mutex::new(Vec::new()));

static CACHE_INITIALIZED: Lazy<Mutex<bool>> = Lazy::new(|| Mutex::new(false));

struct SocketTimeoutStatus {
    expiration_time: AtomicI64,
    thread_id: AtomicU64,
    valid: AtomicBool,
}

impl Default for SocketTimeoutStatus {
    fn default() -> Self {
        Self {
            expiration_time: AtomicI64::new(0),
            thread_id: AtomicU64::new(0),
            valid: AtomicBool::new(false),
        }
    }
}

static TIMEOUT_STATUSES: Lazy<Mutex<Vec<SocketTimeoutStatus>>> =
    Lazy::new(|| Mutex::new(Vec::new()));

static DEVNULL_FILE: Lazy<fs::File> = Lazy::new(|| {
    fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/null")
        .unwrap()
});

/* Resolve a username to a UID */
fn filed_user_lookup(user: &str) -> Result<Uid> {
    if let Ok(Some(u)) = User::from_name(user) {
        return Ok(u.uid);
    }

    let parsed = user.parse::<uid_t>();
    Ok(parsed.map(Uid::from_raw)?)
}

fn filed_log_open(file: &str) -> Result<Box<dyn io::Write>> {
    Ok(if file == "-" {
        Box::new(io::stdout())
    } else if file
        .chars()
        .next()
        .and_then(|c| if c == '|' { Some(c) } else { None })
        .is_some()
    {
        fs::OpenOptions::new()
            .write(true)
            .open(&file[1..])
            .map(|f| Box::new(f) as Box<dyn io::Write>)?
    } else {
        fs::OpenOptions::new()
            .append(true)
            .open(file)
            .map(|f| Box::new(f) as Box<dyn io::Write>)?
    })
}

/* Listen on a particular address/port */
fn filed_listen(address: &str, port: u16) -> Result<net::TcpListener> {
    let address = net::SocketAddr::new(address.parse()?, port);
    let socket = Socket::new(Domain::ipv6(), Type::stream(), None)?;
    socket.bind(&address.into())?;
    socket.listen(128)?;
    Ok(socket.into_tcp_listener())
}

fn filed_sockettimeout_init() -> Result<()> {
    let maxfd = unistd::sysconf(unistd::SysconfVar::OPEN_MAX)?.unwrap_or(4096);

    let mut statuses = TIMEOUT_STATUSES.lock().unwrap();
    for _ in 0..maxfd {
        statuses.push(SocketTimeoutStatus::default());
    }

    Ok(())
}

/* Daemonize */
fn filed_daemonize() -> Result<()> {
    unistd::chdir("/")?;

    match unsafe { unistd::fork()? } {
        unistd::ForkResult::Parent { child } => {
            sys::wait::waitpid(child, None)?;
            std::process::exit(0);
        }
        unistd::ForkResult::Child => {
            match unsafe { unistd::fork()? } {
                unistd::ForkResult::Parent { .. } => std::process::exit(0),
                unistd::ForkResult::Child => {
                    // Grandchild
                    unistd::setsid()?;

                    let fd_in = fs::OpenOptions::new().read(true).open("/dev/null")?;
                    let fd_out = fs::OpenOptions::new().write(true).open("/dev/null")?;
                    let fd_out2 = fs::OpenOptions::new().write(true).open("/dev/null")?;

                    unistd::dup2(fd_in.into_raw_fd(), STDIN_FILENO)?;
                    unistd::dup2(fd_out.into_raw_fd(), STDOUT_FILENO)?;
                    unistd::dup2(fd_out2.into_raw_fd(), STDERR_FILENO)?;
                    Ok(())
                }
            }
        }
    }
}

/* Initialize cache */
fn filed_init_cache(cache_size: u32) -> Result<()> {
    /* Cache may not be re-initialized */
    if *CACHE_INITIALIZED.lock().unwrap() {
        return Err(anyhow!("Cache may not be re-initialized"));
    }

    /* Cache does not need to be allocated if cache is not enabled */
    if cache_size == 0 {
        return Ok(());
    }

    /* Initialize cache entries */
    for _ in 0..cache_size {
        let cache_entry = FileInfo::default();
        FILE_CACHE.lock().unwrap().push(Mutex::new(cache_entry));
    }

    Ok(())
}

/* Initialize process */
fn filed_init(cache_size: u32) -> Result<()> {
    static CALLED: Lazy<Mutex<bool>> = Lazy::new(|| Mutex::new(false));

    if *CALLED.lock().unwrap() {
        return Ok(());
    }

    *CALLED.lock().unwrap() = true;

    /* Attempt to lock all memory to physical RAM (but don't care if we can't) */
    let _ = sys::mman::mlockall(
        sys::mman::MlockAllFlags::MCL_CURRENT | sys::mman::MlockAllFlags::MCL_FUTURE,
    );

    /* Establish signal handlers */
    /* SIGPIPE should interrupt system calls */
    let sigaction = signal::SigAction::new(
        signal::SigHandler::Handler(filed_signal_handler),
        signal::SaFlags::SA_RESTART,
        signal::SigSet::all(),
    );
    unsafe { signal::sigaction(signal::Signal::SIGPIPE, &sigaction)? };

    /* Handle SIGHUP to release all caches */

    let sigaction = signal::SigAction::new(
        signal::SigHandler::Handler(filed_signal_handler),
        signal::SaFlags::empty(),
        signal::SigSet::all(),
    );
    unsafe { signal::sigaction(signal::Signal::SIGHUP, &sigaction)? };

    /* Initialize cache structure */
    filed_init_cache(cache_size)?;

    Ok(())
}

/* Signal Handler */
#[link(name = "filed_signal_handler")]
extern "C" fn filed_signal_handler(signal_number: libc::c_int) {
    let signal = signal::Signal::try_from(signal_number).unwrap();
    if signal == signal::Signal::SIGHUP {
        for f in FILE_CACHE.lock().unwrap().iter() {
            let mut cache = f.lock().unwrap();
            cache.path.clear();
            cache.fd = None;
            cache.lastmod.clear();
            cache.my_type.clear();
        }
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let mut options = Options::new();
    let mut thread_options = ThreadOptions::default();
    let mut bind_addr = BIND_ADDR.to_string();
    let mut newroot: Option<String> = None;
    let mut log_file = LOG_FILE.to_string();
    let mut user = ROOT;
    let mut port = PORT;
    let mut thread_count = THREAD_COUNT;
    let mut cache_size = CACHE_SIZE;
    let mut daemon_enabled = false;
    let mut setuid_enabled = false;
    let log_fp: Box<dyn Write>;
    let listener: net::TcpListener;

    /* Process arguments */
    options.optopt("p", "port", &format!("specifies the TCP port number to listen for incoming HTTP requests on.  The default is {}.", PORT), "<port>");
    options.optopt("t", "threads", &format!("specifies the number of worker threads to create. Each worker thread can service one concurrent HTTP session. Thus the number of threads created will determine how many simultaneous transfers will be possible. The default is {}.", THREAD_COUNT), "<count>");
    options.optopt("c", "cache", &format!("specifies the number of file information cache entries to allocate.  Each cache entry holds file information as well as an open file descriptor to the file, so resource limits (i.e., ulimit) should be considered.  This should be a prime number for ideal use with the lookup method. The default is {}.", CACHE_SIZE), "<entries>");
    options.optopt("b", "bind", &format!("specifies the address to listen for incoming HTTP requests on.  The default value is \"{}\".", BIND_ADDR), "<address>");
    options.optopt("u", "user", "specifies the user to switch user IDs to before servicing requests.  The default is not change user IDs.", "<user>");
    options.optopt("r", "root", "specifies the directory to act as the root directory for the file server.  If this option is specified, chroot(2) is called.  The default is not change root directories, that is, the \"/\" directory is shared out.  This will likely be a security issue, so this option should always be used.", "<directory>");
    options.optflag("h", "help", "prints this usage information.");
    options.optflag("d", "daemon", "instructs filed to become a daemon after initializing the listening TCP socket and log files.");
    options.optopt("l", "log", &format!("specifies a filename to open for writing log entries.  Log entries are made for various stages in transfering files. The log file is opened before switching users (see \"-u\") and root directories (see \"-r\").  The log file is never closed so log rotation without stopping the daemon is will not work.  The value of \"-\" indicates that standard output should be used for logging.  If the filename begins with a pipe (\"|\") then a process is started and used for logging instead of a file.  The default is \"{}\".", LOG_FILE), "<file>");
    options.optflag(
        "v",
        "version",
        "instructs filed print out the version number and exit.",
    );
    options.optflag(
        "V",
        "vhost",
        "instructs filed to prepend all requests with their HTTP Host header.",
    );

    match options.parse(&args[1..]) {
        Ok(matches) => {
            println!("{}", options.usage(USAGE_PREFIX));

            if let Ok(Some(p)) = matches.opt_get("p") {
                port = p;
            }

            if let Ok(Some(t)) = matches.opt_get("t") {
                thread_count = t;
            }

            if let Ok(Some(c)) = matches.opt_get("c") {
                cache_size = c;
            }

            if let Some(b) = matches.opt_str("b") {
                bind_addr = b;
            }

            if let Some(u) = matches.opt_str("u") {
                setuid_enabled = true;
                match filed_user_lookup(&u) {
                    Ok(uid) => user = uid,
                    Err(e) => {
                        eprintln!(
                            "Invalid username specified {}\n\n{}",
                            e,
                            options.usage(USAGE_PREFIX)
                        );
                        std::process::exit(1);
                    }
                };
            }

            if let Some(r) = matches.opt_str("r") {
                newroot = Some(r);
            }

            if let Some(l) = matches.opt_str("l") {
                log_file = l;
            }

            daemon_enabled = matches.opt_present("d");

            thread_options.vhosts_enabled = matches.opt_present("V");

            if matches.opt_present("v") {
                println!("filed version {}", FILED_VERSION);
                std::process::exit(0);
            }

            if matches.opt_present("h") {
                println!("{}", options.usage(USAGE_PREFIX));
                std::process::exit(0);
            }
        }
        Err(e) => {
            eprintln!("{}\n\n{}", e, options.usage(USAGE_PREFIX));
            std::process::exit(1);
        }
    };

    /* Open log file */
    match filed_log_open(&log_file) {
        Ok(f) => log_fp = f,
        Err(e) => {
            eprintln!("filed_log_open {}", e);
            std::process::exit(4);
        }
    }

    /* Create listening socket */
    match filed_listen(&bind_addr, port) {
        Ok(f) => listener = f,
        Err(e) => {
            eprintln!("filed_listen {}", e);
            std::process::exit(1);
        }
    }

    /* Initialize timeout structures */
    match filed_sockettimeout_init() {
        Err(e) => {
            eprintln!("filed_sockettimeout_init {}", e);
            std::process::exit(8);
        }
        _ => {}
    }

    /* Become a daemon */
    if daemon_enabled {
        match filed_daemonize() {
            Err(e) => {
                eprintln!("filed_daemonize {}", e);
                std::process::exit(6);
            }

            _ => {}
        }
    }

    /* Chroot, if appropriate */
    if let Some(newroot) = newroot {
        match unistd::chdir(std::path::Path::new(&newroot)) {
            Err(e) => {
                eprintln!("chdir {}", e);
                std::process::exit(1);
            }

            _ => {}
        };

        match unistd::chroot(".") {
            Err(e) => {
                eprintln!("chroot {}", e);
                std::process::exit(1);
            }

            _ => {}
        }
    }

    /* Drop privileges, if appropriate */
    if setuid_enabled {
        match unistd::setuid(user) {
            Err(e) => {
                eprintln!("setuid {}", e);
                std::process::exit(1);
            }

            _ => {}
        }
    }

    /* Initialize */
    match filed_init(cache_size) {
        Err(e) => {
            eprintln!("filed_init {}", e);
            std::process::exit(3);
        }

        _ => {}
    }

    // /* Create logging thread */
    // init_ret = filed_logging_thread_init(log_fp);
    // if (init_ret != 0) {
    // 	perror("filed_logging_thread_init");

    // 	return(4);
    // }

    // /* Create socket termination thread */
    // init_ret = filed_sockettimeout_thread_init();
    // if (init_ret != 0) {
    // 	perror("filed_sockettimeout_thread_init");

    // 	return(7);
    // }

    // /* Create worker threads */
    // init_ret = filed_worker_threads_init(fd, thread_count, &thread_options);
    // if (init_ret != 0) {
    // 	perror("filed_worker_threads_init");

    // 	return(5);
    // }

    // /* Wait for threads to exit */
    // /* XXX:TODO: Monitor thread usage */
    // while (1) {
    // 	sleep(86400);
    // }

    /* Return in failure */
    std::process::exit(2)
}
