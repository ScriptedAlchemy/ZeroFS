//! Failures after which `zerofs run` must not be restarted until a person
//! decides what to do.
//!
//! A supervisor that restarts the server on failure must tell two kinds of
//! stop apart. An ordinary failure (a port in use, an unreachable backend, a
//! config file that does not parse) exits with status 1, and a restart may
//! help. A refusal because acknowledged state may be lost, storage is damaged,
//! or the storage found is not the storage the volume was created on exits
//! with [`OPERATOR_REQUIRED_EXIT_STATUS`]: restarting refuses again, and
//! proceeding could lose or misattribute data.
//!
//! The code that detects such a condition marks its error with
//! [`operator_required`]. The marker keeps the error's message and cause chain
//! as they were, so the output does not change. [`ServerExit::of`] is the one
//! place that turns a marked error into the exit status.

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::process::ExitCode;

/// Exit status of `zerofs run` when it stops because a person must decide
/// what happens next. Supervisors such as the TrueNAS app package match this
/// value to stop restarting the server, so it must never change.
///
/// Each sysexits.h status names one cause (78 EX_CONFIG a configuration
/// error, 74 EX_IOERR an I/O error), and this status covers several causes:
/// a damaged journal, a failing metadata disk, a catalog or key that does not
/// belong to this volume. 10 is outside the sysexits range (64 to 78), the
/// shell's statuses (126 to 128 and 128 plus a signal number), clap's usage
/// status 2, and the statuses other ZeroFS subcommands return (1, 3 and 4).
pub const OPERATOR_REQUIRED_EXIT_STATUS: u8 = 10;

/// The last line `zerofs run` writes to stderr before it exits with
/// [`OPERATOR_REQUIRED_EXIT_STATUS`].
pub const OPERATOR_REQUIRED_NOTICE: &str =
    "zerofs stopped: this failure needs an operator decision, and restarting will refuse again";

/// The exit statuses `zerofs run --help` describes.
pub(crate) fn run_exit_status_help() -> String {
    format!(
        "Exit status: 0 after a clean shutdown, 1 after an ordinary failure (a restart may \
         help), and {OPERATOR_REQUIRED_EXIT_STATUS} when the server refuses to serve until an \
         operator decides (a damaged journal, a failing metadata disk, a missing or mismatched \
         pool catalog, member marker, device or key). The README section \"Exit status\" lists \
         every case."
    )
}

/// Marks a failure that needs an operator. It displays as the error it wraps
/// and reports that error's causes as its own, so marking changes no output.
#[derive(Debug)]
pub(crate) struct OperatorRequired(Box<dyn Error + Send + Sync + 'static>);

impl OperatorRequired {
    pub(crate) fn new(cause: impl Into<Box<dyn Error + Send + Sync + 'static>>) -> Self {
        Self(cause.into())
    }
}

impl Display for OperatorRequired {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl Error for OperatorRequired {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.0.source()
    }
}

/// Mark `error` as a failure that needs an operator.
pub(crate) fn operator_required(error: impl Into<anyhow::Error>) -> anyhow::Error {
    anyhow::Error::new(OperatorRequired::new(error.into()))
}

/// True when `error` or one of its causes is marked [`OperatorRequired`].
pub(crate) fn requires_operator(error: &(dyn Error + 'static)) -> bool {
    std::iter::successors(Some(error), |&cause| cause.source())
        .any(|cause| cause.is::<OperatorRequired>())
}

/// How `zerofs run` ends after an error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServerExit {
    /// Something failed, and a restart may help.
    Failed,
    /// The server refuses to serve until a person decides.
    OperatorRequired,
}

impl ServerExit {
    pub fn of(error: &(dyn Error + 'static)) -> Self {
        if requires_operator(error) {
            Self::OperatorRequired
        } else {
            Self::Failed
        }
    }

    pub fn status(self) -> u8 {
        match self {
            Self::Failed => 1,
            Self::OperatorRequired => OPERATOR_REQUIRED_EXIT_STATUS,
        }
    }

    /// Write the closing line for this class, if it has one. It follows the
    /// error and its causes, so it is the last line on stderr.
    fn announce(self) {
        if self == Self::OperatorRequired {
            eprintln!("{OPERATOR_REQUIRED_NOTICE}");
        }
    }

    /// Exit a serving process at once after `error`, which the caller has
    /// already logged. For failures of `zerofs run` that cannot unwind to
    /// `main`.
    pub(crate) fn exit_now(error: &(dyn Error + 'static)) -> ! {
        let exit = Self::of(error);
        exit.announce();
        std::process::exit(i32::from(exit.status()))
    }
}

/// A failed command and how the process ends because of it. Only `zerofs
/// run` classifies its error; every other command fails with status 1, as it
/// did before the operator-required status existed.
#[derive(Debug)]
pub struct CliFailure {
    error: anyhow::Error,
    exit: ServerExit,
}

impl CliFailure {
    /// A failure of `zerofs run`, classified by its cause chain.
    pub(crate) fn server(error: anyhow::Error) -> Self {
        let exit = ServerExit::of(error.as_ref());
        Self { error, exit }
    }

    /// End `main`: print the error as a `Result`-returning `main` does, then
    /// the class's closing line, and return the class's status.
    pub fn report(self) -> ExitCode {
        eprintln!("Error: {:?}", self.error);
        self.exit.announce();
        ExitCode::from(self.exit.status())
    }
}

impl From<anyhow::Error> for CliFailure {
    fn from(error: anyhow::Error) -> Self {
        Self {
            error,
            exit: ServerExit::Failed,
        }
    }
}

/// Runs one test in a child copy of the test binary, so that a test can
/// observe a path that ends the process.
#[cfg(test)]
pub(crate) mod exit_probe {
    use super::{OPERATOR_REQUIRED_NOTICE, ServerExit};
    use std::process::{Command, Output};

    const CHILD: &str = "ZEROFS_EXIT_PROBE";

    /// True inside the child, where the test should take the exiting path.
    pub(crate) fn is_child() -> bool {
        std::env::var_os(CHILD).is_some()
    }

    /// Run `test`, a full test path, alone in a child.
    pub(crate) fn run(test: &str) -> Output {
        Command::new(std::env::current_exe().expect("test binary path"))
            .args(["--exact", test, "--nocapture", "--test-threads=1"])
            .env(CHILD, "1")
            .output()
            .expect("start the test binary")
    }

    /// The child exited the way `exit` ends a process.
    pub(crate) fn assert_exited_as(child: &Output, exit: ServerExit) {
        let stderr = String::from_utf8_lossy(&child.stderr);
        assert_eq!(
            child.status.code(),
            Some(i32::from(exit.status())),
            "stderr:\n{stderr}"
        );
        let last = stderr.trim_end().lines().last().unwrap_or("");
        assert_eq!(
            last == OPERATOR_REQUIRED_NOTICE,
            exit == ServerExit::OperatorRequired,
            "stderr:\n{stderr}"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Supervisors, including the TrueNAS app package, match this number to
    /// stop restarting the server. Changing it breaks them silently.
    #[test]
    fn operator_required_exit_status_is_ten() {
        assert_eq!(OPERATOR_REQUIRED_EXIT_STATUS, 10);
        assert_eq!(ServerExit::OperatorRequired.status(), 10);
        assert_eq!(ServerExit::Failed.status(), 1);
    }

    #[test]
    fn marking_changes_neither_the_message_nor_the_causes() {
        let plain = || {
            anyhow::anyhow!("root cause")
                .context("middle")
                .context("top")
        };
        let causes =
            |error: &anyhow::Error| error.chain().map(ToString::to_string).collect::<Vec<_>>();
        let marked = operator_required(plain());
        assert_eq!(format!("{marked}"), format!("{}", plain()));
        assert_eq!(format!("{marked:#}"), format!("{:#}", plain()));
        assert_eq!(causes(&marked), causes(&plain()));
    }

    #[test]
    fn a_marked_cause_classifies_the_whole_chain() {
        let error = operator_required(anyhow::anyhow!("journal is damaged"))
            .context("Failed to attach persistent writeback");
        assert_eq!(ServerExit::of(error.as_ref()), ServerExit::OperatorRequired);
        assert_eq!(
            ServerExit::of(anyhow::anyhow!("Address already in use").as_ref()),
            ServerExit::Failed
        );
    }
}
