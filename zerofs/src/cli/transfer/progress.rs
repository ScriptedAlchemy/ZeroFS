use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

#[derive(Clone)]
pub(super) struct Progress {
    multi: MultiProgress,
    bar: ProgressBar,
    direction: &'static str,
    state: Arc<Mutex<ProgressState>>,
    total_files: usize,
}

#[derive(Default)]
struct ProgressState {
    completed_files: usize,
    path_order: HashMap<PathBuf, usize>,
    next_dynamic_order: usize,
    next_emit_order: usize,
    pending_events: BTreeMap<usize, Vec<String>>,
    terminal_orders: BTreeSet<usize>,
    #[cfg(test)]
    emitted_lines: Vec<String>,
}

impl Progress {
    #[cfg(test)]
    pub(super) fn new(direction: &'static str, total_bytes: u64, total_files: usize) -> Self {
        Self::with_multi_ordered(
            MultiProgress::new(),
            direction,
            total_bytes,
            total_files,
            &[],
        )
    }

    pub(super) fn new_ordered(
        direction: &'static str,
        total_bytes: u64,
        paths: &[PathBuf],
    ) -> Self {
        Self::with_multi_ordered(
            MultiProgress::new(),
            direction,
            total_bytes,
            paths.len(),
            paths,
        )
    }

    #[cfg(test)]
    fn with_multi(
        multi: MultiProgress,
        direction: &'static str,
        total_bytes: u64,
        total_files: usize,
    ) -> Self {
        Self::with_multi_ordered(multi, direction, total_bytes, total_files, &[])
    }

    fn with_multi_ordered(
        multi: MultiProgress,
        direction: &'static str,
        total_bytes: u64,
        total_files: usize,
        paths: &[PathBuf],
    ) -> Self {
        let bar = ProgressBar::with_draw_target(Some(total_bytes), ProgressDrawTarget::hidden());
        bar.set_style(
            ProgressStyle::with_template(
                "{prefix} {bar:16.cyan/blue} {percent:>3}% \
                 {binary_bytes}/{binary_total_bytes} {binary_bytes_per_sec} {eta} {msg}",
            )
            .expect("static progress template is valid")
            .progress_chars("=>-"),
        );
        bar.set_prefix(direction);
        bar.set_message(format!("files 0/{total_files}"));
        let bar = multi.add(bar);
        let path_order = paths
            .iter()
            .cloned()
            .enumerate()
            .map(|(order, path)| (path, order))
            .collect();
        Self {
            multi,
            bar,
            direction,
            state: Arc::new(Mutex::new(ProgressState {
                path_order,
                next_dynamic_order: paths.len(),
                ..ProgressState::default()
            })),
            total_files,
        }
    }

    pub(super) fn start_file(&self, path: &Path, size: u64) -> FileProgress {
        self.order_for(path);
        self.bar.tick();
        let bar = ProgressBar::with_draw_target(Some(size), ProgressDrawTarget::hidden());
        bar.set_style(
            ProgressStyle::with_template(
                "  {msg:16!} {bar:16.green/blue} {percent:>3}% \
                 {binary_bytes}/{binary_total_bytes} {binary_bytes_per_sec} {eta}",
            )
            .expect("static file progress template is valid")
            .progress_chars("=>-"),
        );
        bar.set_message(path.display().to_string());
        FileProgress {
            progress: self.clone(),
            bar: self.multi.add(bar),
            path: path.to_path_buf(),
        }
    }

    pub(super) fn retry_file(
        &self,
        path: &Path,
        next_attempt: usize,
        max_attempts: usize,
        error: &anyhow::Error,
    ) {
        self.queue_file_event(
            path,
            format!(
                "{} retry {next_attempt}/{max_attempts}: {}: {error:#}",
                self.direction,
                path.display()
            ),
            false,
        );
    }

    pub(super) fn fail_file(&self, path: &Path, attempts: usize, error: &anyhow::Error) {
        self.queue_file_event(
            path,
            format!(
                "{} failed after {attempts} attempt{}: {}: {error:#}",
                self.direction,
                if attempts == 1 { "" } else { "s" },
                path.display()
            ),
            true,
        );
    }

    pub(super) fn skip_file(&self, path: &Path, size: u64) {
        self.bar.inc(size);
        self.bar.reset_eta();
        self.record_file(path, "skipped");
    }

    fn finish_file(&self, path: &Path, bytes: u64) {
        self.bar.inc(bytes);
        self.bar.reset_eta();
        self.record_file(path, "complete");
    }

    fn record_file(&self, path: &Path, status: &str) {
        let order = {
            let mut state = self.state.lock().unwrap();
            state.completed_files += 1;
            let completed = state.completed_files;
            let order = Self::order_for_locked(&mut state, path);
            self.set_file_message(completed);
            order
        };
        if self.bar.is_hidden() {
            self.queue_event_at(
                order,
                format!(
                    "{} file {}/{} {status}: {}",
                    self.direction,
                    order + 1,
                    self.total_files,
                    path.display()
                ),
                true,
            );
        }
    }

    /// Restates what cancelled work is still waiting on. A 9P reply wait has no
    /// aggregate deadline while its connection stays provably live, so a wedged
    /// server operation would otherwise make the first Ctrl-C look like a hang.
    pub(super) fn settling(&self, waited: Duration) {
        self.print_line(format!(
            "{} cancelled: in-flight operations still settling after {}s; \
             press Ctrl-C again to exit immediately",
            self.direction,
            waited.as_secs()
        ));
    }

    pub(super) fn syncing_directories(&self) {
        self.bar.set_message("syncing directories");
    }

    pub(super) fn finish(&self) {
        self.bar.finish_with_message(format!(
            "files {}/{} complete",
            self.state.lock().unwrap().completed_files,
            self.total_files
        ));
        if self.bar.is_hidden() {
            eprintln!(
                "{} complete: {} files, {} bytes",
                self.direction,
                self.total_files,
                self.bar.position()
            );
        }
    }

    fn set_file_message(&self, completed: usize) {
        self.bar
            .set_message(format!("files {completed}/{}", self.total_files));
    }

    fn print_line(&self, message: String) {
        if self.bar.is_hidden() {
            #[cfg(test)]
            self.state
                .lock()
                .unwrap()
                .emitted_lines
                .push(message.clone());
            eprintln!("{message}");
        } else {
            let _ = self.multi.println(message);
        }
    }

    fn queue_file_event(&self, path: &Path, message: String, terminal: bool) {
        let order = self.order_for(path);
        self.queue_event_at(order, message, terminal);
    }

    fn queue_event_at(&self, order: usize, message: String, terminal: bool) {
        if !self.bar.is_hidden() {
            let _ = self.multi.println(message);
            return;
        }
        let ready = {
            let mut state = self.state.lock().unwrap();
            state.pending_events.entry(order).or_default().push(message);
            if terminal {
                state.terminal_orders.insert(order);
            }
            let mut ready = Vec::new();
            loop {
                let next = state.next_emit_order;
                if !state.terminal_orders.remove(&next) {
                    break;
                }
                if let Some(events) = state.pending_events.remove(&next) {
                    ready.extend(events);
                }
                state.next_emit_order += 1;
            }
            #[cfg(test)]
            state.emitted_lines.extend(ready.iter().cloned());
            ready
        };
        for line in ready {
            eprintln!("{line}");
        }
    }

    fn order_for(&self, path: &Path) -> usize {
        Self::order_for_locked(&mut self.state.lock().unwrap(), path)
    }

    fn order_for_locked(state: &mut ProgressState, path: &Path) -> usize {
        if let Some(order) = state.path_order.get(path) {
            return *order;
        }
        let order = state.next_dynamic_order;
        state.next_dynamic_order += 1;
        state.path_order.insert(path.to_path_buf(), order);
        order
    }

    #[cfg(test)]
    pub(super) fn transferred_bytes(&self) -> u64 {
        self.bar.position()
    }

    #[cfg(test)]
    pub(super) fn emitted_lines(&self) -> Vec<String> {
        self.state.lock().unwrap().emitted_lines.clone()
    }
}

pub(super) struct FileProgress {
    progress: Progress,
    bar: ProgressBar,
    path: PathBuf,
}

impl FileProgress {
    pub(super) fn advance(&self, bytes: u64) {
        self.bar.inc(bytes);
    }

    pub(super) fn finish(self) {
        let bytes = self.bar.position();
        self.progress.multi.remove(&self.bar);
        self.progress.finish_file(&self.path, bytes);
    }
}

impl Drop for FileProgress {
    fn drop(&mut self) {
        self.progress.multi.remove(&self.bar);
    }
}

#[derive(Clone)]
pub(super) struct DeleteProgress {
    bar: ProgressBar,
    removed: Arc<Mutex<usize>>,
}

impl DeleteProgress {
    pub(super) fn new() -> Self {
        let bar = ProgressBar::new_spinner();
        bar.set_style(
            ProgressStyle::with_template("{prefix} {spinner} {msg}")
                .expect("static delete progress template is valid"),
        );
        bar.set_prefix("delete");
        Self {
            bar,
            removed: Arc::new(Mutex::new(0)),
        }
    }

    pub(super) fn start(&self, path: &Path) {
        self.bar.set_message(format!("removing {}", path.display()));
    }

    pub(super) fn removed(&self, path: &Path) {
        let mut removed = self.removed.lock().unwrap();
        *removed += 1;
        self.bar
            .set_message(format!("{} removed: {}", *removed, path.display()));
    }

    pub(super) fn finish(&self, path: &Path) {
        let removed = *self.removed.lock().unwrap();
        let message = format!("{removed} removed; complete: {}", path.display());
        self.bar.finish_with_message(message.clone());
        if self.bar.is_hidden() {
            eprintln!("delete complete: {message}");
        }
    }

    /// The removal counterpart of [`Progress::settling`].
    pub(super) fn settling(&self, waited: Duration) {
        let message = format!(
            "delete cancelled: in-flight operations still settling after {}s; \
             press Ctrl-C again to exit immediately",
            waited.as_secs()
        );
        if self.bar.is_hidden() {
            eprintln!("{message}");
        } else {
            self.bar.println(message);
        }
    }

    pub(super) fn abandon(&self) {
        self.bar.abandon_with_message("delete stopped");
    }

    #[cfg(test)]
    pub(super) fn removed_entries(&self) -> usize {
        *self.removed.lock().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::Progress;
    use indicatif::{InMemoryTerm, MultiProgress, ProgressDrawTarget};
    use std::path::Path;

    #[test]
    fn progress_tracks_real_byte_and_file_totals() {
        let progress = Progress::new("upload", 1024, 4);
        let file = progress.start_file(Path::new("book.m4b"), 1024);
        assert_eq!(progress.bar.message(), "files 0/4");
        file.advance(512);
        file.finish();

        assert_eq!(progress.bar.position(), 512);
        assert_eq!(progress.bar.length(), Some(1024));
        assert_eq!(progress.bar.prefix(), "upload");
        assert_eq!(progress.bar.message(), "files 1/4");
    }

    #[test]
    fn construction_does_not_leave_a_stale_zero_byte_row() {
        let terminal = InMemoryTerm::new(10, 80);
        let multi = MultiProgress::with_draw_target(ProgressDrawTarget::term_like(Box::new(
            terminal.clone(),
        )));

        let _progress = Progress::with_multi(multi, "upload", 1024, 1);

        assert_eq!(terminal.contents(), "");
    }

    #[test]
    fn concurrent_files_have_independent_progress_bars() {
        let progress = Progress::new("upload", 3072, 2);
        let first = progress.start_file(Path::new("first.m4b"), 1024);
        let second = progress.start_file(Path::new("second.m4b"), 2048);

        first.advance(256);
        second.advance(512);

        assert_eq!(first.bar.position(), 256);
        assert_eq!(first.bar.length(), Some(1024));
        assert_eq!(first.bar.message(), "first.m4b");
        assert_eq!(second.bar.position(), 512);
        assert_eq!(second.bar.length(), Some(2048));
        assert_eq!(second.bar.message(), "second.m4b");
        assert_eq!(progress.bar.position(), 0);
    }

    #[test]
    fn failed_file_attempt_does_not_inflate_aggregate_progress() {
        let progress = Progress::new("upload", 1024, 1);
        let file = progress.start_file(Path::new("book.m4b"), 1024);
        file.advance(512);
        drop(file);

        assert_eq!(progress.bar.position(), 0);
        assert_eq!(progress.bar.message(), "files 0/1");
    }

    #[test]
    fn failed_file_attempt_does_not_inflate_upload_rate() {
        let progress = Progress::new("upload", 1024, 1);
        let file = progress.start_file(Path::new("book.m4b"), 1024);
        file.advance(512);
        drop(file);

        assert_eq!(progress.bar.per_sec(), 0.0);
    }

    #[test]
    fn skipped_bytes_do_not_inflate_upload_rate() {
        let progress = Progress::new("upload", 1024, 1);
        progress.skip_file(Path::new("book.m4b"), 1024);

        assert_eq!(progress.bar.position(), 1024);
        assert_eq!(progress.bar.per_sec(), 0.0);
    }

    #[test]
    fn completed_file_bar_is_removed_from_terminal() {
        let terminal = InMemoryTerm::new(10, 200);
        let progress = Progress::new("upload", 1024, 2);
        progress
            .multi
            .set_draw_target(ProgressDrawTarget::term_like(Box::new(terminal.clone())));

        let file = progress.start_file(Path::new("first.m4b"), 1024);
        file.advance(1024);
        file.finish();

        let rendered = terminal.contents();
        assert_eq!(
            rendered.lines().count(),
            1,
            "rendered terminal:\n{rendered}"
        );
        assert!(rendered.contains("files 1/2"));
    }

    #[test]
    fn narrow_terminal_keeps_each_progress_bar_on_one_line() {
        let terminal = InMemoryTerm::new(10, 80);
        let progress = Progress::new("upload", 3072, 3);
        progress
            .multi
            .set_draw_target(ProgressDrawTarget::term_like(Box::new(terminal.clone())));

        let files = [
            progress.start_file(Path::new("first.m4b"), 1024),
            progress.start_file(Path::new("second.m4b"), 1024),
            progress.start_file(Path::new("third.m4b"), 1024),
        ];
        for file in &files {
            file.advance(1);
        }

        let rendered = terminal.contents();
        assert_eq!(
            rendered.lines().count(),
            4,
            "rendered terminal:\n{rendered}"
        );
    }

    #[test]
    fn narrow_terminal_keeps_large_transfer_rows_on_one_line() {
        const GIB: u64 = 1024 * 1024 * 1024;
        const MIB: u64 = 1024 * 1024;

        let terminal = InMemoryTerm::new(24, 80);
        let progress = Progress::new("upload", 523 * GIB, 1121);
        progress
            .multi
            .set_draw_target(ProgressDrawTarget::term_like(Box::new(terminal.clone())));

        let file = progress.start_file(
            Path::new("Miss Peregrine's Home for Peculiar Children.m4b"),
            265 * MIB,
        );
        file.advance(135 * MIB);

        let rendered = terminal.contents();
        assert_eq!(
            rendered.lines().count(),
            2,
            "rendered terminal:\n{rendered}"
        );
        assert!(
            rendered.lines().all(|line| line.chars().count() < 80),
            "a full-width row can soft-wrap and corrupt redraws:\n{rendered}"
        );
    }

    #[test]
    fn concurrent_file_completion_does_not_regress_the_count() {
        for _ in 0..100 {
            let progress = Progress::new("upload", 2, 2);
            progress
                .multi
                .set_draw_target(ProgressDrawTarget::term_like(Box::new(InMemoryTerm::new(
                    4, 80,
                ))));
            let first = progress.start_file(Path::new("first.m4b"), 1);
            let second = progress.start_file(Path::new("second.m4b"), 1);

            std::thread::scope(|scope| {
                scope.spawn(move || first.finish());
                scope.spawn(move || second.finish());
            });

            assert_eq!(progress.bar.message(), "files 2/2");
        }
    }

    #[test]
    fn non_tty_file_events_are_emitted_in_planned_order() {
        let progress = Progress::new_ordered(
            "upload",
            2,
            &[
                Path::new("first.m4b").into(),
                Path::new("second.m4b").into(),
            ],
        );
        let first = progress.start_file(Path::new("first.m4b"), 1);
        let second = progress.start_file(Path::new("second.m4b"), 1);

        second.advance(1);
        second.finish();
        assert!(progress.emitted_lines().is_empty());

        first.advance(1);
        first.finish();
        assert_eq!(
            progress.emitted_lines(),
            [
                "upload file 1/2 complete: first.m4b",
                "upload file 2/2 complete: second.m4b"
            ]
        );
    }
}
