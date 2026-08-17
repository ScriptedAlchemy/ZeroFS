use indicatif::{ProgressBar, ProgressStyle};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone)]
pub(super) struct Progress {
    bar: ProgressBar,
    direction: &'static str,
    completed_files: Arc<AtomicUsize>,
    total_files: usize,
}

impl Progress {
    pub(super) fn new(direction: &'static str, total_bytes: u64, total_files: usize) -> Self {
        let bar = ProgressBar::new(total_bytes);
        bar.set_style(
            ProgressStyle::with_template(
                "{prefix} [{bar:30.cyan/blue}] {percent_precise}% \
                 {binary_bytes}/{binary_total_bytes} {binary_bytes_per_sec} ETA {eta} {msg}",
            )
            .expect("static progress template is valid")
            .progress_chars("=>-"),
        );
        bar.set_prefix(direction);
        Self {
            bar,
            direction,
            completed_files: Arc::new(AtomicUsize::new(0)),
            total_files,
        }
    }

    pub(super) fn start_file(&self, path: &Path) {
        self.set_file_message(self.completed_files.load(Ordering::Relaxed), path);
    }

    pub(super) fn advance(&self, bytes: u64) {
        self.bar.inc(bytes);
    }

    pub(super) fn finish_file(&self, path: &Path) {
        let completed = self.completed_files.fetch_add(1, Ordering::Relaxed) + 1;
        self.set_file_message(completed, path);
        if self.bar.is_hidden() {
            eprintln!(
                "{} file {completed}/{} complete: {}",
                self.direction,
                self.total_files,
                path.display()
            );
        }
    }

    pub(super) fn syncing_directories(&self) {
        self.bar.set_message("syncing directories");
    }

    pub(super) fn finish(&self) {
        self.bar.finish_with_message(format!(
            "files {}/{} complete",
            self.completed_files.load(Ordering::Relaxed),
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

    fn set_file_message(&self, completed: usize, path: &Path) {
        self.bar.set_message(format!(
            "files {completed}/{} {}",
            self.total_files,
            path.display()
        ));
    }
}

#[derive(Clone)]
pub(super) struct DeleteProgress {
    bar: ProgressBar,
}

impl DeleteProgress {
    pub(super) fn new() -> Self {
        let bar = ProgressBar::new_spinner();
        bar.set_style(
            ProgressStyle::with_template("{prefix} {spinner} {pos} entries {msg}")
                .expect("static delete progress template is valid"),
        );
        bar.set_prefix("delete");
        Self { bar }
    }

    pub(super) fn deleted(&self, path: &Path) {
        self.bar.inc(1);
        self.bar.set_message(path.display().to_string());
    }

    pub(super) fn finish(&self) {
        let deleted = self.bar.position();
        let noun = if deleted == 1 { "entry" } else { "entries" };
        self.bar
            .finish_with_message(format!("{deleted} {noun} removed"));
        if self.bar.is_hidden() {
            eprintln!("delete complete: {deleted} {noun} removed");
        }
    }

    pub(super) fn abandon(&self) {
        self.bar.abandon_with_message("delete stopped");
    }
}

#[cfg(test)]
mod tests {
    use super::Progress;
    use std::path::Path;

    #[test]
    fn progress_tracks_real_byte_and_file_totals() {
        let progress = Progress::new("upload", 1024, 4);
        progress.start_file(Path::new("book.m4b"));
        progress.advance(512);
        progress.finish_file(Path::new("book.m4b"));

        assert_eq!(progress.bar.position(), 512);
        assert_eq!(progress.bar.length(), Some(1024));
        assert_eq!(progress.bar.prefix(), "upload");
        assert_eq!(progress.bar.message(), "files 1/4 book.m4b");
    }
}
