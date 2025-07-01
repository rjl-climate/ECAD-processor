use indicatif::{ProgressBar, ProgressStyle};
use std::time::Duration;

pub struct ProgressReporter {
    progress_bar: Option<ProgressBar>,
    silent: bool,
}

impl ProgressReporter {
    pub fn new(total: u64, message: &str, silent: bool) -> Self {
        if silent {
            Self {
                progress_bar: None,
                silent: true,
            }
        } else {
            let pb = ProgressBar::new(total);
            pb.set_style(
                ProgressStyle::default_bar()
                    .template("{msg}\n{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
                    .unwrap()
                    .progress_chars("#>-"),
            );
            pb.set_message(message.to_string());
            pb.enable_steady_tick(Duration::from_millis(100));

            Self {
                progress_bar: Some(pb),
                silent: false,
            }
        }
    }

    pub fn new_spinner(message: &str, silent: bool) -> Self {
        if silent {
            Self {
                progress_bar: None,
                silent: true,
            }
        } else {
            let pb = ProgressBar::new_spinner();
            pb.set_style(
                ProgressStyle::default_spinner()
                    .template("{spinner:.green} {msg}")
                    .unwrap(),
            );
            pb.set_message(message.to_string());
            pb.enable_steady_tick(Duration::from_millis(100));

            Self {
                progress_bar: Some(pb),
                silent: false,
            }
        }
    }

    pub fn update(&self, current: u64) {
        if let Some(ref pb) = self.progress_bar {
            pb.set_position(current);
        }
    }

    pub fn increment(&self, delta: u64) {
        if let Some(ref pb) = self.progress_bar {
            pb.inc(delta);
        }
    }

    pub fn set_message(&self, message: &str) {
        if let Some(ref pb) = self.progress_bar {
            pb.set_message(message.to_string());
        }
    }

    pub fn finish_with_message(&self, message: &str) {
        if let Some(ref pb) = self.progress_bar {
            pb.finish_with_message(message.to_string());
        }
    }

    pub fn finish(&self) {
        if let Some(ref pb) = self.progress_bar {
            pb.finish();
        }
    }

    pub fn println(&self, message: &str) {
        if !self.silent {
            if let Some(ref pb) = self.progress_bar {
                pb.println(message);
            } else {
                println!("{}", message);
            }
        }
    }
}

impl Drop for ProgressReporter {
    fn drop(&mut self) {
        if let Some(ref pb) = self.progress_bar {
            pb.finish();
        }
    }
}

/// Create a multi-progress reporter for concurrent operations
pub struct MultiProgressReporter {
    bars: Vec<ProgressBar>,
    silent: bool,
}

impl MultiProgressReporter {
    pub fn new(tasks: Vec<(&str, u64)>, silent: bool) -> Self {
        if silent {
            Self {
                bars: vec![],
                silent: true,
            }
        } else {
            let bars: Vec<ProgressBar> = tasks
                .into_iter()
                .map(|(name, total)| {
                    let pb = ProgressBar::new(total);
                    pb.set_style(
                        ProgressStyle::default_bar()
                            .template("{prefix:>12} [{bar:40.cyan/blue}] {pos}/{len}")
                            .unwrap()
                            .progress_chars("#>-"),
                    );
                    pb.set_prefix(name.to_string());
                    pb
                })
                .collect();

            Self {
                bars,
                silent: false,
            }
        }
    }

    pub fn get_bar(&self, index: usize) -> Option<&ProgressBar> {
        if self.silent {
            None
        } else {
            self.bars.get(index)
        }
    }

    pub fn finish_all(&self) {
        for bar in &self.bars {
            bar.finish();
        }
    }
}
