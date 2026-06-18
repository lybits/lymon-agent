// In-memory log ring buffer for remote retrieval (get_logs over the control
// channel). A tracing fmt layer writes here in addition to stdout, so the
// buffer holds exactly the formatted lines the agent emits — its own logs AND
// piped plugin stderr (logged through tracing by the plugin host). Bounded;
// oldest lines drop. No disk, no journal access — portable Linux/Windows.

use std::collections::VecDeque;
use std::io::{self, Write};
use std::sync::{Mutex, OnceLock};

/// Max lines retained. The control-plane caps requests below this anyway.
const CAP: usize = 500;

fn buf() -> &'static Mutex<VecDeque<String>> {
    static BUF: OnceLock<Mutex<VecDeque<String>>> = OnceLock::new();
    BUF.get_or_init(|| Mutex::new(VecDeque::with_capacity(CAP)))
}

fn push_line(line: &str) {
    let line = line.trim_end_matches(['\n', '\r']);
    if line.is_empty() {
        return;
    }
    if let Ok(mut b) = buf().lock() {
        while b.len() >= CAP {
            b.pop_front();
        }
        b.push_back(line.to_string());
    }
}

/// The last `n` lines (capped to what's retained), oldest-first.
pub fn tail(n: usize) -> Vec<String> {
    let Ok(b) = buf().lock() else {
        return Vec::new();
    };
    let take = n.min(b.len());
    b.iter().skip(b.len() - take).cloned().collect()
}

/// A `std::io::Write` that splits the bytes a tracing fmt layer hands it into
/// whole lines and pushes each into the ring buffer.
#[derive(Default)]
pub struct LineWriter {
    partial: Vec<u8>,
}

impl Write for LineWriter {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.partial.extend_from_slice(data);
        while let Some(pos) = self.partial.iter().position(|&b| b == b'\n') {
            let line: Vec<u8> = self.partial.drain(..=pos).collect();
            if let Ok(s) = std::str::from_utf8(&line) {
                push_line(s);
            }
        }
        Ok(data.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for LineWriter {
    fn drop(&mut self) {
        // Flush a trailing line with no newline (fmt events end in '\n', so
        // this is just belt-and-braces).
        if !self.partial.is_empty() {
            if let Ok(s) = std::str::from_utf8(&self.partial) {
                push_line(s);
            }
        }
    }
}

/// `MakeWriter` for the buffer fmt layer — a fresh `LineWriter` per event.
pub struct MakeBufWriter;

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for MakeBufWriter {
    type Writer = LineWriter;
    fn make_writer(&'a self) -> Self::Writer {
        LineWriter::default()
    }
}
