//! This module contains the Python and PyO3 code to be able to use trafilatura
//! from Rust.
use once_cell::sync::Lazy;
use pyo3::{
    types::{PyAnyMethods, PyModule},
    Py, PyAny, PyObject, Python,
};
use tokio::sync::{mpsc, oneshot};
use std::thread;

static PYTHON_SCRIPT: &str = r"
from typing import Optional
from trafilatura import extract

def extract_text(content: str) -> Optional[str]:
    text = extract(content, include_comments=False,
                   include_tables=False, deduplicate=True)
    # also return None if utf-8 decoding failed
    if text is None or isinstance(text, bytes):
        return None
    return text
";

static PYTHON_EXTRACT_FUNCTION: Lazy<Py<PyAny>> = Lazy::new(|| {
    Python::with_gil(move |py| -> PyObject {
        tracing::info!(
            "Loading Python trafilatura with version {:?}.",
            py.version_info()
        );
        let module = PyModule::from_code_bound(py, PYTHON_SCRIPT, "extraction.py", "extraction")
            .expect("Failed to load Python module");
        let extract_function = module
            .getattr("extract_text")
            .expect("Failed to get extract_text function");
        let extract_function = extract_function.into();
        tracing::info!("Loaded Python trafilatura.");
        extract_function
    })
});

/// Extract text from `html` and return the extracted
/// text as string if successful.
/// Might return `Ok(None)` if text extraction was not successful.
fn extract_sync(html: &str) -> Result<Option<String>, anyhow::Error> {
    Python::with_gil(move |py| -> Result<Option<String>, anyhow::Error> {
        PYTHON_EXTRACT_FUNCTION
            .call1(py, (html,))?
            .extract(py)
            .map_err(Into::into)
    })
}

pub struct PythonExtractor {
    sender: mpsc::UnboundedSender<(String, oneshot::Sender<Result<Option<String>, anyhow::Error>>)>,
}

impl PythonExtractor {
    pub fn new() -> Self {
        let (sender, mut receiver) = mpsc::unbounded_channel::<(String, oneshot::Sender<Result<Option<String>, anyhow::Error>>)>();

        // Spawn a dedicated thread for Python operations
        thread::spawn(move || {
            tracing::info!("Python worker thread started");
            while let Some((html, response_sender)) = receiver.blocking_recv() {
                tracing::debug!("Processing extraction request in Python worker thread");
                let result = extract_sync(&html);
                tracing::debug!("Extraction completed in Python worker thread");
                let _ = response_sender.send(result);
            }
            tracing::info!("Python worker thread ended");
        });

        Self { sender }
    }

    pub async fn extract(&self, html: String) -> Result<Option<String>, anyhow::Error> {
        let (response_sender, response_receiver) = oneshot::channel();

        self.sender.send((html, response_sender))
            .map_err(|_| anyhow::anyhow!("Python worker thread died"))?;

        response_receiver.await
            .map_err(|_| anyhow::anyhow!("Python worker response failed"))?
    }
}

static PYTHON_EXTRACTOR: Lazy<PythonExtractor> = Lazy::new(|| {
    PythonExtractor::new()
});

/// Async version of extract that uses a dedicated Python worker thread
pub async fn extract(html: &str) -> Result<Option<String>, anyhow::Error> {
    PYTHON_EXTRACTOR.extract(html.to_string()).await
}
