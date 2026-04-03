/* use crate::type_conversion::{
    ArchivedDictView, ArchivedOwnedValue, OwnedValue, access_archived as access_archived_value, archive_value,
    archived_to_py_bound,
}; */
use pyo3::{prelude::*, types::PyBytes};
use std::sync::OnceLock;

use crate::conversion::{OwnedValue, access_archived as access_archived_value, archive_value, wrap_archived};
mod conversion;
pub fn version() -> &'static str {
    static VERSION: OnceLock<String> = OnceLock::new();
    // Mapping Cargo versioning (e.g., "1.0-alpha1") to Python's PEP 440 format (e.g., "1.0.0a1")
    // This conversion is a simplified compatibility adjustment and covers most common cases.
    VERSION.get_or_init(|| {
        let version = env!("CARGO_PKG_VERSION");
        version.replace("-alpha", "a").replace("-beta", "b")
    })
}

// https://github.com/davidhewitt/pythonize/blob/main/src/de.rs#L498



#[pyfunction]
fn archive(py: Python<'_>, value: OwnedValue) -> PyResult<Py<PyBytes>> {
    let bytes = archive_value(&value).map_err(pyo3::exceptions::PyValueError::new_err)?;
    Ok(PyBytes::new(py, bytes.as_slice()).unbind())
}

#[pyfunction]
fn access_archived(py: Python<'_>, bytes: &[u8]) -> PyResult<Py<PyAny>> {
    let owned = PyBytes::new(py, bytes);
    let root = access_archived_value(owned.as_bytes()).map_err(pyo3::exceptions::PyValueError::new_err)?;

    wrap_archived(py, &owned, root).map(|v| v.unbind())
}

#[pymodule]
fn pyrkyv(_py: Python, module: &Bound<PyModule>) -> PyResult<()> {
    module.add("__version__", version())?;
    module.add_function(wrap_pyfunction!(archive, module)?)?;
    module.add_function(wrap_pyfunction!(access_archived, module)?)?;
    Ok(())
}
