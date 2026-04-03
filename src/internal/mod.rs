use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyDict, PyList, PyString, PyTuple};
use rkyv::{Archive, Deserialize, Serialize};

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
#[rkyv(
    compare(PartialEq),
    derive(Debug),
    serialize_bounds(
        __S: rkyv::ser::Writer + rkyv::ser::Allocator,
        __S::Error: rkyv::rancor::Source,
    ),
    deserialize_bounds(
        __D::Error: rkyv::rancor::Source
    ),
    bytecheck(bounds(
        __C: rkyv::validation::ArchiveContext,
    ))
)]
pub enum OwnedValue {
    None,
    Bool(bool),

    I64(i64),
    U64(u64),
    F64(f64),

    String(String),
    Bytes(Vec<u8>),

    List(#[rkyv(omit_bounds)] Vec<OwnedValue>),
    Tuple(#[rkyv(omit_bounds)] Vec<OwnedValue>),
    Dict(#[rkyv(omit_bounds)] Vec<(String, OwnedValue)>),

    Complex { re: f64, im: f64 },
}

impl<'a, 'py> FromPyObject<'a, 'py> for OwnedValue {
    type Error = PyErr;

    fn extract(obj: Borrowed<'a, 'py, PyAny>) -> Result<Self, Self::Error> {
        // None
        if obj.is_none() {
            return Ok(OwnedValue::None);
        }

        // Bool
        if let Ok(v) = obj.extract::<bool>() {
            return Ok(OwnedValue::Bool(v));
        }

        // i64
        if let Ok(v) = obj.extract::<i64>() {
            return Ok(OwnedValue::I64(v));
        }

        // u64
        if let Ok(v) = obj.extract::<u64>() {
            return Ok(OwnedValue::U64(v));
        }

        // float
        if let Ok(v) = obj.extract::<f64>() {
            return Ok(OwnedValue::F64(v));
        }

        // String
        if let Ok(v) = obj.cast::<PyString>() {
            return Ok(OwnedValue::String(v.to_str()?.to_string()));
        }

        // Bytes
        if let Ok(v) = obj.cast::<PyBytes>() {
            return Ok(OwnedValue::Bytes(v.as_bytes().to_vec()));
        }

        // List
        if let Ok(list) = obj.cast::<PyList>() {
            let mut out = Vec::new();
            for item in list.iter() {
                out.push(item.extract::<OwnedValue>()?);
            }
            return Ok(OwnedValue::List(out));
        }

        // Tuple
        if let Ok(tuple) = obj.cast::<PyTuple>() {
            let mut out = Vec::new();
            for item in tuple.iter() {
                out.push(item.extract::<OwnedValue>()?);
            }
            return Ok(OwnedValue::Tuple(out));
        }

        // Dict
        if let Ok(dict) = obj.cast::<PyDict>() {
            let mut out = Vec::new();
            for (k, v) in dict.iter() {
                let key: String = k.extract()?;
                let value: OwnedValue = v.extract()?;
                out.push((key, value));
            }
            return Ok(OwnedValue::Dict(out));
        }

        // Wenn nichts passt → Fehler
        Err(pyo3::exceptions::PyTypeError::new_err("Unsupported Python type"))
    }
}

impl<'py> IntoPyObject<'py> for OwnedValue {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            OwnedValue::None => Ok(py.None().into_bound(py)),

            OwnedValue::Bool(v) => {
                let py_bool = v.into_pyobject(py)?.to_owned();
                Ok(py_bool.into_any())
            }

            OwnedValue::I64(v) => Ok(v.into_pyobject(py)?.to_owned().into_any()),

            OwnedValue::U64(v) => Ok(v.into_pyobject(py)?.to_owned().into_any()),

            OwnedValue::F64(v) => Ok(v.into_pyobject(py)?.to_owned().into_any()),

            OwnedValue::String(v) => Ok(v.into_pyobject(py)?.to_owned().into_any()),

            OwnedValue::Bytes(v) => Ok(PyBytes::new(py, &v).into_any()),

            OwnedValue::List(values) => {
                let items: Vec<Py<PyAny>> = values
                    .into_iter()
                    .map(|v| v.into_pyobject(py).map(|o| o.unbind()))
                    .collect::<PyResult<_>>()?;

                Ok(PyList::new(py, items)?.into_any())
            }

            OwnedValue::Tuple(values) => {
                let items: Vec<Py<PyAny>> = values
                    .into_iter()
                    .map(|v| v.into_pyobject(py).map(|o| o.unbind()))
                    .collect::<PyResult<_>>()?;

                Ok(PyTuple::new(py, items)?.into_any())
            }

            OwnedValue::Dict(values) => {
                let dict = PyDict::new(py);

                for (k, v) in values {
                    dict.set_item(k, v.into_pyobject(py)?.unbind())?;
                }

                Ok(dict.into_any())
            }

            OwnedValue::Complex { re, im } => Ok(pyo3::types::PyComplex::from_doubles(py, re, im).into_any()),
        }
    }
}

pub fn archive_value(value: &OwnedValue) -> Result<rkyv::util::AlignedVec, String> {
    rkyv::to_bytes::<rkyv::rancor::Error>(value).map_err(|e| format!("archive failed: {e}"))
}

pub fn access_archived(bytes: &[u8]) -> Result<&ArchivedOwnedValue, String> {
    // Zugriff ohne Deserialisierung (zero-copy)
    rkyv::access::<ArchivedOwnedValue, rkyv::rancor::Error>(bytes).map_err(|e| format!("access failed: {e}"))
}

// ======================================================
// ZERO-COPY: ARCHIVED -> PYTHON
// ======================================================
//
// Diese Funktion liest direkt aus ArchivedOwnedValue und baut daraus
// Python-Objekte. Das vermeidet die Zwischen-Deserialisierung nach OwnedValue.
//
// Achtung:
// - Python-Objekte selbst müssen natürlich trotzdem erstellt werden.
// - "zero-copy" bezieht sich hier auf den Zugriff auf die archivierten Daten,
//   nicht auf die Python-Objekterzeugung.
//

#[inline]
pub fn archived_to_py_bound<'py>(py: Python<'py>, value: &ArchivedOwnedValue) -> PyResult<Bound<'py, PyAny>> {
    match value {
        ArchivedOwnedValue::None => Ok(py.None().into_bound(py).into_any()),

        ArchivedOwnedValue::Bool(v) => {
            let py_bool = v.into_pyobject(py)?.to_owned();
            Ok(py_bool.into_any())
        }
        ArchivedOwnedValue::I64(v) => {
            let x: i64 = (*v).into();
            Ok(x.into_pyobject(py)?.into_any())
        }

        ArchivedOwnedValue::U64(v) => {
            let x: u64 = (*v).into();
            Ok(x.into_pyobject(py)?.into_any())
        }

        ArchivedOwnedValue::F64(v) => {
            let x: f64 = (*v).into();
            Ok(x.into_pyobject(py)?.into_any())
        }

        ArchivedOwnedValue::String(v) => Ok(PyString::new(py, v.as_str()).into_any()),

        ArchivedOwnedValue::Bytes(v) => Ok(PyBytes::new(py, v.as_slice()).into_any()),

        ArchivedOwnedValue::List(values) => {
            let list = PyList::empty(py);
            for (i, item) in values.iter().enumerate() {
                list.set_item(i, archived_to_py_bound(py, item)?)?;
            }
            Ok(list.into_any())
        }
        ArchivedOwnedValue::Tuple(values) => {
            let mut items = Vec::with_capacity(values.len());
            for item in values.iter() {
                items.push(archived_to_py_bound(py, item)?);
            }
            Ok(PyTuple::new(py, items)?.into_any())
        }

        ArchivedOwnedValue::Dict(values) => {
            let dict = PyDict::new(py);
            for entry in values.iter() {
                dict.set_item(entry.0.as_str(), archived_to_py_bound(py, &entry.1)?)?;
            }
            Ok(dict.into_any())
        }

        ArchivedOwnedValue::Complex { re, im } => {
            let re: f64 = (*re).into();
            let im: f64 = (*im).into();
            Ok(pyo3::types::PyComplex::from_doubles(py, re, im).into_any())
        }
    }
}

#[pyclass]
pub struct ArchivedDictView {
    pub inner: *const ArchivedOwnedValue,
}
#[pymethods]
impl ArchivedDictView {
    fn __getitem__(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let value = unsafe { &*self.inner };

        if let ArchivedOwnedValue::Dict(entries) = value {
            for entry in entries.iter() {
                if entry.0.as_str() == key {
                    return archived_to_py_bound(py, &entry.1).map(|v| v.unbind());
                }
            }
            Err(pyo3::exceptions::PyKeyError::new_err(key.to_string()))
        } else {
            Err(pyo3::exceptions::PyTypeError::new_err("Not a dict"))
        }
    }

    fn __repr__(&self) -> String {
        "<ArchivedDictView (lazy)>".to_string()
    }
}
unsafe impl Send for ArchivedDictView {}
unsafe impl Sync for ArchivedDictView {}



/* /// A Python module implemented in Rust.

/// Archiviert Python-Wert → Bytes
#[pyfunction]
fn archive(py: Python<'_>, value: OwnedValue) -> PyResult<Py<PyBytes>> {
    let bytes = archive_value(&value).map_err(pyo3::exceptions::PyValueError::new_err)?;

    Ok(PyBytes::new(py, &bytes).unbind())
}
#[pyfunction]
fn access_archived(py: Python<'_>, bytes: &[u8]) -> PyResult<Py<PyAny>> {
    let archived = access_archived_value(bytes).map_err(pyo3::exceptions::PyValueError::new_err)?;

    match archived {
        ArchivedOwnedValue::Dict(_) => {
            let view = ArchivedDictView {
                inner: archived as *const _,
            };
            let res = Py::new(py, view).map(|v| v.into_bound(py).unbind())?;
            Ok(res.into())
        }
        _ => {
            // fallback (primitive types direkt)
            archived_to_py_bound(py, archived).map(|v| v.unbind())
        }
    }
} */