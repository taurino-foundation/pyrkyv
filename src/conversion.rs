use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyDict, PyList, PyString, PyTuple};
use std::collections::HashMap;

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
        if obj.is_none() {
            return Ok(OwnedValue::None);
        }

        if obj.cast::<pyo3::types::PyBool>().is_ok() {
            return Ok(OwnedValue::Bool(obj.extract::<bool>()?));
        }

        if let Ok(v) = obj.cast::<PyString>() {
            return Ok(OwnedValue::String(v.to_str()?.to_owned()));
        }

        if let Ok(v) = obj.cast::<PyBytes>() {
            return Ok(OwnedValue::Bytes(v.as_bytes().to_vec()));
        }

        if let Ok(list) = obj.cast::<PyList>() {
            let mut out = Vec::with_capacity(list.len());
            for item in list.iter() {
                out.push(item.extract::<OwnedValue>()?);
            }
            return Ok(OwnedValue::List(out));
        }

        if let Ok(tuple) = obj.cast::<PyTuple>() {
            let mut out = Vec::with_capacity(tuple.len());
            for item in tuple.iter() {
                out.push(item.extract::<OwnedValue>()?);
            }
            return Ok(OwnedValue::Tuple(out));
        }

        if let Ok(dict) = obj.cast::<PyDict>() {
            let mut out = Vec::with_capacity(dict.len());
            for (k, v) in dict.iter() {
                out.push((k.extract::<String>()?, v.extract::<OwnedValue>()?));
            }
            return Ok(OwnedValue::Dict(out));
        }

        if let Ok(v) = obj.extract::<i64>() {
            return Ok(OwnedValue::I64(v));
        }

        if let Ok(v) = obj.extract::<u64>() {
            return Ok(OwnedValue::U64(v));
        }

        if let Ok(v) = obj.extract::<f64>() {
            return Ok(OwnedValue::F64(v));
        }

        if let Ok(c) = obj.cast::<pyo3::types::PyComplex>() {
            return Ok(OwnedValue::Complex {
                re: c.real(),
                im: c.imag(),
            });
        }

        Err(pyo3::exceptions::PyTypeError::new_err("Unsupported Python type"))
    }
}

pub fn archive_value(value: &OwnedValue) -> Result<rkyv::util::AlignedVec, String> {
    rkyv::to_bytes::<rkyv::rancor::Error>(value).map_err(|e| format!("archive failed: {e}"))
}

pub fn access_archived(bytes: &[u8]) -> Result<&ArchivedOwnedValue, String> {
    rkyv::access::<ArchivedOwnedValue, rkyv::rancor::Error>(bytes).map_err(|e| format!("access failed: {e}"))
}

#[pyclass]
struct ArchivedDictView {
    buffer: Py<PyBytes>,
    index: HashMap<String, usize>,
    cache: std::sync::Mutex<HashMap<String, Py<PyAny>>>, // 🔥
}

#[pyclass]
struct ArchivedListView {
    buffer: Py<PyBytes>,
    len: usize,
    is_tuple: bool,
}

pub fn root_from_buffer<'py>(buffer: &'py Bound<'py, PyBytes>) -> PyResult<&'py ArchivedOwnedValue> {
    access_archived(buffer.as_bytes()).map_err(pyo3::exceptions::PyValueError::new_err)
}

pub fn wrap_archived<'py>(
    py: Python<'py>,
    buffer: &Bound<'py, PyBytes>,
    value: &ArchivedOwnedValue,
) -> PyResult<Bound<'py, PyAny>> {
    match value {
        ArchivedOwnedValue::None => Ok(py.None().into_bound(py).into_any()),

        ArchivedOwnedValue::Bool(v) => {
            let x: bool = (*v).into();
            Ok(x.into_pyobject(py)?.to_owned().into_any())
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

        ArchivedOwnedValue::Dict(entries) => {
            let mut index = HashMap::with_capacity(entries.len());
            for (i, entry) in entries.iter().enumerate() {
                index.insert(entry.0.as_str().to_owned(), i);
            }

            Py::new(
                py,
                ArchivedDictView {
                    buffer: buffer.clone().unbind(),
                    index,
                    cache: std::sync::Mutex::new(HashMap::new()),
                },
            )
            .map(|o| o.into_bound(py).into_any())
        }

        ArchivedOwnedValue::List(values) => Py::new(
            py,
            ArchivedListView {
                buffer: buffer.clone().unbind(),
                len: values.len(),
                is_tuple: false,
            },
        )
        .map(|o| o.into_bound(py).into_any()),

        ArchivedOwnedValue::Tuple(values) => Py::new(
            py,
            ArchivedListView {
                buffer: buffer.clone().unbind(),
                len: values.len(),
                is_tuple: true,
            },
        )
        .map(|o| o.into_bound(py).into_any()),

        ArchivedOwnedValue::Complex { re, im } => {
            let re: f64 = (*re).into();
            let im: f64 = (*im).into();
            Ok(pyo3::types::PyComplex::from_doubles(py, re, im).into_any())
        }
    }
}

#[pymethods]
impl ArchivedDictView {
    fn __len__(&self) -> usize {
        self.index.len()
    }

    fn __contains__(&self, key: &str) -> bool {
        self.index.contains_key(key)
    }

    fn keys(&self) -> Vec<String> {
        self.index.keys().cloned().collect()
    }

    fn __getitem__(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        // 🔥 1. Cache check
        if let Some(cached) = self.cache.lock().unwrap().get(key) {
            return Ok(cached.clone_ref(py));
        }

        let idx = *self
            .index
            .get(key)
            .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err(key.to_owned()))?;

        let buffer = self.buffer.bind(py);
        let root = root_from_buffer(buffer)?;

        let result = match root {
            ArchivedOwnedValue::Dict(entries) => wrap_archived(py, buffer, &entries[idx].1)?,
            _ => return Err(pyo3::exceptions::PyTypeError::new_err("Not a dict")),
        };

        let result_py = result.unbind();

        // 🔥 2. Cache speichern
        self.cache
            .lock()
            .unwrap()
            .insert(key.to_string(), result_py.clone_ref(py));

        Ok(result_py)
    }

    fn get(&self, py: Python<'_>, key: &str, default: Option<Py<PyAny>>) -> PyResult<Py<PyAny>> {
        match self.__getitem__(py, key) {
            Ok(v) => Ok(v),
            Err(_) => Ok(default.unwrap_or_else(|| py.None().into_bound(py).unbind())),
        }
    }

    fn materialize(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let buffer = self.buffer.bind(py);
        let root = root_from_buffer(buffer)?;
        materialize_archived(py, root)
    }

    fn __repr__(&self) -> String {
        format!("<ArchivedDictView len={}>", self.index.len())
    }
}

#[pymethods]
impl ArchivedListView {
    fn __len__(&self) -> usize {
        self.len
    }

    fn __getitem__(&self, py: Python<'_>, index: isize) -> PyResult<Py<PyAny>> {
        let idx = if index < 0 {
            let pos = self.len as isize + index;
            if pos < 0 {
                return Err(pyo3::exceptions::PyIndexError::new_err("index out of range"));
            }
            pos as usize
        } else {
            index as usize
        };

        if idx >= self.len {
            return Err(pyo3::exceptions::PyIndexError::new_err("index out of range"));
        }

        let buffer = self.buffer.bind(py);
        let root = root_from_buffer(buffer)?;

        match root {
            ArchivedOwnedValue::List(values) if !self.is_tuple => {
                wrap_archived(py, buffer, &values[idx]).map(|v| v.unbind())
            }
            ArchivedOwnedValue::Tuple(values) if self.is_tuple => {
                wrap_archived(py, buffer, &values[idx]).map(|v| v.unbind())
            }
            _ => Err(pyo3::exceptions::PyTypeError::new_err("Not a list/tuple view")),
        }
    }

    fn materialize(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let buffer = self.buffer.bind(py);
        let root = root_from_buffer(buffer)?;
        materialize_archived(py, root)
    }

    fn __repr__(&self) -> String {
        if self.is_tuple {
            format!("<ArchivedTupleView len={}>", self.len)
        } else {
            format!("<ArchivedListView len={}>", self.len)
        }
    }
}

fn materialize_archived(py: Python<'_>, value: &ArchivedOwnedValue) -> PyResult<Py<PyAny>> {
    match value {
        ArchivedOwnedValue::None => Ok(py.None().into_bound(py).unbind()),

        ArchivedOwnedValue::Bool(v) => {
            let x: bool = (*v).into();
            Ok(x.into_pyobject(py)?.to_owned().into_any().unbind())
        }

        ArchivedOwnedValue::I64(v) => {
            let x: i64 = (*v).into();
            Ok(x.into_pyobject(py)?.into_any().unbind())
        }

        ArchivedOwnedValue::U64(v) => {
            let x: u64 = (*v).into();
            Ok(x.into_pyobject(py)?.into_any().unbind())
        }

        ArchivedOwnedValue::F64(v) => {
            let x: f64 = (*v).into();
            Ok(x.into_pyobject(py)?.into_any().unbind())
        }

        ArchivedOwnedValue::String(v) => Ok(PyString::new(py, v.as_str()).into_any().unbind()),

        ArchivedOwnedValue::Bytes(v) => Ok(PyBytes::new(py, v.as_slice()).into_any().unbind()),

        ArchivedOwnedValue::List(values) => {
            let items: Vec<Py<PyAny>> = values
                .iter()
                .map(|item| materialize_archived(py, item))
                .collect::<PyResult<_>>()?;
            Ok(PyList::new(py, items)?.into_any().unbind())
        }

        ArchivedOwnedValue::Tuple(values) => {
            let items: Vec<Py<PyAny>> = values
                .iter()
                .map(|item| materialize_archived(py, item))
                .collect::<PyResult<_>>()?;
            Ok(PyTuple::new(py, items)?.into_any().unbind())
        }

        ArchivedOwnedValue::Dict(values) => {
            let dict = PyDict::new(py);
            for entry in values.iter() {
                dict.set_item(entry.0.as_str(), materialize_archived(py, &entry.1)?)?;
            }
            Ok(dict.into_any().unbind())
        }

        ArchivedOwnedValue::Complex { re, im } => {
            let re: f64 = (*re).into();
            let im: f64 = (*im).into();
            Ok(pyo3::types::PyComplex::from_doubles(py, re, im).into_any().unbind())
        }
    }
}
