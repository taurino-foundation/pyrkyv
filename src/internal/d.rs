//! ==========================================================
//! FULL EXAMPLE: PyO3 + rkyv + OwnedValue
//! ==========================================================
//!
//! Features:
//! - Python → Rust (FromPyObject)
//! - Rust → Python (IntoPyObject)
//! - rkyv serialization (to_bytes)
//! - rkyv zero-copy access (access)
//! - optional deserialize
//! - vollständiges PyO3 Modul
//!
//! ==========================================================

use rkyv::{access, rancor::Error, Archive, Deserialize, Serialize};
use pyo3::prelude::*;
use pyo3::types::{
    PyAny, PyBytes, PyComplex, PyDict, PyList, PyString, PyTuple,
};

// ==========================================================
// OWNED VALUE ENUM (rekursiv + rkyv kompatibel)
// ==========================================================

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
#[rkyv(
    compare(PartialEq),
    derive(Debug),

    // notwendig wegen rekursiver Struktur
    serialize_bounds(
        __S: rkyv::ser::Writer + rkyv::ser::Allocator,
        __S::Error: rkyv::rancor::Source,
    ),

    deserialize_bounds(
        __D::Error: rkyv::rancor::Source
    ),

    // notwendig für access() / Validierung
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

    // Rekursive Felder → omit_bounds zwingend
    List(#[rkyv(omit_bounds)] Vec<OwnedValue>),
    Tuple(#[rkyv(omit_bounds)] Vec<OwnedValue>),
    Dict(#[rkyv(omit_bounds)] Vec<(String, OwnedValue)>),

    Complex {
        re: f64,
        im: f64,
    },
}

// ==========================================================
// VALUE KIND (optional Typ-Info Helper)
// ==========================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueKind {
    None,
    Bool,
    I64,
    U64,
    F64,
    String,
    Bytes,
    List,
    Tuple,
    Dict,
    Complex,
}

// ==========================================================
// RKYV HELPERS
// ==========================================================

/// Serialisiert OwnedValue → Bytes
pub fn archive_value(value: &OwnedValue) -> Result<Vec<u8>, String> {
    let bytes = rkyv::to_bytes::<Error>(value)
        .map_err(|e| format!("archive failed: {e}"))?;

    Ok(bytes.to_vec())
}

/// Zero-copy Zugriff auf archivierte Daten
pub fn access_archived(bytes: &[u8]) -> Result<&ArchivedOwnedValue, String> {
    access::<ArchivedOwnedValue, Error>(bytes)
        .map_err(|e| format!("access failed: {e}"))
}

/// Optional: echte Deserialisierung zurück zu OwnedValue
pub fn deserialize_value(bytes: &[u8]) -> Result<OwnedValue, String> {
    let archived = access_archived(bytes)?;

    archived
        .deserialize(&mut rkyv::api::high::deserializers::HighDeserializer::<
            Error,
        >::default())
        .map_err(|e| format!("deserialize failed: {e}"))
}

// ==========================================================
// PYTHON → RUST
// ==========================================================

impl<'a, 'py> FromPyObject<'a, 'py> for OwnedValue {
    type Error = PyErr;

    fn extract(obj: Borrowed<'a, 'py, PyAny>) -> Result<Self, Self::Error> {

        // None
        if obj.is_none() {
            return Ok(OwnedValue::None);
        }

        // Bool (muss vor int!)
        if let Ok(v) = obj.extract::<bool>() {
            return Ok(OwnedValue::Bool(v));
        }

        // Integer
        if let Ok(v) = obj.extract::<i64>() {
            return Ok(OwnedValue::I64(v));
        }

        if let Ok(v) = obj.extract::<u64>() {
            return Ok(OwnedValue::U64(v));
        }

        // Float
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

        // Complex
        if let Ok(v) = obj.cast::<PyComplex>() {
            return Ok(OwnedValue::Complex {
                re: v.real(),
                im: v.imag(),
            });
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

        Err(pyo3::exceptions::PyTypeError::new_err(
            "Unsupported Python type",
        ))
    }
}

// ==========================================================
// RUST → PYTHON
// ==========================================================

impl<'py> IntoPyObject<'py> for OwnedValue {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {

        match self {

            OwnedValue::None => Ok(py.None().into_bound(py)),

            // Primitive Typen → Borrowed → to_owned nötig
            OwnedValue::Bool(v) => Ok(v.into_pyobject(py)?.to_owned().into_any()),
            OwnedValue::I64(v) => Ok(v.into_pyobject(py)?.to_owned().into_any()),
            OwnedValue::U64(v) => Ok(v.into_pyobject(py)?.to_owned().into_any()),
            OwnedValue::F64(v) => Ok(v.into_pyobject(py)?.to_owned().into_any()),

            // String direkt Bound
            OwnedValue::String(v) => Ok(v.into_pyobject(py)?.into_any()),

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

            OwnedValue::Complex { re, im } => {
                Ok(PyComplex::from_doubles(py, re, im).into_any())
            }
        }
    }
}

// ==========================================================
// PYTHON FUNCTIONS (Expose API)
// ==========================================================

/// Einfacher Roundtrip: Python → Rust → Python
#[pyfunction]
fn roundtrip_value(py: Python<'_>, value: OwnedValue) -> PyResult<Py<PyAny>> {
    println!("Rust received: {value:#?}");
    value.into_pyobject(py).map(|o| o.unbind())
}

/// Archiviert Python-Wert → Bytes
#[pyfunction]
fn archive_from_python(py: Python<'_>, value: OwnedValue) -> PyResult<Py<PyBytes>> {
    let bytes = archive_value(&value)
        .map_err(pyo3::exceptions::PyValueError::new_err)?;

    Ok(PyBytes::new(py, &bytes).unbind())
}

/// Debug: zeigt Rust-Struktur
#[pyfunction]
fn inspect_value(value: OwnedValue) -> PyResult<String> {
    Ok(format!("{value:#?}"))
}

// ==========================================================
// PYTHON MODULE
// ==========================================================

#[pymodule]
fn myvalues(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(roundtrip_value, m)?)?;
    m.add_function(wrap_pyfunction!(archive_from_python, m)?)?;
    m.add_function(wrap_pyfunction!(inspect_value, m)?)?;
    Ok(())
}

// ==========================================================
// OPTIONAL: RUST TEST MAIN
// ==========================================================

#[allow(dead_code)]
fn main() {
    let value = OwnedValue::Dict(vec![
        ("name".into(), OwnedValue::String("Erik".into())),
        ("age".into(), OwnedValue::I64(25)),
        (
            "items".into(),
            OwnedValue::List(vec![
                OwnedValue::I64(1),
                OwnedValue::I64(2),
                OwnedValue::I64(3),
            ]),
        ),
    ]);

    println!("Original: {value:#?}");

    let bytes = archive_value(&value).unwrap();
    println!("Bytes len: {}", bytes.len());

    let archived = access_archived(&bytes).unwrap();
    println!("Archived (zero-copy): {archived:#?}");

    let restored = deserialize_value(&bytes).unwrap();
    println!("Restored: {restored:#?}");
}


/// https://github.com/pola-rs/polars/blob/main/crates/polars-python/src/conversion/any_value.rs#L65


pub(crate) fn any_value_into_py_object<'py>(
    av: AnyValue<'_>,
    py: Python<'py>,
) -> PyResult<Bound<'py, PyAny>> {
    let utils = pl_utils(py).bind(py);
    match av {
        AnyValue::UInt8(v) => v.into_bound_py_any(py),
        AnyValue::UInt16(v) => v.into_bound_py_any(py),
        AnyValue::UInt32(v) => v.into_bound_py_any(py),
        AnyValue::UInt64(v) => v.into_bound_py_any(py),
        AnyValue::UInt128(v) => v.into_bound_py_any(py),
        AnyValue::Int8(v) => v.into_bound_py_any(py),
        AnyValue::Int16(v) => v.into_bound_py_any(py),
        AnyValue::Int32(v) => v.into_bound_py_any(py),
        AnyValue::Int64(v) => v.into_bound_py_any(py),
        AnyValue::Int128(v) => v.into_bound_py_any(py),
        AnyValue::Float16(v) => v.to_f32().into_bound_py_any(py),
        AnyValue::Float32(v) => v.into_bound_py_any(py),
        AnyValue::Float64(v) => v.into_bound_py_any(py),
        AnyValue::Null => py.None().into_bound_py_any(py),
        AnyValue::Boolean(v) => v.into_bound_py_any(py),
        AnyValue::String(v) => v.into_bound_py_any(py),
        AnyValue::StringOwned(v) => v.into_bound_py_any(py),
        AnyValue::Categorical(cat, map) | AnyValue::Enum(cat, map) => unsafe {
            map.cat_to_str_unchecked(cat).into_bound_py_any(py)
        },
        AnyValue::CategoricalOwned(cat, map) | AnyValue::EnumOwned(cat, map) => unsafe {
            map.cat_to_str_unchecked(cat).into_bound_py_any(py)
        },
        AnyValue::Date(v) => {
            let date = date32_to_date(v);
            date.into_bound_py_any(py)
        },
        AnyValue::Datetime(v, time_unit, time_zone) => {
            datetime_to_py_object(py, v, time_unit, time_zone)
        },
        AnyValue::DatetimeOwned(v, time_unit, time_zone) => {
            datetime_to_py_object(py, v, time_unit, time_zone.as_ref().map(AsRef::as_ref))
        },
        AnyValue::Duration(v, time_unit) => {
            let time_delta = elapsed_offset_to_timedelta(v, time_unit);
            time_delta.into_bound_py_any(py)
        },
        AnyValue::Time(v) => nanos_since_midnight_to_naivetime(v).into_bound_py_any(py),
        AnyValue::Array(v, _) | AnyValue::List(v) => PySeries::new(v).to_list(py),
        ref av @ AnyValue::Struct(_, _, flds) => {
            Ok(struct_dict(py, av._iter_struct_av(), flds)?.into_any())
        },
        AnyValue::StructOwned(payload) => {
            Ok(struct_dict(py, payload.0.into_iter(), &payload.1)?.into_any())
        },
        #[cfg(feature = "object")]
        AnyValue::Object(v) => {
            let object = v.as_any().downcast_ref::<ObjectValue>().unwrap();
            Ok(object.inner.clone_ref(py).into_bound(py))
        },
        #[cfg(feature = "object")]
        AnyValue::ObjectOwned(v) => {
            let object = v.0.as_any().downcast_ref::<ObjectValue>().unwrap();
            Ok(object.inner.clone_ref(py).into_bound(py))
        },
        AnyValue::Binary(v) => PyBytes::new(py, v).into_bound_py_any(py),
        AnyValue::BinaryOwned(v) => PyBytes::new(py, &v).into_bound_py_any(py),
        AnyValue::Decimal(v, prec, scale) => {
            let convert = utils.getattr(intern!(py, "to_py_decimal"))?;
            let mut buf = DecimalFmtBuffer::new();
            let s = buf.format_dec128(v, scale, false, false);
            convert.call1((prec, s))
        },
    }
}
