from typing import Union, List, Tuple, Dict

from typing import Union, List, Tuple, Dict, Optional, Any, Iterable

__version__: str

# https://github.com/JPK314/pyany-serde/blob/main/src/pyany_serde_type.rs#L477
# =========================
# Core Value Type
# =========================
Value = Union[
    None,
    bool,
    int,
    float,
    str,
    bytes,
    List["Value"],
    Tuple["Value", ...],
    Dict[str, "Value"],
    complex,
]




def archive(value: Value) -> bytes:
    """
    Serialize a Python value into rkyv-encoded bytes.

    This function converts a Python value into a compact binary representation
    using the rkyv zero-copy serialization format.

    Supported types:
        - None
        - bool
        - int (signed and unsigned 64-bit)
        - float (64-bit)
        - str
        - bytes
        - list
        - tuple
        - dict[str, Value]
        - complex

    The resulting bytes can be stored or transmitted and later accessed
    efficiently without full deserialization.

    Args:
        value: The Python value to serialize.

    Returns:
        A bytes object containing the rkyv-encoded representation.

    Raises:
        ValueError: If serialization fails.
        TypeError: If the value contains unsupported types.
    """
    ...


def load_archived(bytes: bytes) -> Value:
    """
    Access rkyv-encoded bytes and convert them into Python objects.

    This function performs zero-copy access to the archived data and reconstructs
    the corresponding Python object structure without fully deserializing
    intermediate representations.

    Note:
        "Zero-copy" refers to accessing the serialized data without allocating
        an intermediate Rust representation. Python objects are still newly created.

    Args:
        bytes: The rkyv-encoded byte buffer.

    Returns:
        The reconstructed Python value.

    Raises:
        ValueError: If the input bytes are invalid or corrupted.
    """
    ...