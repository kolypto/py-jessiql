from __future__ import annotations

import base64
import json


def encode_opaque_cursor(prefix: str, data: dict) -> str:
    """ Encode a dict of data as an opaque cursor. Give it a nice prefix so that the user sees what's up """
    return prefix + ':' + base64.b85encode(json.dumps(data).encode()).decode()


def decode_opaque_cursor(data: str) -> tuple[str, dict]:
    """ Decode an opaque cursor into a (prefix, data dict) tuple

    Raises:
        Exception: all sorts of errors related to bad cursor
    """
    prefix, data_encoded = data.split(':', 1)  # ValueError
    assert prefix in ('skip', 'keys')  # AssertionError
    data = json.loads(base64.b85decode(data_encoded))  # binascii.Error, json.decoder.JSONDecodeError
    return prefix, data
