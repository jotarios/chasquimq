use crate::redis::keys::{NAME_FIELD, PAYLOAD_FIELD};
use bytes::Bytes;
use fred::types::Value;
use std::sync::Arc;

pub(crate) type StreamEntryId = Arc<str>;

#[derive(Debug)]
pub(crate) struct ParsedEntry {
    pub id: StreamEntryId,
    pub payload: Bytes,
    pub delivery_count: i64,
    /// Optional dispatch name carried alongside the payload as a separate
    /// stream-entry field (`n`). Empty when the producer didn't supply one
    /// or when the entry has no `n` field at all (forward-compat with
    /// pre-name-on-wire producers).
    pub name: String,
}

#[derive(Debug)]
pub(crate) enum EntryShape {
    Ok(ParsedEntry),
    MalformedWithId {
        id: StreamEntryId,
        reason: &'static str,
    },
    Unrecoverable,
}

pub(crate) fn parse_xreadgroup_response(value: &Value) -> Vec<EntryShape> {
    let outer = match value {
        Value::Array(items) => items,
        _ => return Vec::new(),
    };
    let stream_pair = match outer.first() {
        Some(Value::Array(items)) if items.len() >= 2 => items,
        _ => return Vec::new(),
    };
    let entries = match &stream_pair[1] {
        Value::Array(items) => items,
        _ => return Vec::new(),
    };
    entries.iter().map(parse_entry).collect()
}

fn parse_entry(value: &Value) -> EntryShape {
    let items = match value {
        Value::Array(items) => items,
        _ => return EntryShape::Unrecoverable,
    };
    let id: StreamEntryId = match items.first() {
        Some(Value::String(s)) => Arc::from(&**s),
        Some(Value::Bytes(b)) => match std::str::from_utf8(b) {
            Ok(s) => Arc::from(s),
            Err(_) => return EntryShape::Unrecoverable,
        },
        _ => return EntryShape::Unrecoverable,
    };
    let fields = match items.get(1) {
        Some(Value::Array(items)) => items,
        _ => {
            return EntryShape::MalformedWithId {
                id,
                reason: "fields not an array",
            };
        }
    };
    let payload = match extract_payload_field(fields) {
        Some(b) => b,
        None => {
            return EntryShape::MalformedWithId {
                id,
                reason: "missing or non-bytes payload field",
            };
        }
    };
    let name = extract_name_field(fields).unwrap_or_default();
    let delivery_count = items
        .get(3)
        .and_then(|v| match v {
            Value::Integer(n) => Some(*n),
            _ => None,
        })
        .unwrap_or(0);
    EntryShape::Ok(ParsedEntry {
        id,
        payload,
        delivery_count,
        name,
    })
}

/// Parses an XRANGE response into a list of (entry_id, fields_map_pairs).
/// Each entry is `[id, [k, v, k, v, ...]]`. We don't decode field-by-field
/// here — caller pulls what they need.
pub(crate) fn parse_xrange_response(value: &Value) -> Vec<XrangeEntry> {
    let entries = match value {
        Value::Array(items) => items,
        _ => return Vec::new(),
    };
    entries.iter().filter_map(parse_xrange_entry).collect()
}

#[derive(Debug)]
pub(crate) struct XrangeEntry {
    pub id: String,
    pub fields: Vec<(String, FieldValue)>,
}

#[derive(Debug)]
pub(crate) enum FieldValue {
    Str(String),
    Bytes(Bytes),
}

impl FieldValue {
    pub fn as_string(&self) -> Option<String> {
        match self {
            FieldValue::Str(s) => Some(s.clone()),
            FieldValue::Bytes(b) => std::str::from_utf8(b).ok().map(|s| s.to_string()),
        }
    }

    pub fn as_bytes(&self) -> Bytes {
        match self {
            FieldValue::Str(s) => Bytes::from(s.as_bytes().to_vec()),
            FieldValue::Bytes(b) => b.clone(),
        }
    }
}

fn parse_xrange_entry(value: &Value) -> Option<XrangeEntry> {
    let items = match value {
        Value::Array(items) if items.len() >= 2 => items,
        _ => return None,
    };
    let id = match &items[0] {
        Value::String(s) => s.to_string(),
        Value::Bytes(b) => std::str::from_utf8(b).ok()?.to_string(),
        _ => return None,
    };
    let raw_fields = match &items[1] {
        Value::Array(f) => f,
        _ => return None,
    };
    let mut fields: Vec<(String, FieldValue)> = Vec::with_capacity(raw_fields.len() / 2);
    let mut iter = raw_fields.iter();
    while let (Some(name), Some(val)) = (iter.next(), iter.next()) {
        let name_str = match name {
            Value::String(s) => s.to_string(),
            Value::Bytes(b) => match std::str::from_utf8(b) {
                Ok(s) => s.to_string(),
                Err(_) => continue,
            },
            _ => continue,
        };
        let field_val = match val {
            Value::String(s) => FieldValue::Str(s.to_string()),
            Value::Bytes(b) => FieldValue::Bytes(b.clone()),
            _ => continue,
        };
        fields.push((name_str, field_val));
    }
    Some(XrangeEntry { id, fields })
}

fn extract_payload_field(fields: &[Value]) -> Option<Bytes> {
    let mut iter = fields.iter();
    while let (Some(name), Some(val)) = (iter.next(), iter.next()) {
        let is_payload = match name {
            Value::String(s) => s.as_bytes() == PAYLOAD_FIELD.as_bytes(),
            Value::Bytes(b) => b.as_ref() == PAYLOAD_FIELD.as_bytes(),
            _ => false,
        };
        if is_payload {
            return match val {
                Value::Bytes(b) => Some(b.clone()),
                Value::String(s) => Some(Bytes::from(s.as_bytes().to_vec())),
                _ => None,
            };
        }
    }
    None
}

fn extract_name_field(fields: &[Value]) -> Option<String> {
    let mut iter = fields.iter();
    while let (Some(name), Some(val)) = (iter.next(), iter.next()) {
        let is_name = match name {
            Value::String(s) => s.as_bytes() == NAME_FIELD.as_bytes(),
            Value::Bytes(b) => b.as_ref() == NAME_FIELD.as_bytes(),
            _ => false,
        };
        if is_name {
            return match val {
                Value::String(s) => Some(s.to_string()),
                Value::Bytes(b) => std::str::from_utf8(b).ok().map(|s| s.to_string()),
                _ => None,
            };
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry_with_fields(id: &str, fields: Vec<(&str, Value)>) -> Value {
        let mut field_array: Vec<Value> = Vec::with_capacity(fields.len() * 2);
        for (k, v) in fields {
            field_array.push(Value::from(k));
            field_array.push(v);
        }
        Value::Array(vec![Value::from(id), Value::Array(field_array)])
    }

    fn xreadgroup_envelope(entries: Vec<Value>) -> Value {
        Value::Array(vec![Value::Array(vec![
            Value::from("stream-key"),
            Value::Array(entries),
        ])])
    }

    #[test]
    fn parses_n_field_when_present() {
        let payload = Bytes::from_static(b"\x91\x01");
        let entry = entry_with_fields(
            "1700000000000-0",
            vec![
                ("d", Value::Bytes(payload.clone())),
                ("n", Value::String("send-email".into())),
            ],
        );
        let parsed = parse_xreadgroup_response(&xreadgroup_envelope(vec![entry]));
        match parsed.into_iter().next().expect("one entry") {
            EntryShape::Ok(e) => {
                assert_eq!(e.name, "send-email");
                assert_eq!(e.payload, payload);
            }
            other => panic!("unexpected shape: {other:?}"),
        }
    }

    /// Forward-compat: an entry with no `n` field at all (the only shape a
    /// pre-slice-1 producer can emit) decodes with `name = ""`. This is
    /// what makes mixed-version deploys safe.
    #[test]
    fn missing_n_field_yields_empty_name() {
        let payload = Bytes::from_static(b"\x91\x01");
        let entry = entry_with_fields(
            "1700000000000-1",
            vec![("d", Value::Bytes(payload.clone()))],
        );
        let parsed = parse_xreadgroup_response(&xreadgroup_envelope(vec![entry]));
        match parsed.into_iter().next().expect("one entry") {
            EntryShape::Ok(e) => {
                assert_eq!(e.name, "");
                assert_eq!(e.payload, payload);
            }
            other => panic!("unexpected shape: {other:?}"),
        }
    }

    #[test]
    fn n_field_can_arrive_as_bytes() {
        let payload = Bytes::from_static(b"\x91\x01");
        let entry = entry_with_fields(
            "1700000000000-2",
            vec![
                ("n", Value::Bytes(Bytes::from_static(b"resize-image"))),
                ("d", Value::Bytes(payload)),
            ],
        );
        let parsed = parse_xreadgroup_response(&xreadgroup_envelope(vec![entry]));
        match parsed.into_iter().next().expect("one entry") {
            EntryShape::Ok(e) => assert_eq!(e.name, "resize-image"),
            other => panic!("unexpected shape: {other:?}"),
        }
    }
}
