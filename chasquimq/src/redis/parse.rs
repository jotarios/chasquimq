use crate::redis::keys::PAYLOAD_FIELD;
use bytes::Bytes;
use fred::types::Value;
use std::sync::Arc;

pub(crate) type StreamEntryId = Arc<str>;

#[derive(Debug)]
pub(crate) struct ParsedEntry {
    pub id: StreamEntryId,
    pub payload: Bytes,
    pub delivery_count: i64,
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
