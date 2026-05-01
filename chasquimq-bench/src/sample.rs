use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Payload {
    pub data: serde_json::Value,
}

pub fn generate_sample(width: usize, depth: usize) -> Payload {
    assert!(depth >= 1, "depth must be >= 1");
    let mut top = serde_json::Map::with_capacity(width);
    for _ in 0..width {
        top.insert(uuid::Uuid::new_v4().to_string(), build_chain(depth));
    }
    Payload {
        data: serde_json::Value::Object(top),
    }
}

fn build_chain(depth: usize) -> serde_json::Value {
    let mut innermost = serde_json::Map::new();
    for _ in 1..depth {
        let key = uuid::Uuid::new_v4().to_string();
        let mut next = serde_json::Map::new();
        next.insert(key, serde_json::Value::Object(innermost));
        innermost = next;
    }
    serde_json::Value::Object(innermost)
}
