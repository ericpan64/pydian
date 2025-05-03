use serde_json::json;
use mapper::{Mapper, DropLevel};
use dicts::flatten_value;
use anyhow::Result;

#[test]
fn test_no_rules_returns_clone() {
    let payload = json!({"foo": 1, "bar": [1, 2, 3]});
    let result = Mapper::new().run(&payload);
    assert_eq!(result, payload);
}

#[test]
fn test_map_and_drop_field() -> Result<()> {
    let payload = json!({
        "a": { "b": [[1, 2], [3, 4]] },
        "c": 5
    });
    let result = Mapper::new()
        .map("a.b", |v| flatten_value(v))?
        .map("c", |v| json!(v.as_i64().unwrap() * 2))?
        .drop(DropLevel::Field)
        .run(&payload);
    assert_eq!(result, json!({
        "a": { "b": [1, 2, 3, 4] },
        "c": 10
    }));
    Ok(())
}

#[test]
fn test_drop_root_removes_empty_values() {
    let payload = json!({
        "empty_list": [],
        "empty_obj": {},
        "keep": {"x": 1},
        "keep_list": [null]
    });
    let result = Mapper::new()
        .drop(DropLevel::Root)
        .run(&payload);
    assert_eq!(result, json!({
        "keep": {"x": 1},
        "keep_list": [null]
    }));
}

#[test]
fn test_invalid_map_path_returns_err() {
    let err = Mapper::new().map("invalid..path", |_| json!(null));
    assert!(err.is_err());
}
