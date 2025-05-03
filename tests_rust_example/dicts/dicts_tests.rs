use serde_json::json;
use dicts::{PathSegment, parse_path, extract, get_value, flatten_value};
use anyhow::Result;

#[test]
fn test_parse_path_nested() {
    let path = "foo.bar[0].baz";
    let segments = parse_path(path).expect("Failed to parse path");
    assert_eq!(segments, vec![
        PathSegment::Field("foo".to_string()),
        PathSegment::Field("bar".to_string()),
        PathSegment::Index(0),
        PathSegment::Field("baz".to_string()),
    ]);
}

#[test]
fn test_extract_field_and_index() -> Result<()> {
    let v = json!({ "foo": { "bar": [10, 20] } });
    let segments = parse_path("foo.bar[1]")?;
    let result = extract(&v, &segments)?;
    assert_eq!(result, json!(20));
    Ok(())
}

#[test]
fn test_get_value_success() -> Result<()> {
    let v = json!({ "a": 100 });
    let result: i64 = get_value(&v, "a")?;
    assert_eq!(result, 100);
    Ok(())
}

#[test]
fn test_get_value_missing() {
    let v = json!({});
    let result: Result<i32> = get_value(&v, "missing");
    assert!(result.is_err());
}

#[test]
fn test_flatten_value_nested() {
    let v = json!([[1, 2], [3, 4]]);
    let flat = flatten_value(v);
    assert_eq!(flat, json!([1, 2, 3, 4]));
}

#[test]
fn test_extract_all_operator() -> Result<()> {
    let v = json!({ "items": [ { "x": 1 }, { "x": 2 }, { "x": 3 } ] });
    let result = extract(&v, &parse_path("items[*].x")?)?;
    assert_eq!(result, json!([1, 2, 3]));
    Ok(())
}
