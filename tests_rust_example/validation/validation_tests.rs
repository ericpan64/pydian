use serde_json::json;
use std::collections::HashSet;
use validation::core::Check;
use validation::core::ValidationError;
use validation::checks::{IsRequired, InRange, InSet, MinCount, MaxCount, IsType};
use validation::rules::RuleGroup;

#[test]
fn test_in_range() {
    let c = InRange(2, 4);
    assert!(c.check(&json!(1)).is_err());
    assert!(c.check(&json!(2)).is_ok());
    assert!(c.check(&json!(3)).is_ok());
    assert!(c.check(&json!(4)).is_ok());
    assert!(c.check(&json!(5)).is_err());
}

#[test]
fn test_in_set() {
    let mut set = HashSet::new();
    set.insert(json!("a"));
    set.insert(json!("b"));
    let c = InSet(set);
    assert!(c.check(&json!("a")).is_ok());
    assert!(c.check(&json!("c")).is_err());
}

#[test]
fn test_min_max_count() {
    let arr = json!([1, 2, 3]);
    let c_min = MinCount(2);
    assert!(c_min.check(&arr).is_ok());
    assert!(c_min.check(&json!([1])).is_err());

    let c_max = MaxCount(2);
    assert!(c_max.check(&json!([1, 2])).is_ok());
    assert!(c_max.check(&arr).is_err());
}

#[test]
fn test_is_required() {
    let c = IsRequired;
    assert!(c.check(&json!("value")).is_ok());
    assert!(c.check(&json!(null)).is_err());
    let err = c.check(&json!(null)).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("Validation failed"));
}

#[test]
fn test_is_type() {
    let c = IsType::String;
    assert!(c.check(&json!("value")).is_ok());
    assert!(c.check(&json!(123)).is_err());
}

#[test]
fn test_rule_group_validate() {
    // group: InRange(1..=3) AND InSet({2,3})
    let mut checks: Vec<Box<dyn Check>> = Vec::new();
    checks.push(Box::new(InRange(1, 3)));
    let mut set = HashSet::new();
    set.insert(json!(2));
    set.insert(json!(3));
    checks.push(Box::new(InSet(set)));
    let rg = RuleGroup::new(checks);
    assert!(rg.validate(&json!(2)).is_ok());
    assert!(rg.validate(&json!(1)).is_err());
}
