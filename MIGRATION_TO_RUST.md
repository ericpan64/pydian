# Migration Plan: pydian (Python) → Rust

## 1. Goals

- **Performance & Safety**: Leverage Rust’s zero-cost abstractions and strong typing.  
- **Feature Parity**: Match existing Python API (dict `get`, `Mapper`, DataFrame DSL, validation).  
- **Modularity**: Split into crates for each concern.  
- **Ease of Use**: Provide ergonomic Rust API and meaningful error messages.

## 2. Workspace Layout

```text
pydian/               # Cargo workspace root
├── Cargo.toml        # [workspace] members = ["dicts", "mapper", "dataframes", "validation"]
├── dicts/            # nested-get & core dict transforms
├── mapper/           # composable mappings with DROP/KEEP
├── dataframes/       # Polars-based DSL crate
└── validation/       # Rule/RuleGroup validation crate
```

## 3. Common Dependencies

- `serde` + `serde_json` for data interchange  
- `anyhow` for error handling and context (use `anyhow::Result<T>` & `anyhow::Context`)  
- `jmespath` (Rust crate) or custom path parser  
- `polars` Rust crate  
- `nom` for DSL parsing

## 4. Module-by-Module Plan

### 4.1 dicts
- **Error Handling:** use `anyhow::Result` for public APIs and `with_context` for richer errors.
- **PathSegment & Parser:**
  ```rust
  pub enum PathSegment { Field(String), Index(usize), All, Multi(Vec<String>) }
  pub fn parse_path(input: &str) -> IResult<&str, Vec<PathSegment>> { /* nom parser */ }
  ```
- **Functional Extractors & Traversal:**
  ```rust
  use anyhow::{Result, Context};
  pub fn extract(value: &Value, path: &[PathSegment]) -> Result<Value> {
      path.iter().try_fold(value.clone(), |cur, seg| {
          Ok(match seg {
              PathSegment::Field(k) => cur.get(k)
                  .cloned()
                  .with_context(|| format!("key not found: {}", k))?,
              PathSegment::Index(i) => cur.get(*i)
                  .cloned()
                  .with_context(|| format!("index out of bounds: {}", i))?,
              PathSegment::All => Value::Array(cur.as_array().cloned().unwrap_or_default()),
              PathSegment::Multi(keys) => Value::Array(
                  keys.iter().map(|k| cur.get(k).cloned().unwrap_or(Value::Null)).collect()
              ),
          })
      })
  }

  pub fn get_value<T: DeserializeOwned>(value: &Value, path: &str) -> Result<T> {
      let segments = parse_path(path).context("parsing path")?;
      let v = extract(value, &segments)?;
      serde_json::from_value(v).context("deserializing value")
  }
  ```
- **Transforms & Utilities:**
  ```rust
  pub fn flatten_value(v: Value) -> Value { /* recursive flatten */ }
  pub fn apply_transform(v: Value, f: impl Fn(Value) -> Value) -> Value { f(v) }
  ```
- **Testing:**
  - Tests in `dicts/tests/` using `serde_json::json!` and `assert_eq!`.

### 4.2 mapper
- **Crate Layout:**
  ```text
  mapper/
  ├── Cargo.toml      # depends on dicts
  └── src/lib.rs      # Mapper and MappingRule definitions
  ```
- **MappingRule & DropLevel:**
  ```rust
  pub enum DropLevel { Root, Field }
  pub enum MappingRule {
      Map { path: Vec<PathSegment>, func: Box<dyn Fn(Value) -> Value> },
      Drop(DropLevel),
  }
  ```
- **MappingRule Explanation:**
  The `MappingRule` enum captures the core Python `Mapper` operations:
  - `Map { path, func }`: mirrors Python’s `Mapper.map(path, func)`. We parse `path`
    into `Vec<PathSegment>` for nested traversal and store `func` as a boxed callback.
    During `run()`, each rule applies its transform where specified.
  - `Drop(level)`: corresponds to Python’s `Mapper.drop(level)`, removing null or
    empty entries at the `Root` or `Field` level after all mappings.

  This enum-based design provides exhaustive matching, type safety, and
  an API familiar to Python users while leveraging Rust’s performance.
- **Mapper Struct & API:**
  ```rust
  pub struct Mapper { rules: Vec<MappingRule> }

  impl Mapper {
      pub fn new() -> Self { Self { rules: Vec::new() } }

      pub fn map<F>(mut self, path: &str, func: F) -> Result<Self, DictError>
      where F: Fn(Value) -> Value + 'static {
          let segments = parse_path(path)?;
          self.rules.push(MappingRule::Map { path: segments, func: Box::new(func) });
          Ok(self)
      }

      pub fn drop(mut self, level: DropLevel) -> Self {
          self.rules.push(MappingRule::Drop(level));
          self
      }

      pub fn run(&self, input: &Value) -> Value {
          let mut v = input.clone();
          for rule in &self.rules {
              match rule {
                  MappingRule::Map { path, func } => apply_map(&mut v, path, func),
                  MappingRule::Drop(level) => apply_drop(&mut v, level),
              }
          }
          v
      }
  }
  ```
- **Helper Functions:**
  ```rust
  fn apply_map(v: &mut Value, path: &[PathSegment], f: &dyn Fn(Value) -> Value) {
      if let Some((first, rest)) = path.split_first() {
          match first {
              PathSegment::Field(key) => {
                  if let Some(val) = v.get_mut(key) {
                      if rest.is_empty() { *val = f(val.take()); }
                      else { apply_map(val, rest, f); }
                  }
              }
              PathSegment::Index(i) => { /* similar for arrays */ }
              _ => {}
          }
      }
  }

  fn apply_drop(v: &mut Value, level: &DropLevel) {
      // recursively remove Null or empty arrays/objects at specified level
  }
  ```
- **Example Usage:**
  ```rust
  let result = Mapper::new()
      .map("a.b", |v| flatten_value(v))?
      .map("c", |v| json!(v.as_i64().unwrap() * 2))?
      .drop(DropLevel::Field)
      .run(&payload);
  ```

### 4.3 dataframes
- **Crate Layout & Features:**
  ```toml
  [dependencies]
  polars = { version = "*", features = ["lazy"] }
  nom = "7"
  ```
- **DSL Parser (`parser.rs` using nom):**
  ```rust
  use nom::{IResult, branch::alt, bytes::complete::tag, character::complete::{alphanumeric1, digit1, multispace0}, combinator::{map, map_res}, multi::separated_list1, sequence::{delimited, tuple}};

  fn identifier(input: &str) -> IResult<&str, String> {
      map(alphanumeric1, |s: &str| s.to_string())(input)
  }

  fn columns(input: &str) -> IResult<&str, Vec<String>> {
      separated_list1(tag(","), identifier)(input)
  }

  pub fn parse_query(input: &str) -> IResult<&str, QueryAst> {
      // use nom combinators to parse select, filter, join, union, groupby into QueryAst
      todo!()
  }
  ```
- **AST & Translator:**
  ```rust
  #[derive(Debug)]
  pub enum QueryAst {
      Select { cols: Vec<String>, filter: Option<String> },
      Join { left: String, right: String, on: Vec<String>, kind: JoinType },
      Union,
      GroupBy { by: Vec<String>, aggs: Vec<Agg> },
  }

  pub fn translate(
      ast: QueryAst,
      df: LazyFrame,
      others: Vec<LazyFrame>
  ) -> Result<LazyFrame, PydianError> {
      match ast {
          QueryAst::Select{cols, filter} => df.select(...).filter(...),
          QueryAst::Join{..} => df.join(...),
          // handle Union, GroupBy
      }
  }
  ```
- **Error Types & Tests:**
  ```rust
  #[derive(thiserror::Error, Debug)]
  pub enum PydianError {
      #[error("DSL parse error: {0}")]
      Parse(#[from] nom::Err<(&str, nom::error::ErrorKind)>),
      #[error("Polars error: {0}")]
      Polars(#[from] PolarsError),
  }
  ```
  - Tests in `dataframes/tests/` with sample `LazyFrame` operations.

#### Implementation Details for dataframes
- Use nom combinators in `parser.rs` to parse the DSL:
  - Parsers for identifiers, columns, conditions, joins, unions, groupby using `nom::branch`, `nom::bytes`, `nom::character`, `nom::sequence`, `nom::multi`, and `nom::combinator`.
  - Build `QueryAst` by mapping nom parse results within map/map_res combinators.
  - Use `IResult<&str, QueryAst>` for error handling and chaining combinators.
- In `translate`, convert `QueryAst` to `LazyFrame` operations: use `col`, `filter`, `join`, `with_columns`, and `groupby` APIs.
- Ensure `translate` supports chaining join/unions/groupby in order.

### 4.4 validation
- **Crate Layout & Dependencies:**
  ```toml
  [dependencies]
  serde_json = "1.0"
  thiserror = "1.0"
  ```
- **Check Trait & ValidationError:**
  ```rust
  pub trait Check {
      fn check(&self, v: &Value) -> Result<(), ValidationError>;
  }

  #[derive(thiserror::Error, Debug)]
  #[error("Validation failed on {field}: {message}")]
  pub struct ValidationError {
      pub field: String,
      pub message: String,
  }
  ```
- **Rule & RuleGroup Execution:**
  ```rust
  pub struct Rule { field: Vec<PathSegment>, check: Box<dyn Check> }
  pub struct RuleGroup { rules: Vec<Rule> }

  impl RuleGroup {
      pub fn validate(&self, data: &Value) -> Result<Value, Vec<ValidationError>> {
          let mut errs = Vec::new();
          for rule in &self.rules {
              if let Err(e) = rule.check_field(data) { errs.push(e); }
          }
          if errs.is_empty() { Ok(data.clone()) } else { Err(errs) }
      }
  }
  ```
- **Built-in Checks & Tests:**
  - Implement `IsRequired`, `IsType`, `InRange`, `InSet`, `MinCount`, `MaxCount` in `checks.rs`.
  - Tests in `validation/tests/` using `serde_json::json!`.

#### Implementation Details for validation
- In `checks.rs`, implement each `Check` with clear error messages, e.g.:
  ```rust
  pub struct InRange(pub i64, pub i64);
  impl Check for InRange {
      fn check(&self, v: &Value) -> Result<(), ValidationError> {
          match v.as_i64() {
              Some(n) if n >= self.0 && n <= self.1 => Ok(()),
              Some(n) => Err(ValidationError { field: "".into(), message: format!("{} out of range", n) }),
              None => Err(ValidationError { field: "".into(), message: "not integer".into() }),
          }
      }
  }
  ```
- Use dicts `parse_path` to resolve `Rule.field` into `PathSegment` for nested access.
- Aggregate errors in `RuleGroup::validate` preserving field context for each error.

## 5. CI & Testing
- Run `cargo test --workspace`, `cargo fmt -- --check`, and `cargo clippy -- -D warnings`.  
- Port pytest tests to Rust using `serde_json::json!` for fixtures.  
- Define grammar in `nom` (`*.rs` file).  
- Build AST enums: `Select`, `Filter`, `Join`, `Union`, `GroupBy`.  
- Unit-test parser separately.
- Example GitHub Actions CI:
  ```yaml
  name: CI
  on: [push, pull_request]
  jobs:
    build:
      runs-on: ubuntu-latest
      steps:
        - uses: actions/checkout@v3
        - uses: actions-rs/toolchain@v1
          with:
            toolchain: stable
        - run: cargo test --workspace
        - run: cargo fmt -- --check
        - run: cargo clippy -- -D warnings
  ```

## 6. Roadmap & Milestones
1. Scaffold workspace & crates.  
2. Implement `dicts` crate & tests.  
3. Implement `mapper` crate & tests.  
4. Build DSL parser & `dataframes` crate & tests.  
5. Implement `validation` crate & tests.  
6. End-to-end examples & benchmarks.  
7. Documentation & release.
