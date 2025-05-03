use polars::prelude::*;
use polars::lazy::prelude::*;
use dataframes::select;
use anyhow::Result;

fn get_simple_df() -> DataFrame {
    DataFrame::new(vec![
        Series::new("a", &[0i64, 1, 2, 3, 4, 5]),
        Series::new("b", &["q", "w", "e", "r", "t", "y"]),
        Series::new("c", &[true, false, true, false, false, true]),
        Series::new("d", &[None::<i32>, None, None, None, None, None]),
    ])
    .unwrap()
}

#[test]
fn test_select_basic() -> Result<()> {
    let df = get_simple_df();
    let lf = df.clone().lazy();
    // single column
    let res = select(lf.clone(), "a", Vec::new())?;
    let df_res = res.collect()?;
    assert_eq!(df_res, df.select(&["a"]).unwrap());
    // multiple columns
    let res = select(lf.clone(), "a, b", Vec::new())?;
    let df_res = res.collect()?;
    assert_eq!(df_res, df.select(&["a", "b"]).unwrap());
    // star
    let res = select(lf.clone(), "*", Vec::new())?;
    let df_res = res.collect()?;
    assert_eq!(df_res, df);
    Ok(())
}

#[test]
fn test_select_missing_error() {
    let df = get_simple_df();
    let lf = df.lazy();
    let err = select(lf.clone(), "non_existent", Vec::new());
    assert!(err.is_err());
}

#[test]
fn test_filter() -> Result<()> {
    let df = get_simple_df();
    let lf = df.clone().lazy();
    let res = select(lf.clone(), "b : [a == 0]", Vec::new())?;
    let df_exp = df.filter(&col("a").eq(lit(0))).select(&["b"]).unwrap();
    let df_res = res.collect()?;
    assert_eq!(df_res, df_exp);
    Ok(())
}

#[test]
fn test_union_basic() -> Result<()> {
    let df = get_simple_df();
    let lf = df.clone().lazy();
    let rows = DataFrame::new(vec![
        Series::new("a", &[6i64]),
        Series::new("b", &["u"]),
        Series::new("c", &[false]),
        Series::new("d", &[None::<i32>]),
    ])
    .unwrap();
    let res = select(lf.clone(), "* from A ++ B", vec![rows.clone().lazy()])?;
    let df_res = res.collect()?;
    let mut exp = df.clone();
    exp.vstack_mut(&rows).unwrap();
    assert_eq!(df_res, exp);
    Ok(())
}
