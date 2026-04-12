use polars::prelude::*;
use std::fs::File;
use std::path::Path; // Added this for path handling

fn main() -> PolarsResult<()> {
    let input_path = "data/olist_orders_dataset.csv";
    let output_path = "data/orders_cleaned.parquet";

    println!("🚀 Reading CSV from {}...", input_path);

    // 1. In 0.53, we use .into() to turn the string into the required path type
    // 2. 'with_infer_schema_some' was replaced by 'with_infer_schema_length'
    let df = LazyCsvReader::new(input_path.into())
        .with_has_header(true)
        .with_infer_schema_length(Some(100)) 
        .finish()?
        .collect()?;

    println!("✅ Data loaded. Shape: {:?}", df.shape());

    // In 0.53, drop_nulls syntax is cleaner
    let cleaned_df = df.drop_nulls::<String>(Some(&["order_id".to_string()]))?;

    println!("💾 Saving to Parquet: {}...", output_path);
    let mut file = File::create(output_path).expect("Could not create file");
    ParquetWriter::new(&mut file).finish(&mut cleaned_df.clone())?;

    println!("✨ Process Complete!");
    println!("👀 First 5 rows:\n{}", cleaned_df.head(Some(5)));

    Ok(())
}