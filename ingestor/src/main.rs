use polars::prelude::*;
use std::fs::File;

fn main() -> PolarsResult<()> {
    // Prefix with 'ingestor/' so main.py can find them from the root directory
    let input_path = "ingestor/data/olist_order_payments_dataset.csv";
    let output_path = "ingestor/data/orders_payments.parquet";

    println!("🚀 Reading CSV from {}...", input_path);

    // Load the data
    let df = LazyCsvReader::new(input_path.into())
        .with_has_header(true)
        .with_infer_schema_length(Some(100)) 
        .finish()?
        .collect()?;

    println!("✅ Data loaded. Shape: {:?}", df.shape());

    // Drop nulls based on order_id
    let cleaned_df = df.drop_nulls::<String>(Some(&["order_id".to_string()]))?;

    println!("💾 Saving to Parquet: {}...", output_path);
    
    // Ensure the output file is created successfully
    let mut file = File::create(output_path).map_err(|e| {
        PolarsError::ComputeError(format!("Could not create file {}: {}", output_path, e).into())
    })?;

    ParquetWriter::new(&mut file).finish(&mut cleaned_df.clone())?;

    println!("✨ Process Complete!");
    println!("👀 First 5 rows:\n{}", cleaned_df.head(Some(5)));

    Ok(())
}