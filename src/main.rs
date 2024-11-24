use calamine::{Reader, open_workbook, Xlsx};
use polars::prelude::*;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    // Open the Excel file
    let path = "Беркут.xlsx";
    let mut workbook: Xlsx<_> = open_workbook(path)?;

    // Select worksheet
    if let Ok(range) = workbook.worksheet_range("МАЙ  2024") {
        // Get headers from first row
        let headers = range.rows()
            .next()
            .unwrap()
            .iter()
            .map(|c| c.to_string())
            .collect::<Vec<String>>();

        // Convert data rows to vectors
        let data: Vec<Vec<String>> = range.rows()
            .skip(1)  // Skip header row
            .map(|row| {
                row.iter()
                    .map(|cell| cell.to_string())
                    .collect()
            })
            .collect();

        // Create series for each column
        let mut columns = Vec::new();
        for (i, header) in headers.iter().enumerate() {
            let series = Series::new(
                PlSmallStr::from_str(header),
                data.iter().map(|row| row[i].clone()).collect::<Vec<String>>()
            );
            columns.push(series.into_column());
        }

        // Create DataFrame
        let df = DataFrame::new(columns)?;
        println!("{}", df.head(Some(10)));
    }

    Ok(())
}