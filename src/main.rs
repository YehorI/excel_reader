use clap::{Command, Arg};
use calamine::{Data, Range, Reader, Xlsx, open_workbook};
use polars::prelude::*;
use std::error::Error;


fn main() -> Result<(), Box<dyn Error>> {
    // Parse command line arguments
    let matches = Command::new("Excel Reader")
        .version("1.0")
        .author("YehorI")
        .about("Excel worksheet processor")
        .arg(Arg::new("path")
            .short('p')
            .long("path")
            .help("Path to the Excel file")
            .required(true))
        .arg(Arg::new("worksheet")
            .short('w')
            .long("worksheet")
            .help("Name of the worksheet to process")
            .required(false))
        .get_matches();

    // Extract values from matches
    let path = matches.get_one::<String>("path").unwrap();
    let worksheet = {
        matches
        .get_one::<String>("worksheet")
        .map(|s| s.as_str())
    };

    // Use the arguments from CLI
    let df = process_excel_worksheet(path, worksheet)?;
    println!("{}", df.head(Some(10)));
    Ok(())
}


fn process_excel_worksheet(path: &str, worksheet_name: Option<&str>) -> Result<DataFrame, Box<dyn Error>> {
    let range = get_worksheet_range(path, worksheet_name)?;
    let headers = extract_headers(&range)?;
    let data = extract_data(&range, headers.len());
    let df = create_dataframe(headers, data)?;
    Ok(df)
}


fn get_worksheet_range(path: &str, worksheet_name: Option<&str>) -> Result<Range<Data>, Box<dyn Error>> {
    let mut workbook: Xlsx<_> = open_workbook(path)?;

    let range = match worksheet_name {
        Some(name) => workbook.worksheet_range(name)?,
        None => {
            // Get the first worksheet
            let sheets = workbook.worksheets();
            if sheets.is_empty() {
                return Err("No worksheets found in the workbook".into());
            }

            // Clone the range from the first worksheet
            // sheets[0] contains a tuple of (name, range)
            sheets[0].1.clone()
        }
    };

    Ok(range)
}


fn extract_headers(range: &Range<Data>) -> Result<Vec<String>, Box<dyn Error>> {
    let header_row = range
        .rows()
        .next()
        .ok_or("Worksheet contains no header row")?;
    Ok(header_row
        .iter()
        .map(|cell| cell.to_string())
        .collect())
}


fn extract_data(range: &Range<Data>, header_len: usize) -> Vec<Vec<String>> {
    range
        .rows()
        .skip(1)
        .map(|row| {
            let mut cells: Vec<String> = row.iter().map(|cell| cell.to_string()).collect();
            cells.resize(header_len, String::new());
            cells
        })
        .collect()
}


fn create_dataframe(headers: Vec<String>, data: Vec<Vec<String>>) -> Result<DataFrame, Box<dyn Error>> {
    let mut columns = Vec::with_capacity(headers.len());
    for (i, header) in headers.iter().enumerate() {
        let series_data: Vec<&str> = data.iter().map(|row| row[i].as_str()).collect();
        let series = Series::new(header.into(), series_data);
        columns.push(series.into());
    }
    Ok(DataFrame::new(columns)?)
}
