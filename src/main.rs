use clap::{Command, Arg};
use calamine::{Data, Range, Reader, Xlsx, open_workbook};
use polars::prelude::*;
use rayon::prelude::*;
use polars::error::PolarsError;
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
        .arg(Arg::new("header")
            .short('t')
            .long("header")
            .help("Header row number")
            .required(false))
        .get_matches();

    // Extract values from matches
    let path = matches.get_one::<String>("path").unwrap();
    let worksheet = {
        matches
        .get_one::<String>("worksheet")
        .map(|s| s.as_str())
    };
    let header_rows = matches
    .get_one::<String>("header")
    .map(|s| {
        s.split(',')
            .map(|v| v.trim().parse::<usize>())
            .collect::<Result<Vec<_>, _>>()
    })
    .transpose()?;

    // Use the arguments from CLI
    let df = process_excel_worksheet(path, worksheet, header_rows)?;
    println!("{}", df.head(Some(10)));
    Ok(())
}


fn process_excel_worksheet(
    path: &str,
    worksheet_name: Option<&str>,
    header_rows: Option<Vec<usize>>, // <-- modified type
) -> Result<DataFrame, Box<dyn Error>> {
    let range = get_worksheet_range(path, worksheet_name)?;
    let header_rows = header_rows.unwrap_or(vec![0]);

    let rows: Vec<Vec<Data>> = range.rows().map(|row| row.to_vec()).collect();
    // Check header indices are in bounds
    for &idx in &header_rows {
        if idx >= rows.len() {
            return Err("One of header row indices is out of bounds".into());
        }
    }

    // Collect header rows
    let header_cells: Vec<&[Data]> = header_rows.iter().map(|&i| &rows[i][..]).collect();
    // Collapse headers
    let headers = collapse_multi_headers(&header_cells)?;

    // Data starts after the last header row
    let data_start = header_rows.iter().max().map(|x| x+1).unwrap_or(1);
    let data_rows = &rows[data_start..];
    let data = extract_data(data_rows, headers.len());
    let df = create_dataframe(headers, data)?;
    Ok(df)
}


fn collapse_multi_headers(header_cells: &Vec<&[Data]>) -> Result<Vec<String>, Box<dyn Error>> {
    if header_cells.is_empty() {
        return Err("Empty header cells".into());
    }
    let cols = header_cells[0].len();
    let mut collapsed = Vec::with_capacity(cols);
    for col_idx in 0..cols {
        let parts: Vec<String> = header_cells.iter()
            .map(|row| row.get(col_idx).map(|d| d.to_string()).unwrap_or_default())
            .filter(|part| !part.starts_with("Unnamed") && !part.trim().is_empty())
            .collect();

        collapsed.push(if parts.is_empty() {
            format!("Unnamed_{}", col_idx)
        } else {
            parts.join(" ")
        });
    }
    Ok(collapsed)
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


fn extract_data(data_rows: &[Vec<Data>], header_len: usize) -> Vec<Vec<String>> {
    data_rows
        .iter()
        .map(|row| {
            let mut cells: Vec<String> = row.iter().map(|cell| cell.to_string()).collect();
            cells.resize(header_len, String::new());
            cells
        })
        .collect()
}

/// Processes a vector of header names to ensure uniqueness.
///
/// This function takes a vector of strings representing header names and processes them
/// to ensure that each header name is unique. If a header name is empty, it is replaced
/// with a default name in the format "Unnamed_{index}", where {index} is the position
/// of the header in the input vector. If a header name already exists in the processed
/// list, a suffix is appended to the name to make it unique, following the format
/// "{base_name}_{suffix}".
///
/// # Parameters
///
/// - `headers`: A vector of strings containing the header names to be processed.
///
/// # Returns
///
/// A vector of strings containing the processed header names, ensuring that all names
/// are unique. If there were any duplicates or empty names in the input, they will be
/// modified accordingly.
///
/// # Example
///
/// ```
/// let headers = vec!["Header1".to_string(), "".to_string(), "Header1".to_string()];
/// let processed = process_headers(headers);
/// assert_eq!(processed, vec!["Header1", "Unnamed_1", "Header1_1"]);
/// ```
///
/// # Panics
///
/// This function does not panic under normal circumstances, but it assumes that the
/// input vector is not excessively large, as it uses a hash set to track used names.
/// 
/// # Complexity
///
/// The function has a time complexity of O(n) where n is the number of headers, as it
/// iterates through the list and performs constant-time operations for each header.
fn process_headers(headers: Vec<String>) -> Vec<String> {
    let mut processed_headers = Vec::with_capacity(headers.len());
    let mut used_names = PlHashSet::new();

    for (i, header) in headers.iter().enumerate() {
        let base_name = if header.is_empty() {
            format!("Unnamed_{}", i)
        } else {
            header.clone()
        };

        let mut candidate = base_name.clone();
        let mut suffix = 0;

        // Generate a unique candidate name
        while used_names.contains(&candidate) {
            suffix += 1;
            candidate = format!("{}_{}", base_name, suffix);
        }

        used_names.insert(candidate.clone());
        processed_headers.push(candidate);
    }
    processed_headers
}


fn create_dataframe(headers: Vec<String>, data: Vec<Vec<String>>) -> Result<DataFrame, PolarsError> {
    let headers = process_headers(headers);
    let columns: Vec<Column> = (0..headers.len())
        .into_par_iter()
        .map(|i| {
            let col_data: Vec<&str> = data.iter().map(|row| row[i].as_str()).collect();
            let series = Series::new(headers[i].clone().into(), &col_data);
            series.into()
        })
        .collect();
    Ok(DataFrame::new(columns)?)
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::error;

    #[test]
    fn it_works() -> Result<(), Box<dyn error::Error>>{
        let path: &str = "/home/yehori/Documents/Projects/Rust learning/excel_reader/src/test.xlsx";
        let worksheet_name: &str = "МАЙ  2024";
        let df = process_excel_worksheet(path, Some(worksheet_name), None)?;
        assert_eq!(df.shape().0, 2100);
        Ok(())
    }
}
