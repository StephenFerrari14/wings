use std::collections::{HashMap, HashSet};
use pad::PadStr;

const DIVIDER: &str = "|";

pub struct DisplayRow {
    raw_row: HashMap<String, String>,
    // value_widths: HashMap<String, usize>,
    columns: Vec<String>,
}

impl DisplayRow {
    pub fn new(row: HashMap<String, String>) -> DisplayRow {
        let mut columns: Vec<String> = Vec::new();
        // let mut value_widths: HashMap<String, usize> = HashMap::new();
        // let mut column_widths: HashMap<String, usize> = HashMap::new();

        row.keys().for_each(|column| {
            columns.push(column.clone());
            // column_widths.insert(column.clone(), column.len());
            // let row_value = row.get(column).unwrap();
            // value_widths.insert(row_value.clone(), row_value.len()); // Change this to be column -> size of value
        });

        return DisplayRow {
            raw_row: row,
            // value_widths,
            columns,
            // column_widths
        };
    }
}

/// Render display lines in result table
pub fn render(display_rows: Vec<DisplayRow>) {
    let columns = get_column_row(&display_rows);
    let max_widths = get_max_widths(&display_rows, &columns);

    let mut total_width: usize = max_widths.values().sum();
    total_width = total_width + (columns.len() - 1);

    let first_line = get_first_display_line(total_width);
    let column_row = get_column_display_line(&columns, &max_widths);
    let render_rows = get_data_display_lines(display_rows, columns, max_widths);

    // Put it all together
    let result_table = first_line.clone()
        + "\n"
        + &column_row.to_owned()
        + "\n"
        + &first_line.clone()
        + "\n"
        + &render_rows
        + &first_line.clone();
    println!("{}", result_table);
}

/// Format data display line with dividers between values
fn get_data_display_lines(
    display_rows: Vec<DisplayRow>,
    columns: HashSet<String>,
    max_widths: HashMap<String, usize>,
) -> String {
    let mut result_rows: Vec<String> = vec![];
    for drow in display_rows {
        let mut results_row_vec: Vec<String> = vec![DIVIDER.to_string()];
        for column in &columns {
            let cell_width = max_widths.get(column).unwrap().clone();
            let value_default = String::from(""); // Make this a constant
            let content = drow.raw_row.get(column).unwrap_or(&value_default);
            let cell = content.pad_to_width(cell_width).clone();
            results_row_vec.push(cell);
            results_row_vec.push(DIVIDER.to_string());
        }
        let result_row = results_row_vec.into_iter().collect::<String>();
        result_rows.push(result_row + "\n");
    }
    let render_rows = result_rows.into_iter().collect::<String>();
    render_rows
}

/// Format column display line with dividers between values
fn get_column_display_line(
    columns: &HashSet<String>,
    max_widths: &HashMap<String, usize>,
) -> String {
    let mut column_row_vec: Vec<String> = vec![DIVIDER.to_string()];
    columns.iter().for_each(|column| {
        let cell_width = max_widths.get(column).unwrap().clone();
        let cell = column.pad_to_width(cell_width).clone();
        column_row_vec.push(cell);
        column_row_vec.push(DIVIDER.to_string());
    });
    let column_row = column_row_vec.into_iter().collect::<String>();
    column_row
}

/// Get result table border line
fn get_first_display_line(total_width: usize) -> String {
    let first_line_border = vec!["-"; total_width];
    let mut first_line_vec = vec!["+"];
    let first_line_end = vec!["+"];
    first_line_vec.extend_from_slice(&first_line_border);
    first_line_vec.extend_from_slice(&first_line_end);
    let first_line = first_line_vec.into_iter().collect::<String>();
    first_line
}

/// Get set of columns based on display rows
/// Each display row isn't gaurenteed to have the same columns so all have to be checked
fn get_column_row(display_rows: &Vec<DisplayRow>) -> HashSet<String> {
    let mut columns: HashSet<String> = HashSet::new();
    display_rows.iter().for_each(|drow| {
        drow.columns.iter().for_each(|col| {
            columns.insert(col.clone());
        })
    });

    return columns;
}

/// Get the maximum widths of each column's values
/// Including column name
fn get_max_widths(
    display_rows: &Vec<DisplayRow>,
    columns: &HashSet<String>,
) -> HashMap<String, usize> {
    let mut max_widths: HashMap<String, usize> = HashMap::new();
    // Get widths of each column into map
    columns.iter().for_each(|column| {
        max_widths.insert(column.clone(), column.len());
    });

    display_rows.iter().for_each(|drow| {
        drow.columns.iter().for_each(|drow_column| {
            let column_width = drow.raw_row.get(drow_column).unwrap().len();
            if max_widths.contains_key(drow_column) {
                if max_widths.get(drow_column).unwrap() < &column_width {
                    max_widths.insert(drow_column.clone(), column_width.clone());
                }
            } else {
                max_widths.insert(drow_column.clone(), column_width.clone());
            }
        });
    });

    max_widths
}

/// Generic function to convert vector of hashmaps to DisplayRows
pub fn display_rows_from_maps(maps: Vec<HashMap<String, String>>) -> Vec<DisplayRow> {
    let display_rows: Vec<DisplayRow> = maps.iter().map(|map| {
        DisplayRow::new(map.clone())
    }).collect();
    return display_rows;
}
