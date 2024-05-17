use std::{collections::BTreeMap, fs::File, io::Error, path::Path};

use apache_avro::{
    from_value,
    Reader,
};
use csv::{ReaderBuilder, StringRecord};

use crate::metadata::TableMetadata;

// Move to metadata?
pub fn schema_to_db(table_metadata: &TableMetadata) -> String {
    // Turn table metadata into sql create statement
    let create_table_statement = format!(
        "create table if not exists {} (\n{})",
        table_metadata.metadata.name,
        table_metadata.table_definition()
    );
    return create_table_statement;
}

pub fn read_path(path: &Path, format: &String) -> Result<Vec<BTreeMap<String, String>>, Error> {
    // Open the file for given format
    let mut rows: Vec<BTreeMap<String, String>> = Vec::new();
    if path.is_file() {
        if *format == String::from("csv") && path.extension().unwrap() == "csv" {
            let file = File::open(path)?;

            // Create a CSV reader
            let mut rdr = ReaderBuilder::new()
                .has_headers(true) // Specify that the CSV file has headers
                .from_reader(file);
            #[allow(unused_assignments)]
            let mut perm_headers = StringRecord::new();
            {
                // Read and print the headers
                let headers = rdr.headers()?;
                perm_headers = headers.clone();
            }
            // Iterate over each record (row) in the CSV file
            for result in rdr.records() {
                let mut row: BTreeMap<String, String> = BTreeMap::new();
                // Extract the record
                let record = result?;
                for entry in record.iter().enumerate() {
                    let key = perm_headers.get(entry.0).unwrap();
                    row.insert(key.to_string(), entry.1.to_string());
                }
                if row.len() > 0 {
                    rows.push(row);
                }
            }
        } else if *format == "avro" && path.extension().unwrap() == "avro" {
            let f = File::open(path)?;
            let r = Reader::new(f).unwrap();
            for value in r {
                match value {
                    Ok(v) => {
                        let mut row: BTreeMap<String, String> = BTreeMap::new();
                        if let Ok(rec) = from_value::<serde_json::Value>(&v) {
                            if let Some(object_row) = rec.as_object() {
                                for column in object_row.keys() {
                                    if let Some(val) = object_row.get(column) {
                                        let mut record: String = String::from("");
                                        if val.is_string() {
                                            record = val.to_string();
                                            // Trim quotes
                                            let mut chars = record.chars();
                                            chars.next();
                                            chars.next_back();
                                            record = chars.as_str().to_string();
                                        } else {
                                           record = val.to_string();
                                        }
                                        row.insert(column.clone(), record);
                                    }
                                }
                                rows.push(row);
                            }
                        }
                    }
                    Err(e) => println!("Error: {}", e),
                };
            }
        }
    }
    Ok(rows)
}
