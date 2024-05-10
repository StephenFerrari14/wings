use std::{collections::BTreeMap, fs::File, io::Error, path::Path};

use csv::{ReaderBuilder, StringRecord};

use crate::metadata::TableMetadata;


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
  // Change to check extension
  let mut rows: Vec<BTreeMap<String, String>> = Vec::new();
  if path.is_file() {
      if *format == String::from("csv") {
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
      }
  }
  Ok(rows)
}