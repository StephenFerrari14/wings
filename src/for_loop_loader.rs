use std::{collections::BTreeMap, path::PathBuf, time::SystemTime};

use sqlite::{Connection, Value};
use walkdir::WalkDir;

use crate::{
    metadata,
    utils::{read, read_path, schema_to_db},
};

pub fn load(files: Vec<PathBuf>, format: String) -> Vec<Vec<BTreeMap<String, String>>> {
    let for_rows: Vec<Vec<BTreeMap<String, String>>> = files
        .iter()
        .map(|entry| {
            let raw_rows = read_path(entry.as_path(), &format);
            match raw_rows {
                Ok(ok_rows) => {
                    // Add projection function here
                    return ok_rows;
                }
                Err(_) => panic!("Cannot read data rows"),
            }
        })
        .collect();
    return for_rows;
}

// pub fn for_loop_load(connection: &Connection, tables: Vec<PathBuf>) {
//     // For table get table.yaml metadata (parallelize)
//     for table in tables {
//         let now = SystemTime::now();
//         let table_metadata = metadata::get_table_metadata(table).unwrap();
//         //   Get table schema
//         //   Create table in sqlite
//         create_table_from_metadata(connection, &table_metadata);

//         //   Get data_path and format
//         //   For all files in data_path with format (parallelize)
//         let data_path = table_metadata.metadata.data_path.clone();
//         let format = table_metadata.metadata.format.clone();

//         // Either specify fields in insert or make sure values are in the right order
//         let columns: Vec<String> = table_metadata.schema.keys().cloned().collect();
//         let values: Vec<String> = columns.iter().map(|f| format!(":{}", f)).collect();

//         let mut rows: Vec<BTreeMap<String, String>> = Vec::new();
//         let file_now = SystemTime::now();
//         //// This is whats different
//         for entry in WalkDir::new(data_path) {
//             // Read data
//             let raw_rows = read(entry.unwrap(), &format);
//             match raw_rows {
//                 Ok(ok_rows) => {
//                     for row in ok_rows {
//                         //     Convert to table schema
//                         // let converted_row = project(row, table_metadata.schema);
//                         let converted_row = row;
//                         rows.push(converted_row);
//                     }
//                 }
//                 Err(_) => panic!("Cannot read data rows"),
//             }
//         }
//         ///
//         match file_now.elapsed() {
//             Ok(elapsed) => {
//                 println!(
//                     "Files read for {} in {}ms",
//                     table_metadata.metadata.name,
//                     elapsed.as_millis()
//                 );
//             }
//             Err(e) => {
//                 // an error occurred!
//                 println!("Error: {e:?}");
//             }
//         }

//         if rows.len() == 0 {
//             println!("No rows found");
//             return;
//         }

//         // Do this in batches instead of single inserts
//         load_db(
//             connection,
//             &table_metadata.metadata.name,
//             columns,
//             values,
//             rows,
//         );

//         match now.elapsed() {
//             Ok(elapsed) => {
//                 println!(
//                     "File load for {} in {}ms",
//                     table_metadata.metadata.name,
//                     elapsed.as_millis()
//                 );
//             }
//             Err(e) => {
//                 // an error occurred!
//                 println!("Error: {e:?}");
//             }
//         }
//     }
// }
