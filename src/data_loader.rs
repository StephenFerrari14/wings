use std::{
    collections::BTreeMap,
    fs::File,
    io::Error,
    path::PathBuf, time::SystemTime,
};

use csv::{ReaderBuilder, StringRecord};
use sqlite::{Connection, Value};
use walkdir::{DirEntry, WalkDir};

use crate::{metadata::{self, TableMetadata}, rayon_loader, utils::{read, schema_to_db}};

enum RunMode {
    ForLoop,
    Async,
    Rayon,
}

pub fn load(connection: &Connection, tables: Vec<PathBuf>) {
    // In series first then parallelize, add timers

    // For table get table.yaml metadata (parallelize)
    //   Get table schema
    //   Create table in sqlite
    //   Get data_path and format
    //   For all files in data_path with format (parallelize)
    //     Read data
    //     Convert to table schema
    //   return converted data as vec of rows
    //   Insert rows into sqlite

    let mode = RunMode::Rayon;
    match mode {
        RunMode::ForLoop => for_loop_load(connection, tables),
        RunMode::Async => todo!(),
        RunMode::Rayon => rayon_loader::load(connection, tables),
    }
}

fn project(
    row: BTreeMap<String, String>,
    schema: BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    // TODO
    return row;
}

fn for_loop_load(connection: &Connection, tables: Vec<PathBuf>) {
    // For table get table.yaml metadata (parallelize)
    for table in tables {
        let now = SystemTime::now();
        let table_metadata = metadata::get_table_metadata(table).unwrap();
        //   Get table schema
        //   Create table in sqlite
        let create_table_sql = schema_to_db(&table_metadata);
        connection.execute(create_table_sql).unwrap();

        //   Get data_path and format
        //   For all files in data_path with format (parallelize)
        let data_path = table_metadata.metadata.data_path.clone();
        let format = table_metadata.metadata.format.clone();

        // Either specify fields in insert or make sure values are in the right order
        let columns: Vec<String> = table_metadata.schema.keys().cloned().collect();
        let columns_clause = columns.join(",");

        let values: Vec<String> = columns.iter().map(|f| format!(":{}", f)).collect();
        let values_clause = values.join(",");

        // Insert rows into sqlite
        let query = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            table_metadata.metadata.name, columns_clause, values_clause
        );


        let mut rows: Vec<BTreeMap<String, String>> = Vec::new();
        let file_now = SystemTime::now();
        for entry in WalkDir::new(data_path) {
            // Read data
            let raw_rows = read(entry.unwrap(), &format);
            match raw_rows {
                Ok(ok_rows) => {
                  for row in ok_rows {
                      //     Convert to table schema
                      // let converted_row = project(row, table_metadata.schema);
                      let converted_row = row;
                      rows.push(converted_row);
                  }
                }
                Err(_) => panic!("Cannot read data rows"),
            }
        }
        match file_now.elapsed() {
            Ok(elapsed) => {
                println!("Files read for {} in {}ms", table_metadata.metadata.name, elapsed.as_millis());
            }
            Err(e) => {
                // an error occurred!
                println!("Error: {e:?}");
            }
        }

        if rows.len() == 0 {
          println!("No rows found");
          return;
        }

        
        // Do this in batches instead of single inserts
        let insert_now = SystemTime::now();
        for row in rows {
            let mut statement = connection.prepare(query.clone()).unwrap();
            // For each row create a vector of tuples that is
            // (":column", "value")
 
            let mut index = 0;
            let bind_vars: Vec<(&str, Value)> = columns
                .iter()
                .map(|f| {
                    let bind_var: (&str, Value) =
                        (values[index].as_str(), row.get(f).unwrap().as_str().into());
                    index = index + 1;
                    return bind_var;
                })
                .collect();
            let res = statement.bind_iter::<_, (_, Value)>(bind_vars);
            match res {
                Ok(_) => (),
                Err(e) => println!("There was an error inserting data. {}", e),
            }

            let _ = statement.next();
        }
        match insert_now.elapsed() {
            Ok(elapsed) => {
                println!("Data insert for {} in {}ms", table_metadata.metadata.name, elapsed.as_millis());
            }
            Err(e) => {
                // an error occurred!
                println!("Error: {e:?}");
            }
        }
        match now.elapsed() {
            Ok(elapsed) => {
                println!("File load for {} in {}ms", table_metadata.metadata.name, elapsed.as_millis());
            }
            Err(e) => {
                // an error occurred!
                println!("Error: {e:?}");
            }
        }
    }
}
