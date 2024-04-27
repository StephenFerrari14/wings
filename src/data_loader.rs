use std::{
    collections::BTreeMap,
    fs::{File, FileType},
    io::Error,
    path::PathBuf,
    sync::RwLock,
};

use csv::{ReaderBuilder, StringRecord};
use sqlite::{Connection, State, Value};
use walkdir::{DirEntry, WalkDir};

use crate::metadata::{self, TableMetadata};

enum RunMode {
    ForLoop,
    Async,
    Threads,
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

    let mode = RunMode::ForLoop;
    match mode {
        RunMode::ForLoop => for_loop_load(connection, tables),
        RunMode::Async => todo!(),
        RunMode::Threads => todo!(),
        RunMode::Rayon => rayon_load(connection, tables),
    }
}

fn schema_to_db(table_metadata: &TableMetadata) -> String {
    // Turn table metadata into sql create statement
    let create_table_statement = format!(
        "create table if not exists {} (\n{})",
        table_metadata.metadata.name,
        table_metadata.table_definition()
    );
    return create_table_statement;
}

fn read(file_name: DirEntry, format: &String) -> Result<Vec<BTreeMap<String, String>>, Error> {
    // Open the CSV file
    let mut rows: Vec<BTreeMap<String, String>> = Vec::new();
    // Something is wrong with this logic
    if file_name.path().is_file() { //} && file_name.path().ends_with(format) {
        if *format == String::from("csv") {
            let file = File::open(file_name.path())?;

            // Create a CSV reader
            let mut rdr = ReaderBuilder::new()
                .has_headers(true) // Specify that the CSV file has headers
                .from_reader(file);
            let mut perm_headers = StringRecord::new();
            {
              // Read and print the headers
              let headers = rdr.headers()?;
              println!("Headers: {:?}", headers);
              perm_headers = headers.clone();
            }
            // Iterate over each record (row) in the CSV file
            for result in rdr.records() {
                let mut row: BTreeMap<String, String> = BTreeMap::new();
                // Extract the record
                let record = result?;
                // Print the record
                println!("{:?}", record);
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
        let table_metadata = metadata::get_table_metadata(table);
        //   Get table schema
        //   Create table in sqlite
        let create_table_sql = schema_to_db(&table_metadata);
        println!("{}", create_table_sql);
        connection.execute(create_table_sql).unwrap();

        //   Get data_path and format
        //   For all files in data_path with format (parallelize)
        let data_path = table_metadata.metadata.data_path.clone();
        let format = table_metadata.metadata.format.clone();
        let mut rows: Vec<BTreeMap<String, String>> = Vec::new();
        for entry in WalkDir::new(data_path) {
            println!("Finding data...");
            //     Read data
            let raw_rows = read(entry.unwrap(), &format);
            match raw_rows {
                Ok(ok_rows) => {
                  for row in ok_rows {
                      //     Convert to table schema
                      // let converted_row = project(row, table_metadata.schema);
                      let converted_row = row;
                      //   return converted data as vec of rows
                      rows.push(converted_row);
                  }
                }
                Err(_) => panic!("Cannot read data rows"),
            }
        }

        if rows.len() == 0 {
          println!("No rows found");
          return;
        }

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
        // Do this in batches instead of single inserts
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
                    // bind_vars.push(bind_var.clone());
                })
                .collect();
            println!("Insert row");
            let res = statement.bind_iter::<_, (_, Value)>(bind_vars);
            match res {
                Ok(_) => println!("Rows inserted"),
                Err(e) => println!("There was an error inserting data. {}", e),
            }

            statement.next();
            // while let Ok(State::Row) = statement.next() {
            //     println!("id = {}", statement.read::<i64, _>("id").unwrap());
            //     println!("name = {}", statement.read::<String, _>("name").unwrap());
            // }
        }
    }
}

fn rayon_load(connection: &Connection, tables: Vec<PathBuf>) {
    // For table get table.yaml metadata (parallelize)
    //   Get table schema
    //   Create table in sqlite
    //   Get data_path and format
    //   For all files in data_path with format (parallelize)
    //     Read data
    //     Convert to table schema
    //   return converted data as vec of rows
    //   Insert rows into sqlite

    // use rayon::prelude::*;
    // fn sum_of_squares(input: &[i32]) -> i32 {
    // input.par_iter() // <-- just change that!
    // .map(|&i| i * i)
    // .sum()
    // }
}