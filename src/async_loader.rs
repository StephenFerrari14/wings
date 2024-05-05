use sqlite::{Connection, Value};
use std::{collections::BTreeMap, path::PathBuf, time::SystemTime};
use walkdir::{DirEntry, WalkDir};

use crate::{
    metadata,
    utils::{read, read_async, schema_to_db},
};

struct LoadEntry {
    entry: DirEntry,
    format: String,
}

pub async fn load(connection: &Connection, tables: Vec<PathBuf>) {
    println!("Async load");
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
        let format: String = table_metadata.metadata.format.clone();

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

        // Put timer around this
        let file_now = SystemTime::now();
        let load_entries: Vec<LoadEntry> = WalkDir::new(data_path)
            .into_iter()
            .map(|entry| LoadEntry {
                entry: entry.unwrap(),
                format: format.clone(),
            })
            .collect();
        // let file_now = SystemTime::now();
        let tasks: Vec<
            tokio::task::JoinHandle<Result<Vec<BTreeMap<String, String>>, std::io::Error>>,
        > = load_entries
            .into_iter()
            .map(|load_entry| {
                let handle = tokio::spawn(async {
                    let raw_rows = read_async(load_entry.entry, load_entry.format);
                    raw_rows
                });
                return handle;
            })
            .collect();

        let mut rows: Vec<BTreeMap<String, String>> = Vec::new();
        for task in tasks {
            let a = task.await.unwrap();
            match a {
                Ok(row) => {
                    let mut a_row = row.clone();
                    rows.append(&mut a_row)
                }
                Err(_) => println!("Skipping row"),
            }
        }

        match file_now.elapsed() {
            Ok(elapsed) => {
                println!(
                    "Files read for {} in {}ms",
                    table_metadata.metadata.name,
                    elapsed.as_millis()
                );
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
                println!(
                    "Data insert for {} in {}ms",
                    table_metadata.metadata.name,
                    elapsed.as_millis()
                );
            }
            Err(e) => {
                // an error occurred!
                println!("Error: {e:?}");
            }
        }
        match now.elapsed() {
            Ok(elapsed) => {
                println!(
                    "File load for {} in {}ms",
                    table_metadata.metadata.name,
                    elapsed.as_millis()
                );
            }
            Err(e) => {
                // an error occurred!
                println!("Error: {e:?}");
            }
        }
    }
}
