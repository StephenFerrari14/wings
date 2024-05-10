use std::{collections::BTreeMap, path::PathBuf, time::SystemTime};

use sqlite::{Connection, Value};
use walkdir::WalkDir;

use crate::rayon_loader;
use crate::{for_loop_loader, metadata};
use crate::{metadata::TableMetadata, utils::schema_to_db};

enum RunMode {
    #[allow(dead_code)]
    ForLoop,
    Rayon,
}

pub fn load(connection: &Connection, tables: Vec<PathBuf>) {
    let mode = RunMode::Rayon;

    for table in tables {
        let now = SystemTime::now();
        let table_metadata = metadata::get_table_metadata(table).unwrap();
        //   Get table schema
        //   Create table in sqlite
        create_table_from_metadata(connection, &table_metadata);

        //   Get data_path and format
        //   For all files in data_path with format (parallelize)
        let data_path = table_metadata.metadata.data_path.clone();
        let format = table_metadata.metadata.format.clone();

        // Either specify fields in insert or make sure values are in the right order
        let columns: Vec<String> = table_metadata.schema.keys().cloned().collect();
        let values: Vec<String> = columns.iter().map(|f| format!(":{}", f)).collect();

        let load_now = SystemTime::now();
        let files: Vec<PathBuf> = WalkDir::new(data_path)
            .into_iter()
            .map(|f| return f.unwrap().path().to_path_buf())
            .collect();

        #[allow(unused_assignments)]
        let mut rows = Vec::new();
        match mode {
            RunMode::ForLoop => {
                rows = for_loop_loader::load(files, format);
            }
            RunMode::Rayon => {
                rows = rayon_loader::load(files, format);
            }
        }

        match load_now.elapsed() {
            Ok(elapsed) => {
                println!(
                    "run_mode::load for {} in {}ms",
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

        // Put flatten in loaders
        let flatten_now = SystemTime::now();
        let flat_rows = flatten(rows);
        match flatten_now.elapsed() {
            Ok(elapsed) => {
                println!(
                    "flatten for {} in {}ms",
                    table_metadata.metadata.name,
                    elapsed.as_millis()
                );
            }
            Err(e) => {
                // an error occurred!
                println!("Error: {e:?}");
            }
        }

        // Do this in batches instead of single inserts
        load_db(
            connection,
            &table_metadata.metadata.name,
            columns,
            values,
            flat_rows,
        );

        match now.elapsed() {
            Ok(elapsed) => {
                println!(
                    "data_loader::load for {} in {}ms",
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

pub fn create_table_from_metadata(connection: &Connection, table_metadata: &TableMetadata) {
    let create_table_sql = schema_to_db(&table_metadata);
    connection.execute(create_table_sql).unwrap();
}

pub fn flatten(rows: Vec<Vec<BTreeMap<String, String>>>) -> Vec<BTreeMap<String, String>> {
    let mut flat_rows = Vec::new();
    for row in rows {
        for part in row {
            flat_rows.push(part);
        }
    }
    return flat_rows;
}

pub fn load_db(
    connection: &Connection,
    table_name: &String,
    columns: Vec<String>,
    values: Vec<String>,
    rows: Vec<BTreeMap<String, String>>,
) {
    let insert_now = SystemTime::now();
    let columns_clause = columns.join(",");
    let values_clause = values.join(",");
    let query = format!(
        "INSERT INTO {} ({}) VALUES ({})",
        table_name, columns_clause, values_clause
    );

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
            println!("load_db for {} in {}ms", table_name, elapsed.as_millis());
        }
        Err(e) => {
            // an error occurred!
            println!("Error: {e:?}");
        }
    }
}

#[allow(dead_code)]
fn project(
    row: BTreeMap<String, String>,
    #[allow(unused_variables)] schema: BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    // TODO
    return row;
}
