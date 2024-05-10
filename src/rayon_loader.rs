use std::{collections::BTreeMap, path::PathBuf, time::SystemTime};

use rayon::prelude::*;
use sqlite::{Connection, Value};
use walkdir::WalkDir;

use crate::{
    metadata,
    utils::{read_path, schema_to_db},
};

// verify return types
pub fn load(
    files: Vec<PathBuf>,
    format: String,
) -> Vec<Vec<BTreeMap<String, String>>> {
    let rayon_rows: Vec<Vec<BTreeMap<String, String>>> = files
        .par_iter()
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
    return rayon_rows;
}

pub fn load2(connection: &Connection, tables: Vec<PathBuf>) {
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

        let file_now = SystemTime::now();
        let files: Vec<PathBuf> = WalkDir::new(data_path)
            .into_iter()
            .map(|f| return f.unwrap().path().to_path_buf())
            .collect();

        //// This is whats different
        let rayon_rows: Vec<Vec<std::collections::BTreeMap<String, String>>> = files
            .par_iter()
            .map(|entry| {
                let raw_rows = read_path(entry.as_path(), &format);
                match raw_rows {
                    Ok(ok_rows) => {
                        return ok_rows;
                    }
                    Err(_) => panic!("Cannot read data rows"),
                }
            })
            .collect();
        ////
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

        let insert_now = SystemTime::now();
        for rows in rayon_rows {
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
                println!("Error: {e:?}");
            }
        }
    }
}
