use std::{collections::BTreeMap, fs, io::{Error, ErrorKind}, path::PathBuf};

use serde::{Deserialize, Serialize};
use serde_yaml::Error as SerdeError;
use walkdir::WalkDir;

pub fn get_path_for_table(table: &String) -> PathBuf {
    let homedir = dirs::home_dir().unwrap_or_else(|| {
        panic!("Cannot find home directory, create home directory to continue.")
    });
    let mut table_path = homedir.clone();
    table_path.push(".wings");
    table_path.push("tables");
    table_path.push(table.to_owned() + ".yaml");
    return table_path;
}

pub fn get_tables() -> Vec<String> {
    let homedir = dirs::home_dir().unwrap_or_else(|| {
        panic!("Cannot find home directory, create home directory to continue.")
    });
    let mut table_path = homedir.clone();
    table_path.push(".wings");
    table_path.push("tables");

    let tables: Vec<String> = WalkDir::new(table_path)
        .into_iter()
        .map(|f| match f {
            Ok(entry) => {
              if entry.path().is_file() {
                entry
                  .file_name()
                  .to_str()
                  .unwrap()
                  .to_string()
                  .replace(".yaml", "")
              } else {
                String::from(".")
              }
              },
            Err(_) => todo!(),
        })
        .collect();
    return tables.into_iter().filter(|t| *t != String::from(".")).collect();
}

pub fn create_table(
    table: &String,
    config: &PathBuf,
    file_path: &String,
    format: &String,
) -> Result<(), SerdeError> {
    // Add table and config to dir
    // Strcuture
    // ~/.wings/tables/$tableName.toml
    let table_path = get_path_for_table(table);

    if table_path.exists() {
        println!("Table {} already exists. Drop and create to update.", table);
    } else {
        // Create
        let mut map: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
        let mut meta_map: BTreeMap<String, String> = BTreeMap::new();
        // name
        meta_map.insert("name".to_string(), table.to_string());
        // format
        meta_map.insert("format".to_string(), format.to_string());
        // data_path
        meta_map.insert("data_path".to_string(), file_path.to_string());
        map.insert("metadata".to_string(), meta_map);
        // schema
        let contents = fs::read_to_string(config).unwrap();
        let schema_map: BTreeMap<String, String> = serde_yaml::from_str(&contents)?;
        map.insert("schema".to_string(), schema_map);
        let yaml = serde_yaml::to_string(&map)?;
        let _ = fs::write(table_path, yaml);
        println!("Table {} created", table);
    }
    Ok(())
}

pub fn drop_table(table: &String) {
    let table_path = get_path_for_table(table);
    if table_path.exists() {
        let _ = fs::remove_file(table_path);
        println!("Table {} dropped", table)
    } else {
        println!("Table does not exist to drop.")
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
enum Format {
    Csv,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Metadata {
    pub name: String,
    pub format: String,
    pub data_path: PathBuf,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct TableMetadata {
    pub metadata: Metadata,
    pub schema: BTreeMap<String, String>,
}

impl TableMetadata {
    pub fn table_definition(&self) -> String {
        // Turn each entry in schema into table definition
        let mut definition = String::from("");
        for (key, value) in self.schema.iter() {
            definition = definition + format!("{} {} NULL,\n", key, value).as_str();
        }
        definition = definition + "end int NULL"; // Fix
        return definition;
    }
}

pub fn get_table_metadata(table_path: PathBuf) -> Result<TableMetadata, SerdeError> {
    let contents = fs::read_to_string(table_path).expect("Should have been able to read the file");

    let table_metadata: TableMetadata = serde_yaml::from_str(&contents)?;
    return Ok(table_metadata);
}

pub fn create_table_render(table_metadata: TableMetadata) -> String {
  let columns: Vec<String> = table_metadata.schema.iter().map(|c| {
    format!("{}, {}", c.0, c.1)
  }).collect();
  let column_render = columns.join("\n");

  return "Name: ".to_owned() + &table_metadata.metadata.name + 
  "\n--------------------\nMetadata\n" +
  "Path: " + table_metadata.metadata.data_path.to_str().unwrap() + "\n" +
  "Format: " + &table_metadata.metadata.format +
  "\n--------------------\nColumns\n" +
  &column_render;
}

pub fn get_metadata_for_display(name: &String) -> Result<String, Error> {
    let path = get_path_for_table(name);
    if path.exists() {
        let metadata_res = get_table_metadata(path);
        match metadata_res {
            Ok(table_metadata) => {
                let render = create_table_render(table_metadata);
                return Ok(render);
            },
            Err(err) => panic!("{}", err),
        }
    } else {
        let error = Error::new(ErrorKind::InvalidInput, format!("Can't find table {}", name));
        Err(error)
    }
}

pub fn render_tables() {
    println!("Tables:");
    let tables: Vec<String> = get_tables();
    if tables.len() > 0 {
        for table in tables {
            println!("{}", table);
        }
    } else {
        println!("No tables found");
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        path::{Path, PathBuf},
    };

    use crate::metadata::{Format, TableMetadata};

    use super::Metadata;

    #[test]
    fn table_metadata_definition() {
        let mut schema: BTreeMap<String, String> = BTreeMap::new();
        schema.insert("id".to_string(), "int".to_string());
        schema.insert("name".to_string(), "text".to_string());
        let mut metadata: BTreeMap<String, String> = BTreeMap::new();
        let table_metadata = TableMetadata {
            metadata: Metadata {
                name: "test".to_string(),
                format: "csv".to_string(),
                data_path: PathBuf::new(),
            },
            schema,
        };

        let expected_result = "id int null\nname text null".to_string();
        assert_eq!(table_metadata.table_definition(), expected_result)
    }
}
