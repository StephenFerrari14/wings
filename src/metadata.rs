use std::{collections::BTreeMap, fmt::format, fs, path::PathBuf};

use serde::{Serialize, Deserialize};

pub fn get_path_for_table(table: &String) -> PathBuf{
  let homedir = dirs::home_dir().unwrap_or_else(|| {
      panic!("Cannot find home directory, create home directory to continue.")
  });
  let mut table_path = homedir.clone();
  table_path.push(".wings");
  table_path.push("tables");
  table_path.push(table.to_owned() + ".yaml");
  return table_path
}

pub fn create_table(table: &String, config: &PathBuf, file_path: &String, format: &String) {
    // Add table and config to dir
    // Strcuture
    // ~/.wings/tables/$tableName.toml
    let table_path = get_path_for_table(table);

    if table_path.exists() {
        println!("Table {} already exists. Drop and create to update.", table)
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
        let schema_map: BTreeMap<String, String> = serde_yaml::from_str(&contents).unwrap();
        map.insert("schema".to_string(), schema_map);
        let yaml = serde_yaml::to_string(&map).unwrap();
        let _ = fs::write(table_path, yaml);
        println!("Table {} created", table)
    }
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

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Metadata {
  pub name: String,
  pub format: String,
  pub data_path: PathBuf
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct TableMetadata {
  pub metadata: Metadata,
  pub schema: BTreeMap<String, String>
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

pub fn get_table_metadata(table_path: PathBuf) -> TableMetadata {
  let contents = fs::read_to_string(table_path)
        .expect("Should have been able to read the file");

  let table_metadata: TableMetadata = serde_yaml::from_str(&contents).unwrap();
  return table_metadata;
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, path::{Path, PathBuf}};

    use crate::metadata::TableMetadata;

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
        schema
      };

      let expected_result = "id int null\nname text null".to_string();
      assert_eq!(table_metadata.table_definition(), expected_result)
    }
}
