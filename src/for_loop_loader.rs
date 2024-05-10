use std::{collections::BTreeMap, path::PathBuf};

use crate::utils::read_path;

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
