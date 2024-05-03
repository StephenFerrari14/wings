use std::fs;

use dirs::home_dir;

pub fn init() {
  let homedir = home_dir().unwrap_or_else(|| {
      panic!("Cannot find home directory, create home directory to continue.")
  });
  let mut wings_dir = homedir.clone();
  wings_dir.push(".wings");
  wings_dir.push("tables");
  if !wings_dir.exists() {
      fs::create_dir_all(wings_dir)
          .unwrap_or_else(|err| panic!("Error creating config, {}", err));
  }
}

pub fn does_program_directory_exist() -> bool {
  let homedir = home_dir().unwrap_or_else(|| {
    panic!("Cannot find home directory, create home directory to continue.")
  });
  let mut wings_dir = homedir.clone();
  wings_dir.push(".wings");
  wings_dir.exists()
}