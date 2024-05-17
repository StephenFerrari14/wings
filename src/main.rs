use std::{collections::HashMap, path::PathBuf, time::SystemTime};

use clap::{Parser, Subcommand};
use sqlite::Error;

use crate::metadata::{get_metadata_for_display, render_tables};

mod data_loader;
mod metadata;
mod program;
mod query_parser;
mod display_row;
mod rayon_loader;
mod utils;
mod for_loop_loader;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize program
    Init,
    /// Create table to query
    Create {
        /// Name of table
        #[arg(short, long)]
        table: String,
        /// Path to table schema yaml
        #[arg(short, long, value_name = "FILE")]
        config: PathBuf,
        /// Path to search for data
        #[arg(short, long)]
        file_path: String,
        /// Format of files to load
        #[arg(long)]
        format: String, // Make into Enum
    },
    /// Drop given table
    Drop {
        /// Table to drop
        // #[arg(short, long)]
        table: String,
    },
    /// Query data using wings
    Query {
        /// Query to execute
        // #[arg(short, long)]
        query: String,
    },
    /// Show information about objects
    Show {
        #[command(subcommand)]
        command: Option<ShowCommands>,
    }
}

#[derive(Debug, Subcommand)]
enum ShowCommands {
    /// Show all tables created by wings
    Tables,
    /// Show information for a given table
    Table { 
        name: String 
    },
}

fn main() {
    let cli = Cli::parse();
    simple_logger::SimpleLogger::new().env().init().unwrap();

    let not_initialized_message = "Wings not initialized, run `wings init` first.";

    match &cli.command {
        Some(Commands::Init) => {
            program::init();
            println!("Init complete.")
        }
        Some(Commands::Create {
            table,
            config,
            file_path,
            format,
        }) => {
            if program::does_program_directory_exist() {
                let _ = metadata::create_table(table, config, file_path, format);
            } else {
                println!("{}", not_initialized_message)
            }
        }
        Some(Commands::Drop { table }) => {
            if program::does_program_directory_exist() {
                metadata::drop_table(table);
            } else {
                println!("{}", not_initialized_message)
            }
        }
        Some(Commands::Query { query }) => {
            if program::does_program_directory_exist() {
                let now = SystemTime::now();
                let _ = run_query(query);
                match now.elapsed() {
                    Ok(elapsed) => {
                        println!("Query ran in {}ms", elapsed.as_millis());
                    }
                    Err(e) => {
                        // an error occurred!
                        println!("Error: {e:?}");
                    }
                }
            } else {
                println!("{}", not_initialized_message)
            }
        }
        Some(Commands::Show { command }) => {
            match command {
                Some(table) => {
                    match table {
                        ShowCommands::Tables => {
                            render_tables();
                        },
                        ShowCommands::Table { name } => {
                            match get_metadata_for_display(name) {
                                Ok(metadata) => println!("{}", metadata),
                                Err(error) => println!("{}", error),
                            }
                        },
                    }
                },
                None => todo!(),
            }
        }
        None => {
            println!("Use --help for command details.")
        }
    }
}

fn run_query(query: &String) -> Result<(), Error> {
    println!("Running query...");
    let tables = query_parser::get_tables_from_query(query);

    let missing_tables = tables.iter().any(|table| {
        let table_path = metadata::get_path_for_table(&table);
        if !table_path.exists() {
            println!(
                "Table {} doesn't exist. Create it first with `wings create`",
                table
            );
            return true;
        } else {
            return false;
        }
    });

    if missing_tables {
        return Ok(());
    }

    let table_paths: Vec<PathBuf> = tables
        .iter()
        .map(|table| {
            let table_path = metadata::get_path_for_table(&table);
            return table_path;
        })
        .collect();

    let connection = sqlite::open(":memory:")?;
    // let connection = sqlite::open("./database").unwrap();
    data_loader::load(&connection, table_paths);

    // Query
    let mut rows: Vec<HashMap<String, String>> = Vec::new();
    connection
        .iterate(query, |pairs| {
            // Render logic
            let mut row: HashMap<String, String> = HashMap::new();
            for &(name, value) in pairs.iter() {
                match value {
                    Some(wvalue) => row.insert(name.to_string(), wvalue.to_string()),
                    None => row.insert(name.to_string(), "NULL".to_string()),
                };
            }
            rows.push(row);
            true
        })?;
    let display_rows = display_row::display_rows_from_maps(rows);
    display_row::render(display_rows);
    Ok(())
}
