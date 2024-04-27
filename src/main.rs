use std::{collections::BTreeMap, fs, path::PathBuf};

use clap::{Parser, Subcommand};

mod program;
mod metadata;
mod query_parser;
mod data_loader;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Optional name to operate on
    // name: Option<String>,

    // /// Sets a custom config file
    // #[arg(short, long, value_name = "FILE")]
    // config: Option<PathBuf>,

    // /// Turn debugging information on
    // #[arg(short, long, action = clap::ArgAction::Count)]
    // debug: u8,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    Init,
    Create {
        #[arg(short, long)]
        table: String,
        #[arg(short, long, value_name = "FILE")]
        config: PathBuf,
        #[arg(short, long)]
        file_path: String,
        #[arg(long)]
        format: String, // Make into Enum
    },
    Drop {
        #[arg(short, long)]
        table: String,
    },
    Query {
        #[arg(short, long)]
        query: String,
    },
}

fn main() {
    let cli = Cli::parse();

    let connection = sqlite::open(":memory:").unwrap();
    // let connection = sqlite::open("./database").unwrap();

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
            metadata::create_table(table, config, file_path, format);
        }
        Some(Commands::Drop { table }) => {
            metadata::drop_table(table);
        }
        Some(Commands::Query { query }) => {
            
            let tables = query_parser::get_tables_from_query(query);
            println!("{}", tables.join(","));

            let missing_tables = tables.iter().any(|table| {
                let table_path = metadata::get_path_for_table(&table);
                if !table_path.exists() {
                    println!("Table {} doesn't exist. Create it first with `wings create`", table);
                    return true;
                } else {
                    return false;
                }
            });

            if missing_tables {
                return;
            }

            let table_paths: Vec<PathBuf> = tables.iter().map(|table| {
                let table_path = metadata::get_path_for_table(&table);
                return table_path
            }).collect();

            println!("{:?}", table_paths[0].to_str());

            data_loader::load(&connection, table_paths);

            // Query
            println!("Running query...");
            connection
            .iterate(query, |pairs| {
                // Render logic
                let mut row = String::from("");
                for &(name, value) in pairs.iter() {
                    match value {
                        Some(wvalue) => row = row + wvalue + ", ",
                        None => row = row + "NULL, ",
                    }
                }
                println!("{}", row);
                true
            })
            .unwrap();
            println!("Done.");
        }
        None => {
            println!("Use --help for command details.")
        }
    }
}
