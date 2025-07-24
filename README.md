# Wings
Query tool for files with on the fly data loading written in Rust. Inspired by [Dremel](https://research.google/pubs/dremel-interactive-analysis-of-web-scale-datasets-2/).

```
Commands:
  init    Initialize program
  create  Create table to query
  drop    Drop given table
  query   Query data using wings
  show    Show information about objects
  help    Print this message or the help of the given subcommand(s)
```

See `examples/schema.yaml` for example schema configuration.

Supported formats
- csv
- json
- avro

## Install
Install Rust

### Build
```
cd ~/
git clone git@github.com:StephenFerrari14/wings.git
cargo build --release
export PATH=~/wings/target/release/:$PATH
```

## Development
Run
```
cargo run -- <command>
```
Ex.  
CSV  
```
cargo run -- create --table test_table --config examples/schema.yaml --file-path ./examples/data/ --format csv

cargo run -- query "select * from test_table"
```
Results
```
Running query...
+----------------------------------------------------------------------------------+
|col2      |id|end |created_at         |col5      |col1      |col4      |col3      |
+----------------------------------------------------------------------------------+
|gegfcecdfg|79|NULL|2024-04-29 22:45:02|fgdebgcace|fefgebcdca|aefecagbga|ddaacgdaff|
|bfgbadadca|53|NULL|2024-04-29 22:45:02|fgdgdfdaac|bcaeegfdeg|gbbccecaag|adbdgbfggd|
+----------------------------------------------------------------------------------+
Query ran in 3ms
```

AVRO
```
cargo run -- create --table avro_table --config examples/avro_schema.yaml --file-path ./examples/avro_data/ --format avro

cargo run -- query "select * from avro_table"
```
Results
```
Running query...
+---------------------------------------------------------+
|username  |timestamp |tweet                              |
+---------------------------------------------------------+
|miguno    |1366150681|Rock: Nerf paper, scissors is fine.|
|BlizzardCS|1366154481|Works as intended.  Terran is IMBA.|
+---------------------------------------------------------+
Query ran in 5ms
```

JSON

For the json format each json file is processed as a single record. `json_array` and `json_path` format can be used to query multiple records in a single file.

```
cargo run -- create -t json_table -c ./examples/schema.yaml -f examples/json_data/ --format json

cargo run -- query "select * from json_table"
```

Build
```
cargo build
```

Test
```
cargo test
```

## Build for windows
```
rustup target add x86_64-pc-windows-gnu
brew install mingw-w64
cargo build --release --target x86_64-pc-windows-gnu
```

## TODO
- Tests
- Format as enum
- Implement projections function
- Add json arrays format
- Add json path format
- Put flatten in loaders
- Insert to database optimization
- Query optimization (counts, limits, etc)
- Add parquet format
- Fix unwraps
- Wild card in data path
- Auto detect schema
- Schema types validation
- Maybe an option to put all data not in schema into json column
- Show table definition spacing
- env var for testing config
- Query parser for multiple tables
- Add logger
