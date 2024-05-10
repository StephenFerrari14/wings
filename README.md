# Wings
Query tool for files with on the fly data loading written in Rust. Inspired by Dremel.

```
Commands:
  init    
  create  
  drop    
  query   
  help    Print this message or the help of the given subcommand(s)
```

## Development
Run
```
cargo run -- <command>
```
Ex.
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
Query ran in 13ms
```

Build
```
cargo build
```

Test
```
cargo test
```

## TODO
- Tests
- Packager
- Loader for avro/parquet
- Converter and Loader for json
- Projection function
- Put flatten in loaders
- Insert to database optimization
- Query optimization (counts, limits, etc)
- Implement projections function
- Fix unwraps
- Format as enum
- Wild card in data path
- Auto detect schema
- Schema types validation
- Maybe an option to put all data not in schema into json column
- Show table definition spacing
- env var for testing config
- Query parser for multiple tables
- Add logger
