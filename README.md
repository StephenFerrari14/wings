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

Build
```
cargo build
```

Test
```
cargo test
```

## TODO
- Clean up
- Show tables
- Show table definition
- Tests
- Insert to database optimization
- Query optimization (counts, limits, etc)
- Fix unwraps
- Format as enum
- Wild card in data path
- Loader for avro/parquet
- Converter and Loader for json
- Auto detect schema
- Schema types validation
- Maybe an option to put all data not in schema into json column
- env var for testing config
- Query parser for multiple tables
- Add logger
