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

Build
```
cargo build
```

Test
```
cargo test
```

## TODO
- Add descriptions to commands
- Fix unwraps
- Format as enum
- Remove --query from query command
- Add query result rendering
- Performance timers and logging
- Test for large amount of large files
- Try different performance improvements
- Tests
- Query parser for multiple tables
- Wild card in data path
- Loader for avro/parquet/json
- Auto detect schema
- Schema types validation
- Maybe an option to put all data not in schema into json column
-  env var for testing config
