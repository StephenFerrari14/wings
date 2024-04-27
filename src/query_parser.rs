
pub fn get_tables_from_query(query: &String) -> Vec<String> {
  // Parse query, get tables, load config, load table, run query
  // Does not work for multiple tables yet
  let mut clauses: Vec<&str> = query.split("from").collect();
  clauses.remove(0); // Not performant
  let mut tables: Vec<String> = vec![];
  for clause in clauses {
      let words: Vec<&str> = clause.split(" ").collect();
      let table = words[1].to_string();
      tables.push(table)
  }
  return tables;
}

#[cfg(test)]
mod tests {
    use crate::query_parser::get_tables_from_query;

    #[test]
    fn got_table_from_query() {
        let query = String::from("select * from test_table where id = 1");
        let result = vec![String::from("test_table")];
        assert_eq!(get_tables_from_query(&query), result);
    }
}