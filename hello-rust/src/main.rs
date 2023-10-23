extern crate rusqlite;
extern crate prettytable;

use rusqlite::{Connection, NO_PARAMS};
use prettytable::{Table, Row, Cell};

fn print_table(data: Vec<Vec<String>) {
    let mut table = Table::new();
    if data.is_empty() {
        return;
    }
    let column_count = data[0].len();
    let field_names: Vec<Cell> = (0..column_count)
        .map(|i| Cell::new(&data[0][i]))
        .collect();
    table.set_titles(Row::new(field_names));

    for i in 1..data.len() {
        let cells: Vec<Cell> = (0..column_count)
            .map(|j| Cell::new(&data[i][j]))
            .collect();
        table.add_row(Row::new(cells));
    }

    table.printstd();
}

fn query_count_imecas() -> Result<(), rusqlite::Error> {
    let conn = Connection::open("ET/data/my_airDB.db")?;
    let mut stmt = conn.prepare("SELECT zona, COUNT(*) AS total FROM my_airDB GROUP BY zona")?;
    let rows: Vec<Vec<String>> = stmt
        .query_map(NO_PARAMS, |row| {
            Ok(vec![
                row.get(0)?, // assuming the first column is a string
                row.get(1)?.to_string(), // assuming the second column is an integer
            ])
        })?
        .map(|result| result.unwrap())
        .collect();

    println!("Zones in dataset:\n");
    print_table(rows);

    let mut stmt = conn.prepare("SELECT imecas, COUNT(*) AS total FROM my_airDB GROUP BY imecas")?;
    let rows: Vec<Vec<String>> = stmt
        .query_map(NO_PARAMS, |row| {
            Ok(vec![
                row.get(0)?, // assuming the first column is a string
                row.get(1)?.to_string(), // assuming the second column is an integer
            ])
        })?
        .map(|result| result.unwrap())
        .collect();

    println!("\n IMECAS in this dataset:\n");
    print_table(rows);

    let mut stmt = conn.prepare(
        "SELECT zona, imecas, COUNT(*) AS type_of_IMECAS_by_zone FROM my_airDB GROUP BY zona, imecas ORDER BY zona DESC",
    )?;
    let rows: Vec<Vec<String>> = stmt
        .query_map(NO_PARAMS, |row| {
            Ok(vec![
                row.get(0)?, // assuming the first column is a string
                row.get(1)?, // assuming the second column is a string
                row.get(2)?.to_string(), // assuming the third column is an integer
            ])
        })?
        .map(|result| result.unwrap())
        .collect();

    println!("\n IMECAS per zone in this dataset:\n");
    print_table(rows);

    Ok(())
}

fn main() -> Result<(), rusqlite::Error> {
    // Query
    println!("Querying data...");
    query_count_imecas()
}
