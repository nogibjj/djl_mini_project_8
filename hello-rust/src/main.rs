extern crate rusqlite;
extern crate prettytable;

use rusqlite::{Connection, NO_PARAMS};
use prettytable::{Table, Row, Cell};

fn query_count_imecas() -> Result<(), rusqlite::Error> {
    let conn = Connection::open("ET/data/my_airDB.db")?;
    let mut stmt = conn.prepare("SELECT zona, COUNT(*) AS total FROM my_airDB GROUP BY zona")?;
    let rows: Vec<Vec<String>> = stmt.query_map(NO_PARAMS, |row| {
        Ok(vec![
            row.get(0)?, // assuming the first column is a string
            row.get(1)?, // assuming the second column is a count (integer)
        ])
    })?;

    println!("Zones in dataset:\n");
    print_table(&stmt, rows);

    let mut stmt = conn.prepare("SELECT imecas, COUNT(*) AS total FROM my_airDB GROUP BY imecas")?;
    let rows: Vec<Vec<String>> = stmt.query_map(NO_PARAMS, |row| {
        Ok(vec![
            row.get(0)?, // assuming the first column is a string
            row.get(1)?, // assuming the second column is a count (integer)
        ])
    })?;

    println!("\n IMECAS in this dataset:\n");
    print_table(&stmt, rows);

    let mut stmt = conn.prepare(
        "SELECT zona, imecas, COUNT(*) AS type_of_IMECAS_by_zone FROM my_airDB GROUP BY zona, imecas ORDER BY zona DESC",
    )?;
    let rows: Vec<Vec<String>> = stmt.query_map(NO_PARAMS, |row| {
        Ok(vec![
            row.get(0)?, // assuming the first column is a string (zona)
            row.get(1)?, // assuming the second column is a string (imecas)
            row.get(2)?, // assuming the third column is a count (integer)
        ])
    })?;

    println!("\n IMECAS per zone in this dataset:\n");
    print_table(&stmt, rows);

    Ok(())
}

fn print_table(_stmt: &rusqlite::Statement, data: Vec<Vec<String>>) {
    let mut table = Table::new();
    for row in data.iter() {
        let cells: Vec<Cell> = row.iter().map(|value| Cell::new(value)).collect();
        table.add_row(Row::new(cells));
    }
    table.printstd();
}

fn main() -> Result<(), rusqlite::Error> {
    // Query
    println!("Querying data...");
    query_count_imecas()
}
