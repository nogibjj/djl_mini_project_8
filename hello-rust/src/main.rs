use std::error::Error;
use csv::Reader;
use rusqlite::{params, Connection};
use prettytable::{Table, Row, Cell};
use reqwest;

fn extract() -> Result<String, Box<dyn Error>> {
    let url = "https://raw.githubusercontent.com/jjsantos01/aire_cdmx/master/datos/contaminantes_2019-05-16.csv";
    let file_path = "ET/data/my_air_cont2.csv";

    let response = reqwest::blocking::get(url)?.bytes()?;
    println!("Response length: {}", response.len());
    std::fs::write(file_path, &response)?;
    println!("File written to: {}", file_path);

    Ok(file_path.to_string())
}

fn load() -> Result<String, Box<dyn Error>> {
    let dataset = "ET/data/my_air_cont2.csv";
    let conn = Connection::open("ET/data/my_airDB2.db")?;

    conn.execute("DROP TABLE IF EXISTS my_airDB2", params![])?;
    conn.execute(
        "CREATE TABLE my_airDB2 (Fecha TEXT, Hora TEXT, ZP TEXT, imecas TEXT, zona TEXT, contaminante TEXT, color TEXT)",
        params![],
    )?;

    let mut stmt = conn.prepare("INSERT INTO my_airDB2 (Fecha, Hora, ZP, imecas, zona, contaminante, color) VALUES (?, ?, ?, ?, ?, ?, ?)")?;

    let file = std::fs::File::open(dataset)?;
    let mut rdr = Reader::from_reader(file);

    for result in rdr.records() {
        let record = result?;
        stmt.execute(params![
            &record[0],
            &record[1],
            &record[2],
            &record[3],
            &record[4],
            &record[5],
            &record[6],
        ])?;
    }

    Ok("my_airDB.db2".to_string())
}

fn print_table(data: &Vec<Vec<String>>) {
    let mut table = Table::new();
    for row in data.iter() {
        let cells: Vec<Cell> = row.iter().map(|value| Cell::new(value)).collect();
        table.add_row(Row::new(cells));
    }
    table.printstd();
}

fn query_count_imecas() -> Result<String, Box<dyn Error>> {
    let conn = Connection::open("ET/data/my_airDB2.db")?;
    let mut stmt = conn.prepare(
        "SELECT zona, COUNT(*) AS total FROM my_airDB2 GROUP BY zona; \
        SELECT imecas, COUNT(*) AS total FROM my_airDB2 GROUP BY imecas; \
        SELECT zona, imecas, COUNT(*) AS type_of_IMECAS_by_zone FROM my_airDB2 GROUP BY zona, imecas ORDER BY zona DESC;",
    )?;

    let mut data: Vec<Vec<String>> = Vec::new();

    while let Some(row) = stmt.query(params![])?.next()? {
        let mut row_data = Vec::new();
        for i in 0..row.column_count() {
            let value = match row.get(i)? {
                rusqlite::types::Value::Text(text) => text.to_string(),
                rusqlite::types::Value::Integer(int) => int.to_string(),
                rusqlite::types::Value::Real(float) => float.to_string(),
                _ => "N/A".to_string(),
            };
            row_data.push(value);
        }
        data.push(row_data);
    }

    print_table(&data);

    Ok("Success".to_string())
}
   

fn main() -> Result<(), Box<dyn Error>> {
    // Extract data
    println!("Extracting data...");
    extract()?;

    // Transform and load
    println!("Transforming data...");
    load()?;

    // Query
    println!("Querying data...");
    query_count_imecas()?;

    Ok(())
}