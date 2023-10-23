use std::error::Error;
use csv::Reader;
use rusqlite::{Connection, params, NO_PARAMS};
use prettytable::{Table, Row, Cell};
use reqwest;

fn extract() -> Result<String, Box<dyn Error>> {
    let url = "https://raw.githubusercontent.com/jjsantos01/aire_cdmx/master/datos/contaminantes_2019-05-16.csv";
    let file_path = "ET/data/my_air_cont.csv";

    let response = reqwest::blocking::get(url)?.bytes()?;
    std::fs::write(file_path, &response)?;

    Ok(file_path.to_string())
}

fn load() -> Result<String, Box<dyn Error>> {
    let dataset = "ET/data/my_air_cont.csv";
    let conn = Connection::open("ET/data/my_airDB.db")?;

    conn.execute("DROP TABLE IF EXISTS my_airDB", params![])?;
    conn.execute(
        "CREATE TABLE my_airDB (Fecha TEXT, Hora TEXT, ZP TEXT, imecas TEXT, zona TEXT, contaminante TEXT, color TEXT)",
        params![],
    )?;

    let mut stmt = conn.prepare("INSERT INTO my_airDB (Fecha, Hora, ZP, imecas, zona, contaminante, color) VALUES (?, ?, ?, ?, ?, ?, ?)")?;

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

    Ok("my_airDB.db".to_string())
}

fn print_table(data: Vec<Vec<String>>) {
    let mut table = Table::new();
    for row in data.iter() {
        let cells: Vec<Cell> = row.iter().map(|value| Cell::new(value)).collect();
        table.add_row(Row::new(cells));
    }
    table.printstd();
}

fn query_count_imecas() -> Result<String, Box<dyn Error>> {
    let conn = Connection::open("ET/data/my_airDB.db")?;
    let mut stmt = conn.prepare(
        "SELECT zona, COUNT(*) AS total FROM my_airDB GROUP BY zona; \
        SELECT imecas, COUNT(*) AS total FROM my_airDB GROUP BY imecas; \
        SELECT zona, imecas, COUNT(*) AS type_of_IMECAS_by_zone FROM my_airDB GROUP BY zona, imecas ORDER BY zona DESC;",
    )?;

    let mut data: Vec<Vec<String>> = Vec::new();

    while let Some(row) = stmt.query(NO_PARAMS)?.next()? {
        let row_data: Vec<String> = (0..row.column_count())
            .map(|i| match row.get_checked::<usize, String>(i) {
                Ok(value) => value,
                Err(_) => "N/A".to_string(),
            })
            .collect();

        data.push(row_data);
    }

    print_table(data);

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
     println!("Query...");
     query_count_imecas()?;
   

    Ok(())
}

