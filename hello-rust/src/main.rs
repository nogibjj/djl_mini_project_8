use std::error::Error;
use csv::Reader;
use rusqlite::{params, Connection};
use std::env;
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



fn main() -> Result<(), Box<dyn Error>> {
    // Extract data
    println!("Extracting data...");
    extract()?;

    // Transform and load
    println!("Transforming data...");
    load()?;

   

    Ok(())
}

