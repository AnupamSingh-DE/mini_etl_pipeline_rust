use std::error::Error;
use std::fs::File;

use csv::ReaderBuilder;
use rusqlite::{params, Connection};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Record {
    name: String,
    age: String,
    city: String,
}

fn main() -> Result<(), Box<dyn Error>> {
    // Extract: Read CSV file
    let file = File::open("data/input.csv")?;
    let mut rdr = ReaderBuilder::new().has_headers(true).from_reader(file);

    // Transform: Clean and prepare data
    let mut records: Vec<Record> = Vec::new();
    for result in rdr.deserialize() {
        let record: Record = result?;
        if record.name.trim().is_empty() || record.age.trim().is_empty() || record.city.trim().is_empty() {
            continue;
        }
        records.push(Record {
            name: capitalize(&record.name),
            age: record.age.trim().to_string(),
            city: record.city.trim().to_string(),
        });
    }

    // Load: Insert into SQLite database
    let conn = Connection::open("data/people.db")?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS people (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            age INTEGER NOT NULL,
            city TEXT NOT NULL
        )",
        [],
    )?;

    for record in records {
        conn.execute(
            "INSERT INTO people (name, age, city) VALUES (?1, ?2, ?3)",
            params![record.name, record.age.parse::<i32>()?, record.city],
        )?;
    }

    println!("✅ ETL completed: data loaded into data/people.db");
    Ok(())
}

fn capitalize(input: &str) -> String {
    let mut chars = input.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
        None => String::new(),
    }
}
