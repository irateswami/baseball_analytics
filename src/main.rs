use csv::Error;
use serde::Deserialize;
use chrono::prelude::*;
use rusqlite::{params, Connection, Result};

#[derive(Debug, Deserialize)]
struct CSVBatter {
    //"Name","HR","R","RBI","SB","AVG","playerid"
    Name: String,
    HR: u64,
    R: u64,
    RBI: u64,
    SB: u64,
    AVG: f64,
    playerid: String,
}

#[derive(Debug, PartialEq, PartialOrd)]
struct Batter {
    name: String,
    id: String,
    score: f64,
}

#[derive(Debug)]
struct Means {
    hr: f64,
    r: f64,
    rbi: f64,
    sb: f64,
    avg: f64,
}

struct StdDevs {
    hr: f64,
    r: f64,
    rbi: f64,
    sb: f64,
    avg: f64,
}

struct ZScore {
    hr: f64,
    r: f64,
    rbi: f64,
    sb: f64,
    avg: f64,
}
 
fn make_zs(batter: &CSVBatter, stddev: &StdDevs, means: &Means)  -> Batter {

    let z_hr = (batter.HR as f64 - means.hr)/stddev.hr;
    let z_r = (batter.R as f64 - means.r)/stddev.r;
    let z_rbi =  (batter.RBI as f64 - means.rbi)/stddev.rbi;
    let z_sb = (batter.SB as f64 - means.sb)/stddev.sb;
    let z_avg = (batter.AVG - means.avg)/stddev.avg;

    let score = z_hr + z_r + z_rbi + z_sb + z_avg;

    Batter {
        name: batter.Name.to_owned(),
        id: batter.playerid.to_owned(),
        score
    }

}

fn get_std_devs(a: &[CSVBatter]) -> StdDevs {
    StdDevs {
        hr: stats::stddev(a.iter().map(|s| s.HR)),
        r: stats::stddev(a.iter().map(|s| s.R)),
        rbi: stats::stddev(a.iter().map(|s| s.RBI)),
        sb: stats::stddev(a.iter().map(|s| s.SB)),
        avg: stats::stddev(a.iter().map(|s| s.AVG)),
    }
}

fn get_means(a: &[CSVBatter]) -> Means {
    let total_hr: u64 = a.iter().map(|s| s.HR).sum();
    let total_r: u64 = a.iter().map(|s| s.R).sum();
    let total_rbi: u64 = a.iter().map(|s| s.RBI).sum();
    let total_sb: u64 = a.iter().map(|s| s.SB).sum();
    let total_avg: f64 = a.iter().map(|s| s.AVG).sum();

    let length = a.len() as f64;
    Means {
        hr: total_hr as f64 / length,
        r: total_r as f64 / length,
        rbi: total_rbi as f64 / length,
        sb: total_sb as f64 / length,
        avg: total_avg as f64 / length,
    }
}

fn main() -> Result<(), Error> {
    let mut rdr = csv::Reader::from_path("batters.csv")?;
    let mut csv_batters: Vec<CSVBatter> = Vec::new();

    for result in rdr.deserialize() {
        csv_batters.push(result?);
    }

    let means = get_means(&csv_batters);
    let stddevs = get_std_devs(&csv_batters);

    let mut ranked_batters: Vec<Batter> = Vec::new();

    for b in csv_batters {
        ranked_batters.push(make_zs(&b, &stddevs, &means));
    }

    // |a, b| a.partial_cmp(b).unwrap())
    ranked_batters.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());

    let utc: DateTime<Utc> = Utc::now(); 

    println!("{:?}-{:?}-{:?}", utc.day(), utc.month(),utc.year());
    let conn = Connection::open("./sqldb.db").unwrap();

    conn.execute(
        "CREATE TABLE IF NOT EXISTS batters (
        id              TEXT PRIMARY KEY,
        name            TEXT NOT NULL,
        score           REAL NOT NULL
        )", []).unwrap();

    for b in ranked_batters {
        conn.execute(
            "INSERT OR IGNORE INTO batters (id, name, score) VALUES (?1, ?2, ?3)",
            params![b.id, b.name, b.score],
        ).unwrap();
    }

    /** 
    for i in 0..20 {

        println!("{:?}", ranked_batters[i])
    }*/


    Ok(())
}
