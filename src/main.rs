//! catcount — count actur articles per category over a date range.
//!
//! The `actur` mongod on con1 binds to 127.0.0.1 only, so by default this
//! program opens its own `ssh -L` tunnel and tears it down on exit.

use std::collections::BTreeMap;
use std::net::{Ipv4Addr, SocketAddr, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Utc};
use clap::{Parser, ValueEnum};
use mongodb::bson::{Bson, DateTime as BsonDateTime, Document, doc};
use mongodb::options::ClientOptions;
use mongodb::{Client, Collection};
use serde::Deserialize;

#[derive(Parser, Debug)]
#[command(
    name = "catcount",
    about = "Count actur articles per category between two dates",
    long_about = "Counts articles in each category of the actur MongoDB database \
                  within a pubdate range.\n\n\
                  Dates accept YYYY-MM-DD, 'YYYY-MM-DD HH:MM[:SS]' or RFC 3339, and are \
                  interpreted as UTC (pubdate is stored as naive UTC). A bare --start date \
                  means 00:00:00 of that day; a bare --end date means 23:59:59.999 of that \
                  day, so the range is inclusive on both ends."
)]
struct Args {
    /// Start of the range (inclusive)
    #[arg(short, long)]
    start: String,

    /// End of the range (inclusive)
    #[arg(short, long)]
    end: String,

    /// MongoDB URI. Defaults to the tunnel endpoint, or 127.0.0.1:27017 with --no-tunnel
    #[arg(long)]
    uri: Option<String>,

    /// Database name
    #[arg(long, default_value = "actur")]
    db: String,

    /// Collection name
    #[arg(long, default_value = "articles")]
    collection: String,

    /// Do not open an ssh tunnel; connect to --uri directly
    #[arg(long)]
    no_tunnel: bool,

    /// ssh host (from ~/.ssh/config) to tunnel through
    #[arg(long, default_value = "con1")]
    ssh_host: String,

    /// Local port for the tunnel
    #[arg(long, default_value_t = 27017)]
    local_port: u16,

    /// Remote mongod port on the ssh host
    #[arg(long, default_value_t = 27017)]
    remote_port: u16,

    /// Merge near-duplicate category labels (trim, strip trailing '.', case-fold)
    #[arg(long)]
    normalize: bool,

    /// Show only the N largest categories
    #[arg(long)]
    top: Option<usize>,

    /// Hide categories with fewer than N articles
    #[arg(long, default_value_t = 0)]
    min: i64,

    /// Output format
    #[arg(long, value_enum, default_value_t = Format::Table)]
    format: Format,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
enum Format {
    Table,
    Csv,
    Json,
}

/// One `$group` result row.
#[derive(Debug, Deserialize)]
struct CatCount {
    #[serde(rename = "_id")]
    cat: String,
    count: i64,
}

/// An `ssh -N -L` child process that is killed when it goes out of scope.
struct Tunnel(Option<Child>);

impl Tunnel {
    /// Opens a tunnel to `host`, unless `local_port` is already accepting
    /// connections (an existing tunnel or a local mongod), in which case that
    /// endpoint is reused.
    fn open(host: &str, local_port: u16, remote_port: u16) -> Result<Self> {
        if port_open(local_port) {
            eprintln!("note: 127.0.0.1:{local_port} already accepting connections; reusing it");
            return Ok(Tunnel(None));
        }

        let child = Command::new("ssh")
            .args([
                "-N",
                "-o",
                "ExitOnForwardFailure=yes",
                "-o",
                "BatchMode=yes",
                "-L",
                &format!("{local_port}:127.0.0.1:{remote_port}"),
                host,
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .spawn()
            .with_context(|| format!("failed to spawn ssh to {host}"))?;

        let mut tunnel = Tunnel(Some(child));

        // ssh needs a moment to authenticate and bind the forward.
        let deadline = Instant::now() + Duration::from_secs(15);
        while Instant::now() < deadline {
            if let Some(status) = tunnel.0.as_mut().unwrap().try_wait()? {
                bail!("ssh tunnel to {host} exited early ({status})");
            }
            if port_open(local_port) {
                return Ok(tunnel);
            }
            std::thread::sleep(Duration::from_millis(150));
        }
        bail!("timed out waiting for the ssh tunnel to {host} on port {local_port}")
    }
}

impl Drop for Tunnel {
    fn drop(&mut self) {
        if let Some(child) = self.0.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn port_open(port: u16) -> bool {
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    TcpStream::connect_timeout(&addr, Duration::from_millis(300)).is_ok()
}

/// Parses YYYY-MM-DD, "YYYY-MM-DD HH:MM[:SS]" or RFC 3339 as UTC.
///
/// A bare date is widened to the start of the day, or to the last millisecond
/// of the day when `end_of_day` is set.
fn parse_datetime(s: &str, end_of_day: bool) -> Result<DateTime<Utc>> {
    let s = s.trim();

    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt.with_timezone(&Utc));
    }
    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M"] {
        if let Ok(naive) = NaiveDateTime::parse_from_str(s, fmt) {
            return Ok(Utc.from_utc_datetime(&naive));
        }
    }
    if let Ok(date) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        let naive = if end_of_day {
            date.and_hms_milli_opt(23, 59, 59, 999).unwrap()
        } else {
            date.and_hms_opt(0, 0, 0).unwrap()
        };
        return Ok(Utc.from_utc_datetime(&naive));
    }

    Err(anyhow!(
        "could not parse date {s:?}; expected YYYY-MM-DD, 'YYYY-MM-DD HH:MM[:SS]' or RFC 3339"
    ))
}

/// Merge key for label variants that differ only by whitespace, trailing
/// periods or case, so that "International Affairs." lands in the same bucket
/// as "International Affairs".
fn normalize_key(cat: &str) -> String {
    let trimmed = cat.trim_matches(|c: char| c.is_whitespace() || c == '.');
    if trimmed.is_empty() {
        return "(none)".to_string();
    }
    trimmed.split_whitespace().collect::<Vec<_>>().join(" ").to_lowercase()
}

/// Merges variant labels. Each bucket is displayed under its most common
/// original spelling, which keeps acronyms intact ("US Politics", not
/// "Us Politics").
fn merge_variants(rows: Vec<CatCount>) -> Vec<CatCount> {
    // key -> (total, best label so far, that label's own count)
    let mut merged: BTreeMap<String, (i64, String, i64)> = BTreeMap::new();
    for row in rows {
        let key = normalize_key(&row.cat);
        let entry = merged.entry(key).or_insert((0, row.cat.clone(), 0));
        entry.0 += row.count;
        if row.count > entry.2 {
            entry.1 = row.cat.clone();
            entry.2 = row.count;
        }
    }

    let mut out: Vec<CatCount> = merged
        .into_values()
        .map(|(count, cat, _)| CatCount { cat, count })
        .collect();
    out.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.cat.cmp(&b.cat)));
    out
}

async fn count_by_category(
    articles: &Collection<Document>,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Vec<CatCount>> {
    let pipeline = vec![
        doc! { "$match": {
            "pubdate": {
                "$gte": Bson::DateTime(BsonDateTime::from_millis(start.timestamp_millis())),
                "$lte": Bson::DateTime(BsonDateTime::from_millis(end.timestamp_millis())),
            }
        }},
        doc! { "$group": {
            // $toString keeps the key a string even if a stray cat value is not one.
            "_id": { "$toString": { "$ifNull": ["$cat", "(none)"] } },
            "count": { "$sum": 1 },
        }},
        doc! { "$sort": { "count": -1, "_id": 1 } },
    ];

    let mut cursor = articles
        .aggregate(pipeline)
        .await
        .context("aggregation failed")?;

    let mut rows = Vec::new();
    while cursor.advance().await? {
        let doc = cursor.current();
        let row: CatCount =
            mongodb::bson::from_slice(doc.as_bytes()).context("unexpected $group result shape")?;
        rows.push(row);
    }
    Ok(rows)
}

fn print_table(rows: &[CatCount], total: i64, start: DateTime<Utc>, end: DateTime<Utc>) {
    let fmt = "%Y-%m-%d %H:%M:%S";
    println!(
        "Articles by category, {} .. {} UTC",
        start.format(fmt),
        end.format(fmt)
    );
    println!();

    let width = rows.iter().map(|r| r.cat.chars().count()).max().unwrap_or(8).max(8);
    println!("{:<width$}  {:>9}  {:>7}", "CATEGORY", "ARTICLES", "PCT", width = width);
    println!("{}  {}  {}", "-".repeat(width), "-".repeat(9), "-".repeat(7));

    for row in rows {
        let pct = if total > 0 { row.count as f64 * 100.0 / total as f64 } else { 0.0 };
        println!(
            "{:<width$}  {:>9}  {:>6.2}%",
            row.cat,
            row.count,
            pct,
            width = width
        );
    }

    println!("{}  {}  {}", "-".repeat(width), "-".repeat(9), "-".repeat(7));
    println!("{:<width$}  {:>9}", "TOTAL", total, width = width);
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let start = parse_datetime(&args.start, false)?;
    let end = parse_datetime(&args.end, true)?;
    if end < start {
        bail!("--end ({end}) is before --start ({start})");
    }

    // Keep the tunnel alive for the whole of main.
    let _tunnel = if args.no_tunnel {
        None
    } else {
        Some(Tunnel::open(&args.ssh_host, args.local_port, args.remote_port)?)
    };

    let uri = args.uri.unwrap_or_else(|| {
        let port = if args.no_tunnel { 27017 } else { args.local_port };
        format!("mongodb://127.0.0.1:{port}")
    });

    let mut options = ClientOptions::parse(&uri)
        .await
        .with_context(|| format!("bad MongoDB URI {uri}"))?;
    options.server_selection_timeout = Some(Duration::from_secs(10));
    options.app_name = Some("catcount".to_string());

    let client = Client::with_options(options)?;
    let articles = client.database(&args.db).collection::<Document>(&args.collection);

    let rows = count_by_category(&articles, start, end).await?;

    // Category labels come straight from the model, so merging is opt-in.
    let mut rows = if args.normalize { merge_variants(rows) } else { rows };

    // Total covers every article in the range, including filtered-out rows.
    let total: i64 = rows.iter().map(|r| r.count).sum();

    rows.retain(|r| r.count >= args.min);
    if let Some(top) = args.top {
        rows.truncate(top);
    }

    match args.format {
        Format::Table => print_table(&rows, total, start, end),
        Format::Csv => {
            println!("category,count");
            for row in &rows {
                // Quote and escape: labels can contain commas or quotes.
                println!("\"{}\",{}", row.cat.replace('"', "\"\""), row.count);
            }
        }
        Format::Json => {
            let out = serde_json::json!({
                "start": start.to_rfc3339(),
                "end": end.to_rfc3339(),
                "total": total,
                "categories": rows.iter().map(|r| serde_json::json!({
                    "category": r.cat,
                    "count": r.count,
                })).collect::<Vec<_>>(),
            });
            println!("{}", serde_json::to_string_pretty(&out)?);
        }
    }

    Ok(())
}
