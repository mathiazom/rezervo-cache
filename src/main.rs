use clap::Parser;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;
use std::fs::File;
use std::io::Write;
use chrono::{Datelike, IsoWeek, NaiveDate, Utc};

mod cache;

use cache::RedisCache;

#[derive(Parser)]
#[command(name = "rezervo-cache")]
#[command(about = "Fetch class schedule for the current ISO week")]
struct Args {
    #[arg(short, long)]
    subdomain: String,

    #[arg(short, long)]
    business_unit: u32,

    #[arg(long, default_value = "redis://redis:6379")]
    redis_url: String,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct FilteredClass {
    #[serde(rename = "bookableEarliest")]
    pub bookable_earliest: String,
    #[serde(rename = "bookableLatest")]
    pub bookable_latest: String,
    pub id: i64,
    pub name: String,
    pub duration: Value,
    #[serde(rename = "groupActivityProduct")]
    pub group_activity_product: Value,
    #[serde(rename = "businessUnit")]
    pub business_unit: Value,
    pub locations: Vec<Value>,
    pub instructors: Vec<Value>,
    #[serde(rename = "externalMessage")]
    pub external_message: Option<String>,
    pub cancelled: bool,
    pub slots: Value,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let redis_cache = RedisCache::new(&args.redis_url)?;

    // Get current and next ISO week dates
    let (current_week_start, current_week_end, current_iso_week) = get_current_iso_week();
    let (next_week_start, next_week_end, next_iso_week) = get_next_iso_week();

    println!("Fetching current week {} ({} to {})",
             format_iso_week(&current_iso_week), current_week_start, current_week_end);
    println!("Fetching next week {} ({} to {})",
             format_iso_week(&next_iso_week), next_week_start, next_week_end);

    // Fetch current week
    let current_schedule = fetch_brp_schedule_for_week(&args.subdomain, args.business_unit, current_week_start, current_week_end).await?;

    // Fetch next week
    let next_schedule = fetch_brp_schedule_for_week(&args.subdomain, args.business_unit, next_week_start, next_week_end).await?;

    // Store current week
    if let Err(e) = redis_cache.store_schedule_with_week(&args.subdomain, args.business_unit, &current_iso_week, &current_schedule) {
        eprintln!("Warning: Failed to store current week schedule: {}", e);
    } else {
        println!("Current week schedule cached successfully");
    }

    for class in &current_schedule {
        if let Err(e) = redis_cache.store_class(&args.subdomain, args.business_unit, class) {
            eprintln!("Warning: Failed to store current week class {}: {}", class.id, e);
        }
    }

    // Store next week
    if let Err(e) = redis_cache.store_schedule_with_week(&args.subdomain, args.business_unit, &next_iso_week, &next_schedule) {
        eprintln!("Warning: Failed to store next week schedule: {}", e);
    } else {
        println!("Next week schedule cached successfully");
    }

    for class in &next_schedule {
        if let Err(e) = redis_cache.store_class(&args.subdomain, args.business_unit, class) {
            eprintln!("Warning: Failed to store next week class {}: {}", class.id, e);
        }
    }

    println!("Successfully cached {} classes for current week {}",
             current_schedule.len(), format_iso_week(&current_iso_week));
    println!("Successfully cached {} classes for next week {}",
             next_schedule.len(), format_iso_week(&next_iso_week));

    Ok(())
}

fn get_next_iso_week() -> (NaiveDate, NaiveDate, IsoWeek) {
    let today = Utc::now().date_naive();
    let next_week_date = today + chrono::Duration::days(7);
    let iso_week = next_week_date.iso_week();

    // Calculate Monday (start of ISO week)
    let days_from_monday = next_week_date.weekday().num_days_from_monday();
    let week_start = next_week_date - chrono::Duration::days(days_from_monday as i64);

    // Calculate Sunday (end of ISO week)
    let week_end = week_start + chrono::Duration::days(6);

    (week_start, week_end, iso_week)
}

fn get_current_iso_week() -> (NaiveDate, NaiveDate, IsoWeek) {
    let today = Utc::now().date_naive();
    let iso_week = today.iso_week();

    // Calculate Monday (start of ISO week)
    let days_from_monday = today.weekday().num_days_from_monday();
    let week_start = today - chrono::Duration::days(days_from_monday as i64);

    // Calculate Sunday (end of ISO week)
    let week_end = week_start + chrono::Duration::days(6);

    (week_start, week_end, iso_week)
}

fn format_iso_week(iso_week: &IsoWeek) -> String {
    format!("{}-W{:02}", iso_week.year(), iso_week.week())
}

async fn fetch_brp_schedule_for_week(
    subdomain: &str,
    business_unit: u32,
    week_start: NaiveDate,
    week_end: NaiveDate,
) -> Result<Vec<FilteredClass>, Box<dyn std::error::Error>> {
    let client = Client::new();
    let mut classes = Vec::new();
    let mut seen_ids = HashSet::new();

    // Fetch the entire week in one request
    let url = format!(
        "https://{}.brpsystems.com/brponline/api/ver3/businessunits/{}/groupactivities",
        subdomain, business_unit
    );

    let params = [
        ("period.start", format!("{}T00:00:00.000Z", week_start)),
        ("period.end", format!("{}T23:59:59.999Z", week_end)),
    ];

    println!("Fetching from: {}", url);
    println!("Period: {} to {}", week_start, week_end);

    let response = client.get(&url).query(&params).send().await?;

    if !response.status().is_success() {
        return Err(format!("Failed to fetch schedule: {}", response.status()).into());
    }

    let items: Vec<Value> = response.json().await?;
    println!("Received {} items from API", items.len());

    for item in items {
        if let Some(id) = item.get("id").and_then(|v| v.as_i64()) {
            let id_string = id.to_string();
            if !seen_ids.contains(&id_string) {
                seen_ids.insert(id_string);

                if item.get("bookableEarliest").is_some() && item.get("bookableLatest").is_some() {
                    if let Ok(filtered_class) = serde_json::from_value::<FilteredClass>(item) {
                        classes.push(filtered_class);
                    }
                }
            }
        }
    }

    println!("Filtered to {} unique classes", classes.len());
    Ok(classes)
}
