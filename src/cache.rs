use redis::{Client, Commands, RedisResult};
use serde_json;
use chrono::{Datelike, IsoWeek};

pub struct RedisCache {
    client: Client,
}

impl RedisCache {
    pub fn new(redis_url: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let client = Client::open(redis_url)?;
        Ok(RedisCache { client })
    }

    pub fn store_schedule_with_week(
        &self,
        subdomain: &str,
        business_unit: u32,
        iso_week: &IsoWeek,
        schedule: &[crate::FilteredClass],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut conn = self.client.get_connection()?;

        let week_key = format!("schedule:{}:{}:{}-W{:02}",
                               subdomain,
                               business_unit,
                               iso_week.year(),
                               iso_week.week());

        let json_data = serde_json::to_string(schedule)?;

        // Store with 7 day expiration (until next week)
        let _: () = conn.set_ex(&week_key, json_data, 7 * 24 * 3600)?;

        println!("Stored schedule with key: {}", week_key);
        Ok(())
    }

    pub fn store_class(
        &self,
        subdomain: &str,
        business_unit: u32,
        class: &crate::FilteredClass,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut conn = self.client.get_connection()?;

        let class_key = format!("class:{}:{}:{}", subdomain, business_unit, class.id);
        let class_json = serde_json::to_string(class)?;

        // 7 days
        let _: () = conn.set_ex(&class_key, class_json, 7 * 24 * 3600)?;

        Ok(())
    }
}
