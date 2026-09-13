//! Explicit schema migration for a configured PostgreSQL deployment.
use ledgence_adapter_postgres::{PostgresOptions, PostgresStore};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("LEDGENCE_POSTGRES_URL")?;
    let store = PostgresStore::connect(&url, PostgresOptions::default()).await?;
    store.migrate().await?;
    store.close().await;
    println!("Ledgence PostgreSQL migrations applied.");
    Ok(())
}
