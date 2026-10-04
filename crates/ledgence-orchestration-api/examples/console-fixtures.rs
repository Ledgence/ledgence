#[path = "../tests/support/console_fixtures.rs"]
mod fixture;

fn main() {
    let output = serde_json::to_string_pretty(&fixture::fixtures()).expect("fixture JSON") + "\n";
    match std::env::args().nth(1).as_deref() {
        Some("--write") => std::fs::write(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/console-v5.json"
            ),
            output,
        )
        .expect("write fixtures"),
        None => print!("{output}"),
        _ => panic!(
            "usage: cargo run -p ledgence-orchestration-api --example console-fixtures -- [--write]"
        ),
    }
}
