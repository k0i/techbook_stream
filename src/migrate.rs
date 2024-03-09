use anyhow::Result;
use sqlx::{Connection, Executor, PgConnection};

#[tokio::main]
pub async fn main() -> Result<()> {
    let mut connection = PgConnection::connect(
        "postgres://materialize@localhost:6875/materialize?options=--cluster quickstart",
    )
    .await?;

    sqlx::query(
        r#"
                CREATE TABLE IF NOT EXISTS transaction_events (
                    "wallet_id" "pg_catalog"."int4" NOT NULL,
                    "transaction_id" "pg_catalog"."uuid" NOT NULL,
                    "amount" "pg_catalog"."int8" NOT NULL,
                    "transaction_time" "pg_catalog"."timestamp" NOT NULL,
                    "declined" "pg_catalog"."bool"
                );
            "#,
    )
    .execute(&mut connection)
    .await?;

    sqlx::query(
            r#"
                CREATE MATERIALIZED VIEW IF NOT EXISTS
                    wallet_balance IN CLUSTER quickstart
                AS
                    SELECT
                        DISTINCT ON (wallet_id)
                        wallet_id,
                        SUM(amount) OVER (PARTITION BY wallet_id) AS balance,
                        FIRST_VALUE(transaction_time) OVER (PARTITION BY wallet_id ORDER BY transaction_time DESC) AS last_transaction_time,
                        FIRST_VALUE(transaction_id) OVER (PARTITION BY wallet_id ORDER BY transaction_time DESC) AS last_transaction_id,
                        FIRST_VALUE(declined) OVER (PARTITION BY wallet_id ORDER BY transaction_time DESC) AS declined
                    FROM
                    transaction_events where declined IS NOT true
                ORDER BY wallet_id DESC, transaction_time DESC;
            "#,
        ).execute(&mut connection).await?;

    sqlx::query("GRANT ALL ON wallet_balance TO PUBLIC")
        .execute(&mut connection)
        .await?;

    sqlx::query("GRANT ALL ON TABLE transaction_events TO PUBLIC")
        .execute(&mut connection)
        .await?;

    Ok(())
}
