//! Own the pooled connection while SQLx starts a borrowed transaction.

use crate::StoreResult;
use sqlx::{Connection, Postgres, Transaction, pool::PoolConnection};

pub(crate) struct TransactionConnection {
    connection: PoolConnection<Postgres>,
    startup_unconfirmed: bool,
}

impl TransactionConnection {
    pub(crate) fn new(connection: PoolConnection<Postgres>) -> Self {
        Self {
            connection,
            startup_unconfirmed: false,
        }
    }

    pub(crate) async fn begin_write(&mut self) -> StoreResult<Transaction<'_, Postgres>> {
        self.begin(false).await
    }

    pub(crate) async fn begin_read(&mut self) -> StoreResult<Transaction<'_, Postgres>> {
        self.begin(true).await
    }

    async fn begin(&mut self, read_only: bool) -> StoreResult<Transaction<'_, Postgres>> {
        // SQLx 0.9.0 records transaction depth only after the BEGIN reply. Its
        // normal rollback-on-drop cannot cover cancellation during that await.
        // Retain ownership here so uncertain startup never returns to the pool.
        self.startup_unconfirmed = true;
        let mut tx = self.connection.begin().await?;
        self.startup_unconfirmed = false;

        // From here, SQLx owns rollback on errors or cancellation. The borrowed
        // transaction is dropped before this connection can return to the pool.
        let isolation = if read_only {
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
        } else {
            "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"
        };
        sqlx::query(isolation).execute(&mut *tx).await?;
        if !read_only {
            sqlx::query("SET LOCAL synchronous_commit = on")
                .execute(&mut *tx)
                .await?;
        }
        Ok(tx)
    }
}

impl Drop for TransactionConnection {
    fn drop(&mut self) {
        if self.startup_unconfirmed {
            // SQLx bounds physical closure and retains the pool permit while
            // closing. Healthy transactions keep ordinary connection reuse.
            self.connection.close_on_drop();
        }
    }
}
