use async_trait::async_trait;
use tokio_postgres::Transaction;

use crate::contexts::migrations::migration_0015_provider_permissions::Migration0015ProviderPermissions;
use crate::error::Result;

use super::database_migration::{ProMigration, ProMigrationImpl};

#[async_trait]
impl ProMigration for ProMigrationImpl<Migration0015ProviderPermissions> {
    async fn pro_migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!("migration_0015_provider_permissions.sql"))
            .await?;

        Ok(())
    }
}
