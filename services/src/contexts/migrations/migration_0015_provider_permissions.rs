use super::database_migration::{DatabaseVersion, Migration};
use crate::contexts::Migration0014MultibandColorizer;
use crate::error::Result;
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// This migration adds the provider permissions
pub struct Migration0015ProviderPermissions;

#[async_trait]
impl Migration for Migration0015ProviderPermissions {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0014MultibandColorizer.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0015_provider_permissions".into()
    }

    async fn migrate(&self, _tx: &Transaction<'_>) -> Result<()> {
        Ok(())
    }
}
