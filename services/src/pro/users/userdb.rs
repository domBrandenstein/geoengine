use crate::contexts::SessionId;
use crate::error::Result;
use crate::pro::permissions::{RoleDescription, RoleId};
use crate::pro::users::oidc::ExternalUserClaims;
use crate::pro::users::{UserCredentials, UserId, UserRegistration, UserSession};
use crate::projects::{ProjectId, STRectangle};
use async_trait::async_trait;
use geoengine_datatypes::primitives::Duration;
use snafu::Snafu;

#[async_trait]
pub trait UserAuth {
    /// Registers a user by providing `UserRegistration` parameters
    ///
    /// # Errors
    ///
    /// This call fails if the `UserRegistration` is invalid.
    ///
    async fn register_user(&self, user: UserRegistration) -> Result<UserId>;

    /// Creates session for anonymous user
    ///
    /// # Errors
    ///
    /// This call fails if the `UserRegistration` is invalid.
    ///
    async fn create_anonymous_session(&self) -> Result<UserSession>;

    /// Creates a `Session` by providing `UserCredentials`
    ///
    /// # Errors
    ///
    /// This call fails if the `UserCredentials` are invalid.
    ///
    async fn login(&self, user: UserCredentials) -> Result<UserSession>;

    /// Creates a `Session` for authorized user by providing `ExternalUserClaims`.
    /// If external user is unknown to the internal system, a new user id is created.
    ///
    /// # Errors
    ///
    /// This call fails if the `ExternalUserClaims` are invalid.
    ///
    async fn login_external(
        &self,
        user: ExternalUserClaims,
        duration: Duration,
    ) -> Result<UserSession>;

    /// Get session by id
    ///
    /// # Errors
    ///
    /// This call fails if the session is invalid.
    ///
    async fn user_session_by_id(&self, session: SessionId) -> Result<UserSession>;
}

#[async_trait]
pub trait UserDb: Send + Sync {
    /// Removes the session from the `UserDB`
    ///
    /// # Errors
    ///
    /// This call fails if the session is invalid.
    ///
    async fn logout(&self) -> Result<()>;

    /// Sets the session project
    ///
    /// # Errors
    ///
    /// This call fails if the session is invalid
    ///
    async fn set_session_project(&self, project: ProjectId) -> Result<()>;

    /// Sets the session view
    ///
    /// # Errors
    ///
    /// This call fails if the session is invalid
    ///
    async fn set_session_view(&self, view: STRectangle) -> Result<()>;

    /// Gets the current users total used quota. `session` is used to identify the user.
    ///
    /// # Errors
    ///
    /// This call fails if the session is invalid
    ///
    async fn quota_used(&self) -> Result<u64>;

    /// Gets the current users available quota. `session` is used to identify the user.
    ///
    /// # Errors
    ///
    /// This call fails if the session is invalid
    ///
    async fn quota_available(&self) -> Result<i64>;

    /// Increments a users quota by the given amount
    ///
    /// # Errors
    ///
    /// This call fails if the user is unknown
    ///
    // TODO: move this method to some AdminDb?
    async fn increment_quota_used(&self, user: &UserId, quota_used: u64) -> Result<()>;

    /// Increments multiple users quota by the given amount
    ///
    /// # Errors
    ///
    /// This call fails if database cannot be accessed
    ///
    // TODO: move this method to some AdminDb?
    async fn bulk_increment_quota_used<I: IntoIterator<Item = (UserId, u64)> + Send>(
        &self,
        quota_used_updates: I,
    ) -> Result<()>;

    /// Gets a specific users used quota
    ///
    /// # Errors
    ///
    /// This call fails if the user is unknown
    ///
    /// // TODO: move this method to some AdminDb?
    async fn quota_used_by_user(&self, user: &UserId) -> Result<u64>;

    /// Gets a specific users available quota
    ///
    /// # Errors
    ///
    /// This call fails if the user is unknown
    ///
    /// // TODO: move this method to some AdminDb?
    async fn quota_available_by_user(&self, user: &UserId) -> Result<i64>;

    /// Updates a specific users available quota
    ///
    /// # Errors
    ///
    /// This call fails if the user is unknown
    ///
    /// // TODO: move this method to some AdminDb?
    async fn update_quota_available_by_user(
        &self,
        user: &UserId,
        new_available_quota: i64,
    ) -> Result<()>;
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)), context(suffix(RoleDbError)))]
pub enum RoleDbError {
    #[snafu(display("Permission error: {source}"))]
    PermissionDb {
        source: crate::pro::permissions::PermissionDbError,
    },
    #[snafu(display("Role with id {role_id} does not exist."))]
    RoleIdDoesNotExist { role_id: RoleId },
    #[snafu(display("Role with name {role_name} does not exist."))]
    RoleNameDoesNotExist { role_name: String },
    #[snafu(display("Role with name {role_name} already exists."))]
    RoleAlreadyExists { role_name: String },
    #[snafu(display("Cannot revoke role {role_id} because it is not assigned."))]
    CannotRevokeRoleThatIsNotAssigned { role_id: RoleId },
    #[snafu(display("An unexpected database error occurred."))]
    Postgres { source: tokio_postgres::Error },
    #[snafu(display("An unexpected database error occurred."))]
    Bb8 {
        source: bb8_postgres::bb8::RunError<tokio_postgres::Error>,
    },
}

#[async_trait]
pub trait RoleDb {
    /// Add a new role
    async fn add_role(&self, role_name: &str) -> Result<RoleId, RoleDbError>;

    /// Load a role by name
    async fn load_role_by_name(&self, role_name: &str) -> Result<RoleId, RoleDbError>;

    /// Remove an existing role
    async fn remove_role(&self, role_id: &RoleId) -> Result<(), RoleDbError>;

    /// Remove an existing role
    async fn assign_role(&self, role_id: &RoleId, user_id: &UserId) -> Result<(), RoleDbError>;

    /// Remove an existing role
    async fn revoke_role(&self, role_id: &RoleId, user_id: &UserId) -> Result<(), RoleDbError>;

    /// Get role descriptions for user
    async fn get_role_descriptions(
        &self,
        user_id: &UserId,
    ) -> Result<Vec<RoleDescription>, RoleDbError>;
}
