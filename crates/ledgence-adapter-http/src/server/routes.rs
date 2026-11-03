//! Routing and access policy use the same exhaustive registration entries.
//! Authentication is applied at the shared ingress before any route dispatch.
use ledgence_orchestration_api::{
    AccessCapability, AuditReadKind, ContractError, PrincipalClass, Result, VerifiedPrincipal,
};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Policy {
    Metadata,
    Sensitive,
    Submit,
    Control,
    Publish,
    Approval,
    EffectResolution,
    ResourceControl,
    Worker,
    Access,
    Login,
    Logout,
}
impl Policy {
    pub(super) fn is_access(self) -> bool {
        matches!(self, Self::Access | Self::Login | Self::Logout)
    }
    pub(super) fn require(self, actor: &VerifiedPrincipal) -> Result<()> {
        if self.is_access() {
            return Ok(());
        }
        if (actor.class() == PrincipalClass::Worker) != (self == Self::Worker) {
            return Err(ContractError::Forbidden);
        }
        use AccessCapability::*;
        let capability = match self {
            Self::Metadata => MetadataRead,
            Self::Sensitive => SensitiveRead,
            Self::Submit => ExecutionSubmit,
            Self::Control => ExecutionControl,
            Self::Publish => ProgramPublish,
            Self::Approval => {
                actor.require(SensitiveRead)?;
                ApprovalDecide
            }
            Self::EffectResolution => EffectResolve,
            Self::ResourceControl => ResourceControl,
            Self::Worker => WorkerExecute,
            Self::Access | Self::Login | Self::Logout => unreachable!(),
        };
        actor.require(capability)
    }
}

#[derive(Clone, Copy)]
pub(super) struct Route {
    pub path: &'static str,
    pub methods: &'static str,
    primary: Policy,
    post: Option<Policy>,
    read: Option<AuditReadKind>,
}
impl Route {
    pub const fn get(path: &'static str, policy: Policy) -> Self {
        Self {
            path,
            methods: "GET",
            primary: policy,
            post: None,
            read: None,
        }
    }
    pub const fn post(path: &'static str, policy: Policy) -> Self {
        Self {
            path,
            methods: "POST",
            primary: policy,
            post: None,
            read: None,
        }
    }
    pub const fn put(path: &'static str, policy: Policy) -> Self {
        Self {
            path,
            methods: "PUT",
            primary: policy,
            post: None,
            read: None,
        }
    }
    pub const fn read_write(path: &'static str, read: Policy, write: Policy) -> Self {
        Self {
            path,
            methods: "GET, POST",
            primary: read,
            post: Some(write),
            read: None,
        }
    }
    pub const fn sensitive_get(path: &'static str, read: AuditReadKind) -> Self {
        Self {
            path,
            methods: "GET",
            primary: Policy::Sensitive,
            post: None,
            read: Some(read),
        }
    }
    pub const fn sensitive_post(path: &'static str, read: AuditReadKind) -> Self {
        Self {
            path,
            methods: "POST",
            primary: Policy::Sensitive,
            post: None,
            read: Some(read),
        }
    }
    pub fn audit_read(&self, method: &str) -> Option<AuditReadKind> {
        match self.policy(method) {
            Some(Policy::Approval) => Some(AuditReadKind::ApprovalDecision),
            Some(Policy::Sensitive) => self.read,
            _ => None,
        }
    }
    pub fn policy(&self, method: &str) -> Option<Policy> {
        if !self.methods.split(", ").any(|allowed| allowed == method) {
            return None;
        }
        Some(if method == "POST" {
            self.post.unwrap_or(self.primary)
        } else {
            self.primary
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    #[test]
    fn every_registered_method_has_an_explicit_policy() {
        let routes: Vec<_> = super::super::ROUTES
            .iter()
            .chain(super::super::console::ROUTES)
            .chain(super::super::publication::ROUTES)
            .collect();
        assert_eq!(routes.len(), 99);
        assert_eq!(
            routes.iter().map(|r| r.path).collect::<HashSet<_>>().len(),
            99
        );
        let mut methods = 0;
        let mut audited = 0;
        for route in routes {
            for method in route.methods.split(", ") {
                assert!(route.policy(method).is_some());
                let sensitive = matches!(
                    route.policy(method),
                    Some(Policy::Sensitive | Policy::Approval)
                );
                assert_eq!(
                    route.audit_read(method).is_some(),
                    sensitive,
                    "{} {method}",
                    route.path
                );
                audited += usize::from(sensitive);
                methods += 1;
            }
            for method in ["HEAD", "OPTIONS", "DELETE", "PATCH", "TRACE", "get"] {
                assert_eq!(route.policy(method), None, "{} {method}", route.path);
            }
        }
        assert_eq!(methods, 102);
        assert_eq!(audited, 18);
    }
    #[test]
    fn payload_worker_and_mixed_method_policies_are_explicit() {
        let routes: Vec<_> = super::super::ROUTES
            .iter()
            .chain(super::super::console::ROUTES)
            .chain(super::super::publication::ROUTES)
            .collect();
        let policy = |path, method| {
            routes
                .iter()
                .find(|r| r.path == path)
                .and_then(|r| r.policy(method))
        };
        for path in ["/v1/tasks", "/v1/console/tasks", "/v1/console/workflows"] {
            assert_eq!(policy(path, "GET"), Some(Policy::Metadata));
            assert_eq!(policy(path, "POST"), Some(Policy::Submit));
        }
        for (path, method) in [
            ("/v1/tasks/result", "GET"),
            ("/v1/tasks/inspect", "GET"),
            ("/v1/workflows/result", "GET"),
            ("/v1/workflows/approvals/inspect", "POST"),
            ("/v1/workflows/approvals/list", "POST"),
            ("/v1/console/approvals/inspect", "POST"),
            ("/v1/console/approvals/list", "POST"),
            ("/v1/console/tasks/result", "GET"),
            ("/v1/console/tasks/inspect", "GET"),
            ("/v1/console/workflows/result", "GET"),
            ("/v1/console/workflows/input", "GET"),
            ("/v1/console/workflows/local-steps/inspect", "GET"),
        ] {
            assert_eq!(policy(path, method), Some(Policy::Sensitive), "{path}");
        }
        assert_eq!(policy("/v1/attempts/inspect", "GET"), Some(Policy::Worker));
        assert_eq!(
            policy("/v1/console/attempts/inspect", "GET"),
            Some(Policy::Metadata)
        );
        assert_eq!(
            policy("/v1/programs/{program_id}/{version}/artifact", "PUT"),
            Some(Policy::Publish)
        );
        assert_eq!(policy("/v1/unclassified", "POST"), None);
    }
}
