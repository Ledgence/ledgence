use super::*;
use ledgence_orchestration_api::console::{
    CONSOLE_METADATA_MAX_BYTES, ConsoleRecord, RegisterProgram, RegisterProgramReply,
};
impl ResponseValue for RegisterProgramReply {
    const MAX_BYTES: usize = CONSOLE_METADATA_MAX_BYTES;
    fn validate_values(&self) -> Result<()> {
        self.version.validate()
    }
}
impl HttpTaskService {
    /// Register one exact, already-published reference. One exchange, no retries.
    pub async fn register_program(
        &self,
        command: &RegisterProgram,
    ) -> Result<RegisterProgramReply> {
        command.validate()?;
        let expected = command.clone();
        self.post_validated(
            "v1/console/programs/register",
            command,
            SUBMISSION_MAX_BYTES,
            move |reply: &RegisterProgramReply| {
                if reply.version.descriptor.program != expected.program
                    || reply.version.metadata != expected.metadata
                    || (reply.metadata_updated
                        && (!expected.update_metadata || !reply.already_registered))
                {
                    return Err(unavailable(
                        "catalog registration reply does not match request",
                    ));
                }
                Ok(())
            },
        )
        .await
    }
}
