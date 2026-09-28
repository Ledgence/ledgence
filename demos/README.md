# Ledgence demos

Runnable applications that show how an agent fits into a durable workflow.
Each demo includes its implementation, setup instructions and verification.
Provider integrations are optional application dependencies, separate from the
Ledgence platform and Python client.

| Demo | Workflow | Integration |
| --- | --- | --- |
| [Codex support agent](codex-support-agent/README.md) | Read a ticket → research local documentation → draft a reply → wait for human review → finish | Codex CLI with ChatGPT sign-in and GPT-6 Luna |
| [Support agent](support-agent/README.md) | Read a ticket → research local documentation → draft a reply → wait for human review → finish | Google ADK and Gemini |

These demos use real Ledgence tasks, workers and persisted workflow state.
Unit tests replace external model responses; the explicitly enabled live checks
call the actual provider.
