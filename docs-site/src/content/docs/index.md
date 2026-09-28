---
title: Build with Ledgence.
description: Run agents, coordinate workflows, and operate a self-hosted Ledgence instance with Console.
tableOfContents: false
---

<p class="ld-home-intro">Run your agents, follow their workflows, and understand what is happening on your infrastructure.</p>

<a class="ld-release-link" href="/reference/releases"><span>Ledgence 0.1</span> Public releases are here <span aria-hidden="true">→</span></a>

<div class="ld-start-panel">
  <div>
    <span class="ld-eyebrow">Console · available from current source</span>
    <h2>See your work in motion.</h2>
    <p>Start a self-hosted instance, inspect real executions, and explore its workflows, registered agents, and worker processes.</p>
    <div class="ld-start-actions"><a class="ld-start-link" href="/tutorials/use-console">Explore Console <span aria-hidden="true">↗</span></a><a class="ld-install-link" href="/tutorials/run-locally">Run the released stack <span aria-hidden="true">→</span></a></div>
  </div>
  <div class="ld-worker-card" aria-label="Illustration of a worker with six process slots">
    <span>Your worker</span>
    <div class="ld-process-grid"><span>✓</span><span></span><span></span><span></span><span></span><span></span></div>
  </div>
</div>

Explore a complete agent workflow: [from bug report to reviewed change](/tutorials/codex-change-review). Codex proposes a fix while Ledgence coordinates local tests, distributed review, and a durable result.

## Find your way

<div class="ld-doc-grid">
  <a class="ld-doc-card" href="/tutorials/use-console"><span class="ld-doc-card-title">Tutorials <span aria-hidden="true">→</span></span><span>Learn through complete, guided examples with a result you can check.</span></a>
  <a class="ld-doc-card" href="/how-to/register-agent"><span class="ld-doc-card-title">How-to guides <span aria-hidden="true">→</span></span><span>Register an agent, coordinate parallel work, or wait for an external event.</span></a>
  <a class="ld-doc-card" href="/reference/console"><span class="ld-doc-card-title">Reference <span aria-hidden="true">→</span></span><span>Look up methods, fields, limits, and precise behavior.</span></a>
  <a class="ld-doc-card" href="/concepts/self-hosted-console"><span class="ld-doc-card-title">Concepts <span aria-hidden="true">→</span></span><span>Understand the instance boundary, worker observations, checkpoints, and recovery.</span></a>
</div>

## A few names to know

Your **agent** is application code. You publish that code and its prepared dependencies as a **program package**. A **task** asks a worker to execute it. A **workflow** coordinates work through explicit checkpoints and continuations.

Ledgence also runs data pipelines and other application code. The initial runtime is Python; the platform is built in Rust.

<p class="ld-home-status">Release tutorials target source 0.1.1 or native 0.1.0. Console guides and typed workflow entrypoints/forks target the current source implementation; those features are not included in the published artifacts. Check the <a href="/reference/releases">release reference</a> before installing or upgrading. Run code you trust on infrastructure you control.</p>
