---
title: Build with Ledgence.
description: Learn to run agents, coordinate workflows, and understand the details of execution on your infrastructure.
tableOfContents: false
---

<p class="ld-home-intro">Run your first agent, coordinate a workflow, and learn how Ledgence fits your infrastructure.</p>

<a class="ld-release-link" href="/reference/releases"><span>Ledgence 0.1</span> Public releases are here <span aria-hidden="true">→</span></a>

<div class="ld-start-panel">
  <div>
    <span class="ld-eyebrow">A working system, on your machine</span>
    <h2>See Ledgence run.</h2>
    <p>Start a local stack, run the example agents, and follow their work through to a result. No vendor account required.</p>
    <div class="ld-start-actions"><a class="ld-start-link" href="/tutorials/run-locally">Run Ledgence locally <span aria-hidden="true">↗</span></a><a class="ld-install-link" href="/how-to/install-native">Install native binaries <span aria-hidden="true">→</span></a></div>
  </div>
  <div class="ld-worker-card" aria-label="Illustration of a worker with six process slots">
    <span>Your worker</span>
    <div class="ld-process-grid"><span>✓</span><span></span><span></span><span></span><span></span><span></span></div>
  </div>
</div>

## Find your way

<div class="ld-doc-grid">
  <a class="ld-doc-card" href="/tutorials/first-workflow"><span class="ld-doc-card-title">Tutorials <span aria-hidden="true">→</span></span><span>Learn through complete, guided examples with a result you can check.</span></a>
  <a class="ld-doc-card" href="/how-to/parallel-tasks"><span class="ld-doc-card-title">How-to guides <span aria-hidden="true">→</span></span><span>Accomplish a task in your own application, from parallel work to event waits.</span></a>
  <a class="ld-doc-card" href="/reference/workflow-context"><span class="ld-doc-card-title">Reference <span aria-hidden="true">→</span></span><span>Look up methods, fields, limits, and precise behavior.</span></a>
  <a class="ld-doc-card" href="/concepts/execution-model"><span class="ld-doc-card-title">Concepts <span aria-hidden="true">→</span></span><span>Understand workers, checkpoints, recovery, and the choices behind them.</span></a>
</div>

## A few names to know

Your **agent** is application code. You publish that code and its prepared dependencies as a **program package**. A **task** asks a worker to execute it. A **workflow** coordinates work through explicit checkpoints and continuations.

Ledgence also runs data pipelines and other application code. The initial runtime is Python; the platform is built in Rust.

<p class="ld-home-status">These guides cover the 0.1 release series. Run code you trust on infrastructure you control. Public APIs may change before 1.0; check the <a href="/reference/releases">release reference</a> when upgrading.</p>
