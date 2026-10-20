// SPDX-License-Identifier: MIT
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

export default defineConfig({
  site: 'https://docs.ledgence.com',
  output: 'static',
  trailingSlash: 'never',
  build: { format: 'directory' },
  integrations: [starlight({
    title: 'Ledgence',
    description: 'Learn to run agents and workflows on infrastructure you control.',
    favicon: '/favicon.svg',
    head: [
      { tag: 'link', attrs: { rel: 'apple-touch-icon', sizes: '180x180', href: '/brand/apple-touch-icon.png' } },
      { tag: 'meta', attrs: { property: 'og:image', content: 'https://docs.ledgence.com/brand/social-github.png' } },
      { tag: 'meta', attrs: { property: 'og:image:width', content: '1280' } },
      { tag: 'meta', attrs: { property: 'og:image:height', content: '640' } },
      { tag: 'meta', attrs: { property: 'og:image:alt', content: 'Ledgence — task orchestration for agents and data pipelines.' } },
      { tag: 'meta', attrs: { name: 'twitter:image', content: 'https://docs.ledgence.com/brand/social-github.png' } },
      { tag: 'meta', attrs: { name: 'twitter:image:alt', content: 'Ledgence — task orchestration for agents and data pipelines.' } },
    ],
    social: [{ icon: 'github', label: 'GitHub', href: 'https://github.com/Ledgence/ledgence' }],
    editLink: { baseUrl: 'https://github.com/Ledgence/ledgence/edit/develop/docs-site/' },
    customCss: ['./src/styles/docs.css'],
    components: {
      SiteTitle: './src/components/SiteTitle.astro',
      Footer: './src/components/Footer.astro',
    },
    tableOfContents: { minHeadingLevel: 2, maxHeadingLevel: 3 },
    expressiveCode: { themes: ['github-dark', 'github-light'], styleOverrides: { borderRadius: '0.75rem' } },
    sidebar: [
      { label: 'Welcome', link: '/' },
      { label: 'Start here', items: [
        { label: '1. Choose an installation', slug: 'getting-started/installation' },
        { label: '2. Your first task', slug: 'tutorials/first-task' },
        { label: '3. Your first workflow', slug: 'tutorials/first-workflow' },
        { label: '4. Explore your executions', slug: 'tutorials/use-console' },
      ] },
      { label: 'Installation options', collapsed: true, items: [
        { label: 'Automatic local setup', slug: 'how-to/install-native' },
        { label: 'Docker Compose', slug: 'how-to/install-compose' },
        { label: 'Build the CLI from source', slug: 'how-to/build-cli-from-source' },
      ] },
      { label: 'Write and run programs', collapsed: true, items: [
        { label: 'Develop Python programs', slug: 'how-to/develop-python-programs' },
        { label: 'Build and publish programs', slug: 'how-to/build-and-publish-programs' },
        { label: 'Register a prepared program', slug: 'how-to/register-agent' },
        { label: 'Receive execution results', slug: 'how-to/receive-results' },
        { label: 'Run tasks in parallel', slug: 'how-to/parallel-tasks' },
        { label: 'Mix local work and branches', slug: 'how-to/fork-workflow-branches' },
        { label: 'Wait for an event', slug: 'how-to/wait-for-event' },
        { label: 'Require approval', slug: 'how-to/require-approval' },
        { label: 'Recover model and tool calls', slug: 'how-to/recover-agent-calls' },
      ] },
      { label: 'Integrations and examples', collapsed: true, items: [
        { label: 'Connect an MCP client', slug: 'how-to/connect-mcp' },
        { label: 'Build a tested change with Codex', slug: 'tutorials/codex-change-review' },
      ] },
      { label: 'Operate your installation', collapsed: true, items: [
        { label: 'Manage the local stack', slug: 'how-to/run-local-distribution' },
        { label: 'Configure observability', slug: 'how-to/configure-observability' },
        { label: 'Upgrade to 0.6.1', slug: 'how-to/upgrade-to-0-6-1' },
        { label: 'Advanced setup', collapsed: true, items: [
          { label: 'Manual archives and native services', slug: 'how-to/manual-native-installation' },
          { label: 'Build the full stack from source', slug: 'tutorials/run-locally' },
        ] },
        { label: 'Earlier upgrades', collapsed: true, items: [
          { label: 'Upgrade to 0.6.0', slug: 'how-to/upgrade-to-0-6' },
          { label: 'Upgrade to 0.5.0', slug: 'how-to/upgrade-to-0-5' },
          { label: 'Upgrade to 0.4.0', slug: 'how-to/upgrade-to-0-4' },
          { label: 'Upgrade to 0.3.1', slug: 'how-to/upgrade-to-0-3' },
          { label: 'Upgrade to 0.2.0', slug: 'how-to/upgrade-to-0-2' },
        ] },
      ] },
      { label: 'Reference', collapsed: true, items: [
        { label: 'Capabilities and availability', slug: 'reference/capabilities' },
        { label: 'Command-line interface', slug: 'reference/cli' },
        { label: 'Console', slug: 'reference/console' },
        { label: 'Workflow context', slug: 'reference/workflow-context' },
        { label: 'Python client', slug: 'reference/python-client' },
        { label: 'Releases and packages', slug: 'reference/releases' },
      ] },
      { label: 'Concepts', collapsed: true, items: [
        { label: 'Execution model', slug: 'concepts/execution-model' },
        { label: 'One self-hosted instance', slug: 'concepts/self-hosted-console' },
        { label: 'Queue delivery', slug: 'concepts/queue-delivery' },
        { label: 'Checkpoints and recovery', slug: 'concepts/checkpoints-and-recovery' },
      ] },
    ],
  })],
});
