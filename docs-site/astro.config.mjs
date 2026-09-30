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
      { label: 'Tutorials', items: [
        { label: 'Explore Console', slug: 'tutorials/use-console' },
        { label: 'Run Ledgence locally', slug: 'tutorials/run-locally' },
        { label: 'Your first workflow', slug: 'tutorials/first-workflow' },
        { label: 'Build a tested change with Codex', slug: 'tutorials/codex-change-review' },
      ] },
      { label: 'How-to guides', items: [
        { label: 'Register an agent', slug: 'how-to/register-agent' },
        { label: 'Install the native release', slug: 'how-to/install-native' },
        { label: 'Run tasks in parallel', slug: 'how-to/parallel-tasks' },
        { label: 'Mix local work and branches', slug: 'how-to/fork-workflow-branches' },
        { label: 'Wait for an event', slug: 'how-to/wait-for-event' },
      ] },
      { label: 'Reference', items: [
        { label: 'Command-line interface', slug: 'reference/cli' },
        { label: 'Console', slug: 'reference/console' },
        { label: 'Workflow context', slug: 'reference/workflow-context' },
        { label: 'Python client', slug: 'reference/python-client' },
        { label: 'Releases & packages', slug: 'reference/releases' },
      ] },
      { label: 'Concepts', items: [
        { label: 'One self-hosted instance', slug: 'concepts/self-hosted-console' },
        { label: 'Execution model', slug: 'concepts/execution-model' },
        { label: 'Checkpoints & recovery', slug: 'concepts/checkpoints-and-recovery' },
      ] },
    ],
  })],
});
