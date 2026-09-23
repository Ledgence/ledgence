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
        { label: 'Run Ledgence locally', slug: 'tutorials/run-locally' },
        { label: 'Your first workflow', slug: 'tutorials/first-workflow' },
      ] },
      { label: 'How-to guides', items: [
        { label: 'Run tasks in parallel', slug: 'how-to/parallel-tasks' },
        { label: 'Wait for an event', slug: 'how-to/wait-for-event' },
      ] },
      { label: 'Reference', items: [
        { label: 'Workflow context', slug: 'reference/workflow-context' },
        { label: 'Python client', slug: 'reference/python-client' },
        { label: 'Registry packages', link: 'https://github.com/Ledgence/ledgence/blob/develop/docs/registry-packages.md' },
      ] },
      { label: 'Concepts', items: [
        { label: 'Execution model', slug: 'concepts/execution-model' },
        { label: 'Checkpoints & recovery', slug: 'concepts/checkpoints-and-recovery' },
      ] },
    ],
  })],
});
