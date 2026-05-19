// @ts-check
import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";

// https://astro.build/config
export default defineConfig({
  site: "https://chasquimq.io",
  integrations: [
    starlight({
      title: "ChasquiMQ",
      description: "The fastest open-source message broker for Redis.",
      logo: {
        src: "./src/assets/chasquimq-logo.svg",
        alt: "ChasquiMQ",
      },
      favicon: "/favicon.svg",
      head: [
        {
          tag: "meta",
          attrs: { property: "og:image", content: "https://chasquimq.io/og.png" },
        },
        {
          tag: "meta",
          attrs: { property: "og:image:width", content: "1200" },
        },
        {
          tag: "meta",
          attrs: { property: "og:image:height", content: "630" },
        },
        {
          tag: "meta",
          attrs: { property: "og:image:alt", content: "ChasquiMQ — the fastest open-source message broker for Redis." },
        },
        {
          tag: "meta",
          attrs: { name: "twitter:image", content: "https://chasquimq.io/og.png" },
        },
        {
          tag: "meta",
          attrs: { name: "twitter:image:alt", content: "ChasquiMQ — the fastest open-source message broker for Redis." },
        },
        {
          tag: "meta",
          attrs: { name: "twitter:site", content: "@chasquimq" },
        },
        {
          tag: "meta",
          attrs: { name: "twitter:creator", content: "@chasquimq" },
        },
      ],
      social: [
        {
          icon: "github",
          label: "GitHub",
          href: "https://github.com/jotarios/chasquimq",
        },
      ],
      editLink: {
        baseUrl:
          "https://github.com/jotarios/chasquimq/edit/main/site/",
      },
      customCss: ["./src/styles/tokens.css", "./src/styles/landing.css"],
      sidebar: [
        {
          label: "Start here",
          items: [
            { label: "Introduction", slug: "index" },
            { label: "Getting started", slug: "start/getting-started" },
            { label: "Your first job with retries", slug: "start/first-job" },
            {
              label: "Delayed and repeatable jobs",
              slug: "start/delayed-and-repeatable",
            },
            {
              label: "Inspecting with the CLI",
              slug: "start/inspecting-with-cli",
            },
          ],
        },
        {
          label: "Guides",
          items: [
            { label: "Overview", slug: "guides" },
            { label: "Configure retries", slug: "guides/configure-retries" },
            { label: "Route to the DLQ", slug: "guides/route-to-dlq" },
            { label: "Replay the DLQ", slug: "guides/replay-the-dlq" },
            {
              label: "Enable result storage",
              slug: "guides/enable-result-storage",
            },
            {
              label: "Schedule repeatable jobs",
              slug: "guides/schedule-repeatable-jobs",
            },
            { label: "Idempotent add", slug: "guides/idempotent-add" },
            { label: "Observe the engine", slug: "guides/observe-the-engine" },
            { label: "Tune for throughput", slug: "guides/tune-for-throughput" },
            {
              label: "Produce from AWS Lambda",
              slug: "guides/produce-from-aws-lambda",
            },
            {
              label: "Connect to a Redis Cluster",
              slug: "guides/connect-to-redis-cluster",
            },
            { label: "Migrate from BullMQ", slug: "guides/migrate-from-bullmq" },
            {
              label: "Migrate from Sidekiq or Celery",
              slug: "guides/migrate-from-sidekiq-celery",
            },
          ],
        },
        {
          label: "Reference",
          autogenerate: { directory: "reference" },
        },
        {
          label: "Concepts",
          items: [
            { label: "Overview", slug: "concepts" },
            {
              label: "Thinking in ChasquiMQ",
              slug: "concepts/thinking-in-chasquimq",
            },
            {
              label: "Redis Streams primer",
              slug: "concepts/redis-streams-primer",
            },
            {
              label: "Delivery semantics",
              slug: "concepts/delivery-semantics",
            },
            { label: "Retry and backoff", slug: "concepts/retry-and-backoff" },
            { label: "DLQ and recovery", slug: "concepts/dlq-and-recovery" },
            { label: "Result backends", slug: "concepts/result-backends" },
            { label: "The scheduler", slug: "concepts/the-scheduler" },
            { label: "Pause and resume", slug: "concepts/pause-and-resume" },
            { label: "Redis Cluster", slug: "concepts/redis-cluster" },
            {
              label: "Architecture decisions",
              slug: "concepts/architecture-decisions",
            },
            {
              label: "Performance trade-offs",
              slug: "concepts/performance-trade-offs",
            },
          ],
        },
        {
          label: "Benchmarks",
          items: [
            { label: "Overview", slug: "benchmarks" },
            { label: "Methodology", slug: "benchmarks/methodology" },
            { label: "The 1.0 numbers", slug: "benchmarks/the-1-0-numbers" },
            {
              label: "Regressions and floors",
              slug: "benchmarks/regressions-and-floors",
            },
          ],
        },
      ],
    }),
  ],
});
