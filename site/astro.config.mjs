// @ts-check
import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";

// https://astro.build/config
export default defineConfig({
  site: "https://chasquimq.pages.dev",
  integrations: [
    starlight({
      title: "ChasquiMQ",
      description: "The fastest open-source message broker for Redis.",
      logo: {
        src: "./src/assets/chasquimq.jpeg",
        alt: "ChasquiMQ",
      },
      favicon: "/favicon.jpeg",
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
      customCss: ["./src/styles/tokens.css"],
      sidebar: [
        {
          label: "Start here",
          items: [
            { label: "Introduction", slug: "index" },
            { label: "Getting started", slug: "start/getting-started" },
          ],
        },
        {
          label: "Guides",
          autogenerate: { directory: "guides" },
        },
        {
          label: "Reference",
          autogenerate: { directory: "reference" },
        },
        {
          label: "Concepts",
          autogenerate: { directory: "concepts" },
        },
        {
          label: "Benchmarks",
          autogenerate: { directory: "benchmarks" },
        },
      ],
    }),
  ],
});
