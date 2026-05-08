# ChasquiMQ docs site

The user-facing documentation for [ChasquiMQ](https://github.com/jotarios/chasquimq),
built with [Astro Starlight](https://starlight.astro.build/). Pagefind search
is included by default. Hosted on Cloudflare Pages.

## Local development

Requires Node 20+.

```bash
cd site
npm install
npm run dev      # serves at http://localhost:4321
npm run build    # static output → site/dist/
npm run preview  # serve the built output
```

## Project layout

```
site/
├── astro.config.mjs           # Starlight config (sidebar, social, edit links)
├── public/
│   ├── _headers               # Cloudflare Pages security + cache headers
│   └── _redirects             # placeholder
├── src/
│   ├── assets/                # logo + brand artwork
│   ├── content/docs/          # all documentation pages (Markdown / MDX)
│   │   ├── index.md           # landing page (placeholder)
│   │   ├── start/             # tutorials
│   │   ├── guides/            # how-to
│   │   ├── reference/         # API reference
│   │   ├── concepts/          # explanation
│   │   └── benchmarks/        # measured numbers
│   ├── styles/tokens.css      # design tokens (navy + electric cyan)
│   └── content.config.ts      # Starlight content collection
└── package.json
```

## Adding a page

Drop a Markdown file under `src/content/docs/<group>/`. The sidebar for
`guides`, `reference`, `concepts`, and `benchmarks` is auto-generated from
the directory; `start/` pages are explicitly listed in `astro.config.mjs`.

Frontmatter every page needs:

```yaml
---
title: Page title
description: One-line summary used by search and meta tags.
---
```

## Design tokens

`src/styles/tokens.css` defines the palette and font stack. Two themes:

- **Dark (default):** near-black surface, navy nav (`#0A2540`), electric cyan
  accent (`#00B8D4`).
- **Light:** white surface, navy nav (kept for brand consistency), darker
  cyan accent (`#00838F`) for AA contrast on white.

Fonts come from [Bunny Fonts](https://fonts.bunny.net/) (privacy-respecting
Google Fonts mirror): Geist (UI), Geist Mono (code), Instrument Serif
(display only).

## Deploying to Cloudflare Pages

1. **Project name:** `chasquimq`
2. **Framework preset:** Astro
3. **Build command:** `npm run build`
4. **Build output directory:** `site/dist`
5. **Root directory (advanced):** `site`
6. **Node version:** `20` (set `NODE_VERSION=20` env var, or via `.node-version`)

Pagefind search is built into Starlight and runs automatically during
`astro build` — no extra step needed.

The `public/_headers` file applies CSP-adjacent security headers
(`X-Frame-Options`, `X-Content-Type-Options`, `Referrer-Policy`,
`Permissions-Policy`, HSTS) and long-lived immutable caching for
`/_astro/*` hashed bundles.

## Editing flow

The "Edit this page" link on every doc points at
`https://github.com/jotarios/chasquimq/edit/main/site/<path>` — community
contributors can fix typos with a single click.
