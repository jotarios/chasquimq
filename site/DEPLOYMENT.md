# Deployment

The ChasquiMQ docs site (this directory) is an [Astro Starlight](https://starlight.astro.build) project that deploys to **Cloudflare Pages** on every push to `main`. The deploy workflow lives at [`.github/workflows/docs-deploy.yml`](../.github/workflows/docs-deploy.yml).

This file covers what you need to know as a contributor: how to preview the site locally, what runs on PRs, and how to roll back if a deploy goes bad.

For maintainer-only operator setup (Cloudflare account, secrets, custom domain), see the private operations notes — they aren't tracked in this repo.

## Local preview

From this directory:

```bash
cd site
npm install            # first time only
npm run dev            # http://localhost:4321 with hot reload
```

To test the *exact* output Pages will serve (static HTML, no dev-server middleware):

```bash
npm run build && npm run preview
```

`npm run build` writes the static output to `site/dist/` — the same artifact GitHub Actions uploads to Cloudflare.

## PR previews

Every PR that touches `site/**` runs a **build smoke test** in CI: the workflow checks out, runs `npm ci` and `npm run build`, and uploads the `site/dist/` artifact for download from the run summary. No secrets are required, so PRs from forks work too.

Full per-PR live previews (each PR getting its own `https://pr-N.chasquimq.pages.dev` URL via `wrangler pages deploy site/dist --branch=PR-N`) are **deferred to v1.1**. The build smoke catches compile-time regressions today; we'll add live previews when there's enough cross-PR design churn to justify the extra deploy traffic and cleanup logic.

## Rolling back

Every deploy is preserved on Cloudflare. If a bad deploy lands on `main`, roll back without reverting code:

```bash
# List recent deploys (newest first; copy the ID of a known-good one)
wrangler pages deployment list --project-name=chasquimq

# Promote that deploy to production
wrangler pages deployment rollback --project-name=chasquimq <deployment-id>
```

You can also do this from the Cloudflare dashboard: *Workers & Pages* → `chasquimq` → *Deployments* → pick a deploy → *…* → *Rollback to this deployment*.

After rolling back, fix forward in code and push again; the next push to `main` redeploys normally.
