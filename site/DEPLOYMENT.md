# Deployment

The ChasquiMQ docs site (this directory) is an Astro Starlight project that
deploys to **Cloudflare Pages** on every push to `main`. The deploy workflow
lives at [`.github/workflows/docs-deploy.yml`](../.github/workflows/docs-deploy.yml).

This guide covers the one-time Cloudflare setup, custom domains, local
preview, PR previews, and rollback.

## One-time Cloudflare Pages setup

You only do this once per repo. After that, every push to `main` deploys
automatically via GitHub Actions.

1. **Create a Cloudflare account** (free tier is fine) at
   <https://dash.cloudflare.com/sign-up>.

2. **Create a scoped API token** at
   <https://dash.cloudflare.com/profile/api-tokens> → *Create Token* →
   *Custom token*. Give it the minimum scopes:

   - **Account → Cloudflare Pages → Edit**
   - **Account → Account Settings → Read**
   - *Account Resources*: include only the account that will host the
     `chasquimq` Pages project.
   - *Zone Resources*: leave at the default (no zone access required for a
     `*.pages.dev` deploy; you'll add zone scope later if you attach a
     custom domain via this same token).

   Copy the token value — you won't see it again.

3. **Grab your Account ID** from the Cloudflare dashboard sidebar (any page
   under your account shows it on the right under *Account ID*) or from the
   URL: `https://dash.cloudflare.com/<account-id>/...`.

4. **Add both as GitHub repo secrets** at
   `https://github.com/jotarios/chasquimq/settings/secrets/actions`:

   | Secret name             | Value                                 |
   | ----------------------- | ------------------------------------- |
   | `CLOUDFLARE_API_TOKEN`  | The token from step 2                 |
   | `CLOUDFLARE_ACCOUNT_ID` | The Account ID from step 3            |

5. **Create the Pages project** named exactly `chasquimq`. Either:

   - **Via dashboard**: *Workers & Pages* → *Create application* → *Pages*
     → *Direct upload* → name it `chasquimq`. Skip the upload — the GitHub
     Actions workflow will push the first deploy.
   - **Via wrangler** (locally, after `npm i -g wrangler` and
     `wrangler login`):

     ```bash
     wrangler pages project create chasquimq --production-branch main
     ```

The next push to `main` that touches `site/**` will publish to
<https://chasquimq.pages.dev>. You can also trigger a deploy manually from
the GitHub Actions tab → *Docs Deploy* → *Run workflow*.

## Custom domain

Until you point a real domain at it, the site lives at the auto-assigned
`https://chasquimq.pages.dev`.

To attach a custom domain (e.g. `docs.chasquimq.dev`):

1. In the Cloudflare dashboard: *Workers & Pages* → `chasquimq` → *Custom
   domains* → *Set up a custom domain* → enter the hostname.
2. Cloudflare guides you through the DNS setup. You'll add **one** of:
   - **Apex / root** (`chasquimq.dev`): a `CNAME` flattened to
     `chasquimq.pages.dev` (Cloudflare DNS supports CNAME flattening at
     the apex; on other DNS providers, use the `A`/`AAAA` records the
     dashboard provides).
   - **Subdomain** (`docs.chasquimq.dev`): a plain
     `CNAME docs → chasquimq.pages.dev`.
3. Cloudflare auto-provisions a TLS certificate (Universal SSL). It's
   typically live within a few minutes; the dashboard shows status.

If your DNS already lives on Cloudflare, the dashboard creates the records
for you in one click.

## Local preview

From this directory:

```bash
cd site
npm install            # first time only
npm run dev            # http://localhost:4321 with hot reload
```

To test the *exact* output that Pages will serve (static HTML, no dev
server middleware):

```bash
npm run build && npm run preview
```

`npm run build` writes the static output to `site/dist/` — the same
artifact the GitHub Actions workflow uploads to Cloudflare.

## PR previews

Every PR that touches `site/**` runs a **build smoke test** in CI: the
workflow checks out, runs `npm ci` and `npm run build`, and uploads the
`site/dist/` artifact for download from the run summary. No secrets are
required, so PRs from forks work too.

Full per-PR live previews (each PR getting its own
`https://pr-N.chasquimq.pages.dev` URL via
`wrangler pages deploy site/dist --branch=PR-N`) are **deferred to v1.1**.
The build smoke catches compile-time regressions today; we'll add live
previews when there's enough cross-PR design churn to justify the extra
deploy traffic and cleanup logic.

## Rolling back

Every deploy is preserved on Cloudflare. If a bad deploy lands on `main`,
roll back without reverting code:

```bash
# List recent deploys (newest first; copy the ID of a known-good one)
wrangler pages deployment list --project-name=chasquimq

# Promote that deploy to production
wrangler pages deployment rollback --project-name=chasquimq <deployment-id>
```

You can also do this from the dashboard: *Workers & Pages* → `chasquimq` →
*Deployments* → pick a deploy → *…* → *Rollback to this deployment*.

After rolling back, fix forward in code and push again; the next push to
`main` will redeploy normally.
