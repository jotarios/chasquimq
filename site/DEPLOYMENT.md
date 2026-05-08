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

## Custom domain — chasquimq.io

The production hostname is **`chasquimq.io`**, registered at Namecheap. We
move DNS to Cloudflare (rather than CNAMEing from Namecheap) so the apex
record works cleanly, the cert auto-provisions, and we get CF analytics +
bot protection for free. Total flip time: 5 minutes of clicks plus 1–24
hours of DNS propagation.

### One-time setup (in order)

1. **Add the zone in Cloudflare.** Dashboard → *Add a domain* → enter
   `chasquimq.io` → *Free* plan. Cloudflare scans Namecheap's existing
   DNS and imports any records it finds. There shouldn't be much for a
   freshly-registered domain.

2. **Copy the two Cloudflare nameservers.** They look like
   `ada.ns.cloudflare.com` and `kai.ns.cloudflare.com` (the names rotate
   per zone). The dashboard shows them on the next screen.

3. **Swap nameservers at Namecheap.** Namecheap dashboard → *Domain List*
   → *Manage* on `chasquimq.io` → *Nameservers* → *Custom DNS* → paste
   the two CF nameservers → save. **This is the load-bearing change.**
   Propagation usually completes within an hour but can take up to 24h.
   Watch the CF zone overview; status flips from *Pending* to *Active*
   once it sees the nameserver change.

4. **Attach the domain to the Pages project.** CF dashboard →
   *Workers & Pages* → `chasquimq` → *Custom domains* → *Set up a custom
   domain* → enter `chasquimq.io`. Because DNS is already in CF, this is
   a one-click setup: CF auto-creates the CNAME-flattened record at the
   apex and provisions a Universal SSL cert. Live within ~5 minutes of
   the zone going Active.

5. **Add `www.chasquimq.io` as a redirect** (optional but recommended).
   Same flow: *Custom domains* → *Set up* → `www.chasquimq.io`. Then in
   *Rules* → *Redirect Rules* → add a 301 from
   `(http.host eq "www.chasquimq.io")` to `https://chasquimq.io/$1`.
   Keeps the canonical URL singular.

6. **Update the Astro site config** to use the production URL. After the
   domain is live, change `astro.config.mjs` `site:` from
   `https://chasquimq.pages.dev` to `https://chasquimq.io` so the
   sitemap, canonical tags, and OG URLs reflect the canonical host.
   Commit, push, redeploy. (Skip this step until DNS is Active — until
   then, `chasquimq.pages.dev` is the only working URL.)

### Until DNS propagates

The site is reachable at `https://chasquimq.pages.dev` from the moment
the first deploy lands, regardless of whether the custom domain is set
up yet. You can hand out that URL while waiting on nameserver
propagation. Once `chasquimq.io` flips to Active, both URLs work; CF
serves the same artifact from both edges.

### If you change your mind on DNS provider later

Cloudflare lets you remove the zone and revert nameservers at Namecheap
at any time. The Pages project itself is unaffected — it lives at
`*.pages.dev` regardless of which DNS provider points at it. Migration
out is one-way for active visitors during the propagation window
(usually a few hours), so plan migrations during low-traffic times.

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
