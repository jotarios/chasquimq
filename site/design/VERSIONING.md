# Docs versioning policy

> Operator-facing decision doc. Not a public site page. The site itself follows the "v1.x = current" model below; this file captures *why* and what changes when v2.0 comes.

**Last updated:** 2026-05-08
**Status:** v1.x current; v2.0 versioning plan deferred until v2.0 work begins.

---

## Today (v1.x): main → live, single version

The site at chasquimq.io serves **one version**: whatever's on `main`. Push to `main` → deploys in 90s. No staging, no version switcher, no version-pinned URLs.

This works because:

1. **One major version exists.** All users are on `chasquimq*` 1.x. There's no v0 long tail to support.
2. **The audience reads docs to install or debug.** They want the current truth, not historical truth.
3. **Binary versions are immutable.** crates.io / npm / PyPI / GitHub Releases lock past versions forever. A user installing 1.0.3 today gets the 1.0.3 binary; the docs at chasquimq.io describe 1.1.0 (latest). The mismatch is small and named.

### Known gap: docs lead the binary

When changes land on `main` that affect CLI flags, API surface, or behavior, the **docs reflect those changes immediately** but the **binary distributed to users is still the last released version**. Examples:

- PR #110 promoted `--redis-url` to a global CLI flag. The site's `/reference/cli/` shows the new help layout. The actual `chasqui` binary on crates.io / Releases is still v1.1.0 with the old flag layout.
- A user reading the docs and running `chasqui --help` sees different things until we cut a release.

Mitigation: cut releases more often, OR add a "Reflects main; latest released is v1.1.0" callout on volatile pages. Not doing either today; flagging for awareness.

---

## When v2.0 ships: switch to versioned docs

The moment we cut a v2.0 with breaking changes (Rust API, Node shim, Python shim, CLI flag breakage, on-wire format change), the single-version model breaks down. Users running 1.x in prod won't be able to read 1.x docs anymore. Time to add the version dimension.

### The plan, when v2.0 work starts

1. **Pick the model.** Two strategies, pick before v2.0 lands:
   - **Snapshot per major.** `/v1/`, `/v2/` paths. Old version frozen at v1.x final state, new version is whatever's on `main`. Simplest.
   - **Snapshot per release.** Every released minor or patch gets a frozen snapshot. Most rigorous, most ceremony, doesn't fit v1.x's release cadence.
   - **Recommendation:** snapshot per major. Fits a small project; matches what Vue 2/3, React 17/18, Vite 5/6 do.

2. **Tool:** `starlight-versions` (`@hideoo/starlight-versions` on npm). MIT-licensed plugin maintained by HiDeoo, who's a Starlight core maintainer. Opinionated, integrates with Starlight's sidebar, generates the `/v1/` paths automatically, handles the version switcher UI. As of 2026-05, "still in early development" but the right tool when the time comes.

3. **Mechanics:**
   - Right before v2.0 ships, run the plugin's freeze step on the current docs tree → snapshots `site/src/content/docs/` to `site/src/content/docs/v1/` (frozen).
   - Continue editing the unfrozen tree for v2.x changes.
   - Plugin renders a sidebar version switcher and `/v1/` URLs automatically.
   - Set canonical link tags to point at `/latest/` for SEO; `/v1/` pages are noindex or canonical-to-themselves depending on traffic.

4. **What needs to be true before flipping:** the doc tree should be reasonably stable (no half-finished sections), and at least one v2.0 RC should exist so the "latest" branch has actual v2 content to differentiate from v1.

### What we are NOT doing now

- No `/v1/` URL prefix (everything lives at `/`).
- No version dimension in frontmatter — pages have no `version:` field. When freezing for v1, the plugin treats the entire current tree as v1 by directory rename.
- No staging subdomain. Push to main → live.
- No tag-gated promotion. Cloudflare Pages rollback button is the safety net.

Resisting these pre-v2.0 because: every one of them adds operator surface area (a switcher to maintain, a redirect strategy, a build matrix) and none of them solves a problem we have today.

---

## Decisions log

| Date | Decision | Why |
|---|---|---|
| 2026-05-08 | v1.x docs are unversioned: main → live, no switcher | One major version, small project, audience wants current truth. Adding the version dimension now would be ceremony without value |
| 2026-05-08 | When v2.0 ships, use `starlight-versions` plugin with snapshot-per-major | HiDeoo plugin (Starlight core maintainer); fits the small-project pattern (Vue 2/3, React 17/18); avoids hand-rolling. Captured the dependency now so v2.0 work doesn't re-litigate it |
| 2026-05-08 | Accept the docs-leads-binary gap on `main`; do NOT add a "reflects main" callout yet | Gap is small and honest; release cadence is the right lever. Revisit if a user complains |

---

## Related

- [DESIGN.md](./DESIGN.md) — visual design system. Independent of versioning.
- [DEPLOYMENT.md](../DEPLOYMENT.md) — how the site deploys today. Single environment.
- [`starlight-versions` plugin](https://github.com/HiDeoo/starlight-versions) — adopt at v2.0.
