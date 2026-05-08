# ChasquiMQ — Design System

> Source of truth for every visual and interaction decision on the docs site, README assets, and any user-facing surface that ships under the ChasquiMQ name.
>
> If a token, type ramp, spacing value, or motion duration is not in this file, it does not exist. Add it here first; reference it from `site/src/styles/tokens.css` second.

**Last updated:** 2026-05-08
**Owner:** Jorge Rios
**Status:** v1 — locked for the docs-site/scaffold cut.

---

## 1. Product context

**What:** ChasquiMQ is a Rust-native, Redis-Streams-backed message broker and background-job queue. Engine in Rust on `tokio`; first-class Node and Python bindings via NAPI-RS and PyO3.

**Who for:** Senior backend, platform, and SRE engineers running production job queues on Redis 8.x. They've operated Sidekiq, BullMQ, Celery, RQ, or Resque, hit the throughput or CPU ceiling, and want a drop-in shape (Queue / Worker / Job) without paying the per-job round-trip tax.

**Industry:** Backend infrastructure / developer tools. Open-source-first. Adoption is decided by reading docs in a single sitting and running a benchmark, not by sales motion.

**Why a design system at all:** the product is "fast and trustworthy systems software." If the docs site looks like a marketing landing page, the visual language undermines the technical claim. The system below exists to make the site look like the engine reads — disciplined, dense, no theatre.

---

## 2. Aesthetic direction

**Brutalist-restrained / systems-tool.** Type and whitespace do the work. Color is a scalpel, not a paint roller. Monospace shows up wherever the user is meant to read precision: code, telemetry, queue names, job IDs, key shortcuts, latency numbers. A single bright accent (electric blue) carries every link, every primary CTA, every focus ring, and nothing else.

**Reference points (energy, not imitation):**
- Rust Book — calm density, generous line-height on prose, code is a first-class citizen.
- redis.io / Redis docs — navy on white, monospace tables, no decorative gradients.
- Tailwind CSS docs — disciplined grid, tight vertical rhythm, ample inline code.
- Linear.com — hairline borders, deliberate use of weight, restraint in motion.

**Reference points (anti-references):**
- Generic SaaS landing pages with three columns of icon-in-circle "features."
- Product pages where a purple-pink gradient does the persuading.
- Pages where the only typeface is `system-ui` and the eye has nowhere to land.

**Memorable thing (the user-verifiable test):**

> When I open the homepage, the page reads like a piece of well-engineered terminal output: navy, mono, deliberate. One serif word in the hero ("Chasqui") tells me a human cared. The accent color appears once above the fold.

If that sentence is no longer true after a change, the change deviates from the system.

---

## 3. Brand voice (visual layer only)

The runner — a Chasqui, the Inca relay messenger — is the heritage motif. The name carries it; the visual layer should not over-explain it. **No illustrated runners as hero art.** A small monoline glyph (used as favicon, README badge, and one place in the docs nav) is the entire visual quota for the runner. Treat it like Rust's gear or Redis's cube: present, not loud.

The current logo lives at `docs/chasquimq.jpeg` (raster). It needs to be redrawn as SVG before launch — out of scope for this doc, but flagged.

---

## 4. Typography

Three families, loaded from **Bunny Fonts** (GDPR-clean, no Google CDN). All three are open-source.

| Role | Family | Weights used | Where |
|---|---|---|---|
| Body / UI | **Geist** | 400, 500, 600, 700 | Everything that isn't code or hero display |
| Code / data | **Geist Mono** | 400, 500, 600 | Code blocks, inline code, queue names, job IDs, shortcuts, telemetry, table numerics |
| Display | **Instrument Serif** | 400, 400 italic | Hero word(s) only; section openers on the homepage; never in-prose |

```css
/* Bunny Fonts — single request per family, no JS, GDPR-safe */
@import url('https://fonts.bunny.net/css?family=geist:400,500,600,700|geist-mono:400,500,600|instrument-serif:400,400i&display=swap');

--font-sans: 'Geist', ui-sans-serif, -apple-system, 'Segoe UI', sans-serif;
--font-mono: 'Geist Mono', ui-monospace, 'SF Mono', 'JetBrains Mono', Consolas, monospace;
--font-display: 'Instrument Serif', Georgia, serif;
```

### 4.1 Modular scale (rem-based, 16px root)

| Token | px | rem | Use |
|---|---:|---:|---|
| `--text-xs` | 13 | 0.8125 | Micro labels, table caption, footnotes |
| `--text-sm` | 14 | 0.875 | UI chrome, secondary text, table cells |
| `--text-base` | 16 | 1.0 | Body prose default |
| `--text-md` | 18 | 1.125 | Lede paragraph, callout body |
| `--text-lg` | 21 | 1.3125 | h4 / card title |
| `--text-xl` | 26 | 1.625 | h3 / section heading |
| `--text-2xl` | 34 | 2.125 | h2 / page title |
| `--text-3xl` | 48 | 3.0 | h1 / homepage section opener |
| `--text-4xl` | 64 | 4.0 | Hero display only |

### 4.2 Line-height & tracking

- Body prose: `line-height: 1.6`, `letter-spacing: 0`.
- Headings: `line-height: 1.15`, `letter-spacing: -0.01em` (sans), `-0.02em` (display serif).
- Code: `line-height: 1.55`, `letter-spacing: 0`.
- Tabular contexts (benchmarks, queue stats): `font-feature-settings: "tnum", "cv11"` to lock column widths.

### 4.3 Weight rules

- Body: 400. Bold body: 600 (never 700 in prose — too dense at 16px).
- UI labels and table headers: 500.
- Headings: 600.
- Hero display (Instrument Serif): 400, no bold variant exists and we don't want one.

### 4.4 Hero composition (the one allowed flourish)

Hero combines Geist (sans) and Instrument Serif (serif italic) on adjacent lines:

```
The fastest open-source                        ← Geist 600, --text-3xl
message broker for Redis.                      ← Instrument Serif 400 italic, --text-4xl
```

This is the only place serif appears. It signals "a human designed this, the runner has a name." Used a second time on the page, it dilutes; used on a subpage, it's broken.

---

## 5. Color

Two themes, defined as CSS custom properties, swapped via `data-theme="dark"` on `<html>`. Both pass WCAG AA on body text; `colors.html` shows the computed contrast ratios for every pair.

### 5.1 Light theme

| Token | Hex | Role |
|---|---|---|
| `--bg` | `#FFFFFF` | Page background |
| `--surface` | `#F7F8FA` | Card, sidebar, callout |
| `--surface-2` | `#EEF1F5` | Hover, table row stripe, code wrapper |
| `--border` | `#DDE3EB` | Hairline 1px borders |
| `--border-strong` | `#C4CDD9` | Form inputs, focused chrome |
| `--text` | `#0A2540` | Primary text — navy, not black |
| `--text-muted` | `#4A5A6E` | Captions, meta, table cells |
| `--text-subtle` | `#6B7A8F` | Placeholder, disabled labels |
| `--accent` | `#0066FF` | Links, primary CTA, focus ring |
| `--accent-hover` | `#0052CC` | Active CTA, hovered link |
| `--accent-soft` | `#E6F0FF` | Selection bg, accent surface |
| `--code-bg` | `#0A2540` | Code block bg (inverted — calls the eye) |
| `--code-border` | `#0A2540` | Same as bg; flat |
| `--code-text` | `#E6EEF8` | Code body |

> **Why navy `#0A2540` instead of black:** matches Redis/Stripe heritage, reads warmer at body sizes, gives the bright `#0066FF` accent room to feel electric instead of generic-link-blue.
>
> **Contrast notes (verified in `colors.html`):** `--text` on `--bg` = 15.5:1; `--text-muted` on `--bg` = 7.0:1 (AAA); `--accent` on `--bg` = 4.8:1 (AA, link-text safe). `--text-subtle` is exempt from AA — it's WCAG-classified disabled/placeholder, not body content.

### 5.2 Dark theme

| Token | Hex | Role |
|---|---|---|
| `--bg` | `#07101C` | Near-black navy |
| `--surface` | `#0E1B2C` | Card, sidebar |
| `--surface-2` | `#16263A` | Hover, code wrapper |
| `--border` | `#1F3247` | Hairline borders |
| `--border-strong` | `#2C435E` | Form inputs |
| `--text` | `#E6EEF8` | Primary text |
| `--text-muted` | `#94A6BC` | Caption, meta |
| `--text-subtle` | `#6E8197` | Placeholder, disabled |
| `--accent` | `#58B8FF` | Links, primary CTA, focus ring |
| `--accent-hover` | `#86CCFF` | Active / hovered |
| `--accent-soft` | `#102A44` | Selection bg, accent surface |
| `--code-bg` | `#040A12` | Slightly darker than page |
| `--code-border` | `#1F3247` | Hairline |
| `--code-text` | `#E6EEF8` | Code body |

> **Why `#58B8FF` and not the same `#0066FF`:** `#0066FF` on dark navy measures ~3.4:1 on body text — fails AA. `#58B8FF` on `#07101C` measures 8.8:1 on body, ~7.5:1 on `--surface`. Verified against the matrix in `colors.html`.

### 5.3 Semantic colors

Used sparingly: status badges, log levels, alert callouts. Never in body prose.

| Role | Light hex | Dark hex |
|---|---|---|
| Success | `#1F9E5C` | `#3DD68C` |
| Warn | `#C77700` | `#F2A93B` |
| Error | `#D7263D` | `#FF5670` |
| Info | `#0066FF` (= accent) | `#58B8FF` (= accent) |

### 5.4 Code-block syntax palette

Designed for Geist Mono on `--code-bg`. Keep distinct from semantic palette — green here is "string", not "success".

| Token | Light | Dark | Role |
|---|---|---|---|
| `--code-keyword` | `#FF7AB6` | `#FF8FBE` | `let`, `async`, `pub` |
| `--code-string` | `#7DD897` | `#86E89B` | string literals |
| `--code-number` | `#F2A93B` | `#F4B859` | numerics, ports |
| `--code-comment` | `#7E8A9C` | `#7E8A9C` | `//`, `#` |
| `--code-fn` | `#58B8FF` | `#86CCFF` | function names |
| `--code-type` | `#C792EA` | `#D5A8F2` | types, structs |

Light-mode code is intentionally inverted (light text on navy `#0A2540` block) — this is a deliberate choice, see §11 Risks taken.

### 5.5 Accent rule

**One accent moment per landing-page viewport.** On the homepage and other compositional pages (benchmarks index, comparison, the perf row), the eye should land on exactly one `--accent` element above the fold — typically the primary CTA. Demote competing accents in the same viewport to `--text` underline or `--text-muted`. The accent earns its loudness by being rare *in marketing context*.

**In-prose links use `--accent`.** Long-form pages (tutorials, guides, concepts, reference) carry many links by nature; demoting them all to underlined `--text` made the prose feel unstyled in practice and broke a hard web convention (links are blue and underlined). The "one accent moment" rule applies to compositional layouts, not to body prose.

Hover state deepens to `--accent-hover` (light: `#0052CC` for AAA on white; dark: `#86CCFF`). Visited links are not specially styled — Starlight's default visited handling is sufficient.

---

## 6. Spacing — 4px base

| Token | px | Use |
|---|---:|---|
| `--space-1` | 4 | Icon gap, badge inner |
| `--space-2` | 8 | Tight stack (label → input) |
| `--space-3` | 12 | Form rows, inline gaps |
| `--space-4` | 16 | Default paragraph rhythm |
| `--space-5` | 24 | Card inner, list spacing |
| `--space-6` | 32 | Section sub-block |
| `--space-7` | 48 | Section break |
| `--space-8` | 64 | Page section break |
| `--space-9` | 96 | Hero / major page rhythm |

**Vertical rhythm rule:** prose paragraphs are separated by `--space-4` (16px). Headings get `--space-7` above and `--space-3` below (asymmetric — the heading belongs to the content beneath it, not the content above).

---

## 7. Layout

- **Prose max-width:** `760px` — long-form docs page (60–75 chars/line at `--text-base`).
- **Landing section max-width:** `1100px` — homepage, benchmarks, comparison.
- **App shell max-width:** `1280px` — full chrome with sidebar.
- **Sidebar width:** `280px`, fixed.
- **Page gutter:** `--space-6` (32px) desktop, `--space-4` (16px) ≤640px.
- **Grid:** 12-column with `--space-5` (24px) gutter — only used for landing-page sections, never inside prose.
- **Vertical scroll anchor:** sticky H2 marker on the right rail of docs pages (Tailwind-style).

### 7.1 Breakpoints

| Name | Min width | Notes |
|---|---:|---|
| `sm` | 640px | Single-column mobile |
| `md` | 768px | Sidebar collapses to drawer |
| `lg` | 1024px | Sidebar persistent |
| `xl` | 1280px | App shell max width |

---

## 8. Border radius

Hierarchical, deliberately small. **No `border-radius: 9999px` pills as default CTA.** No bubble radius.

| Token | px | Use |
|---|---:|---|
| `--radius-sm` | 2 | Inline tag, kbd shortcut |
| `--radius-md` | 4 | Buttons, inputs, badges |
| `--radius-lg` | 6 | Cards, callouts, popovers |
| `--radius-xl` | 8 | Modals only |
| `--radius-none` | 0 | **Code blocks, full-bleed sections, terminal output** |

Code blocks are `--radius-none` on purpose: they read as embedded terminal output, not as decorative cards. Terminals have square corners.

---

## 9. Motion

Minimal-functional. Motion exists to confirm a state change, never to perform.

| Token | Duration | Easing | Use |
|---|---:|---|---|
| `--motion-instant` | 80ms | linear | Pressed states, focus ring appearance |
| `--motion-fast` | 150ms | `cubic-bezier(0.2, 0, 0, 1)` | Hovers, link color |
| `--motion-medium` | 250ms | `cubic-bezier(0.2, 0, 0, 1)` | Menu, popover, drawer |
| `--motion-slow` | 400ms | `cubic-bezier(0.4, 0, 0.2, 1)` | Theme toggle (cross-fade only) |

**Forbidden:**
- Scroll-driven animations on the homepage.
- Number "count-up" on benchmark figures.
- Fade-in-on-scroll for body paragraphs.
- Any `transition: all`.

`prefers-reduced-motion: reduce` collapses every duration above to `0ms` except focus-ring appearance.

---

## 10. Component primitives

### 10.1 Buttons

Four variants. Height `36px` (compact, list-friendly) or `44px` (touch-comfortable, hero). Padding `0 16px`. Border-radius `--radius-md`. Font: Geist 500, `--text-sm`.

| Variant | Light bg | Light text | Border | Use |
|---|---|---|---|---|
| Primary | `--accent` | `#FFFFFF` | none | One per viewport — install/quickstart CTA |
| Secondary | `--bg` | `--text` | `1px solid --border-strong` | "View benchmarks", "Compare" |
| Ghost | transparent | `--text` | none, hover bg `--surface-2` | Toolbar, in-nav |
| Destructive | `--bg` | `#D7263D` | `1px solid #D7263D` | DLQ purge, queue drop confirm |

Hover states shift `background-color` only — never `transform: scale()`. Active state shifts text down 1px and saturates background by ~10%.

### 10.2 Inputs

Border `1px solid --border-strong`, radius `--radius-md`, padding `0 12px`, height `36px`. Focused state: border `--accent`, outer ring `0 0 0 3px var(--accent-soft)`. No drop-shadow chrome.

### 10.3 Code blocks

```
┌─ Geist Mono 400, 14px, line-height 1.55
│  Background: --code-bg (navy on light, near-black on dark)
│  Text: --code-text
│  Padding: 16px 20px
│  Radius: 0
│  Optional header strip: --surface-2 with filename in --text-muted
└─ Inline code: --surface-2 background, --text color, --radius-sm, padding 1px 6px
```

Inline code uses page-tone surface, not the inverted block — readability inside prose matters more than aesthetic consistency.

### 10.4 Tables

- Header row: Geist 500, `--text-sm`, `--text-muted`, uppercase tracking `0.04em`.
- Body row: Geist 400, `--text-sm`, `--text`.
- Numerics: `font-feature-settings: "tnum"`, right-aligned.
- Hairline `--border` between rows; no zebra stripes by default (only when row count > 8).
- No outer border; the page does the framing.

### 10.5 Callouts

`Info`, `Warn`, `Note` only. Left border `4px solid` semantic color. Background `--surface`. Padding `--space-5`. The colored stripe is the entire signal — no icon-in-circle.

### 10.6 Keyboard shortcuts

`<kbd>` styled as Geist Mono `--text-xs`, `--surface-2` background, `1px solid --border`, `--radius-sm`, padding `1px 6px`, displayed inline.

### 10.7 Navigation chrome

The two persistent UI elements: the top header and the docs sidebar. Both visible on every page; both must read as "the building," not "the content."

**Top header** — full-width, navy in both themes for brand consistency:

- Background: `#0A2540` (logo navy). Same color in light AND dark to give the brand mark one consistent home across themes. The only element in the system that ignores theme.
- Text and links: `--code-text` (`#E6EEF8`).
- Search box: `rgba(255,255,255,0.06)` background, `#B9C1CB` placeholder, `1px solid rgba(255,255,255,0.08)`. On focus: ring becomes `--accent` (cyan-on-navy in dark, blue-on-navy in light — both pass AA).
- Bottom border: `1px solid rgba(255,255,255,0.06)` — barely visible, separates header from page.
- Height: 60px desktop, 56px mobile. Logo + product name flush left, search center-right, theme toggle far right.

**Sidebar** — left rail, persistent on `lg` and above, collapses to a drawer below `md`:

- Background: `--surface` (`#F7F8FA` light, `#0E1B2C` dark) — quieter than the page.
- Width: 280px fixed.
- Hairline border on the right: `1px solid --border`.
- Group headings: Geist 600, `--text-sm`, `--text-muted`, `letter-spacing: 0.04em`, uppercase.
- Items: Geist 400, `--text-sm`, `--text-muted` resting, `--text` on hover.
- Active item: `--text` color, `--accent-soft` background, `2px solid --accent` left border. No fill, no pill.
- Item padding: `--space-2 --space-4` (8px 16px). Items click anywhere in their row.
- Internal scroll on overflow with `--surface-2` thin scrollbar.

**Mobile drawer** (≤768px):

- Trigger: `Menu` icon in the top-left of the header, 24px Lucide.
- Drawer overlay: 80% width (max 320px), slides in from left.
- Backdrop: `rgba(7,16,28,0.6)` (dark navy, 60% opacity), click-to-dismiss.
- Animation: `--motion-medium` (250ms) translateX. Respects `prefers-reduced-motion`.

**Right rail (table-of-contents on docs pages)** — Tailwind-style sticky outline:

- Background: transparent.
- Items: Geist 400, `--text-xs`, `--text-muted` resting, `--text` on hover, `--accent` for the active heading.
- Active indicator: `2px solid --accent` left border on the active item, no fill.
- Width: 200px. Hidden below `lg`.

---

## 11. Risks taken (and why)

A design system that doesn't take a risk is just a parts catalog. The three load-bearing risks here:

1. **Instrument Serif in the hero.** Atypical for systems / devtool products — most reach for sans-only. We use it once, italic, paired with Geist sans. Why: ChasquiMQ literally means "messenger / relay runner" in Quechua. The serif is the human in the engine; without it, the page is indistinguishable from any other Rust devtool. **Risk:** reads as decorative to engineers who skim. **Mitigation:** capped at one instance per page; never in prose; never on subpages.

2. **Tighter spacing than typical SaaS docs.** Section breaks at 64–96px instead of the 120–160px the post-Stripe SaaS template defaults to. Tailwind-density rather than Vercel-density. Why: dense content respects engineer time and signals seriousness. **Risk:** reads "cramped" to a marketing reviewer. **Mitigation:** prose `line-height: 1.6` keeps individual paragraphs breathable; the density is between paragraphs, not within them.

3. **Monochrome-first; one accent moment per viewport.** Most docs sites distribute accent color across links, badges, CTAs, callouts, and graphs. We strictly demote in-prose links to underlined `--text` on long-form pages, reserving `--accent` for the primary CTA and the active nav item. Why: a single bright moment is more memorable than ten. **Risk:** "the page looks unstyled" on first glance. **Mitigation:** the typography ramp and the hero serif do the heavy lifting; the accent earns its loudness.

4. **Inverted (dark) code blocks on the light theme.** Code reads as terminal output, not as a card. **Risk:** breaks the soft-card aesthetic Stripe / Linear use. **Mitigation:** consistent — Rust Book and the official Redis docs do the same.

---

## 12. Anti-patterns — explicitly forbidden

- Purple, pink, or pink-to-orange gradients anywhere. The hero is not a sunset.
- Three-column "feature grid" with icon-in-circle + 30-word blurb. The README does this; the site doesn't.
- Gradient CTAs. Solid `--accent`, end of conversation.
- Centered-everything pages. Left-align prose; the eye starts on the left.
- Hero stock illustrations of abstract servers, robots, or connection lines.
- `system-ui` as primary font. We loaded Geist for a reason.
- Animated number count-ups on benchmark figures. The number is true; performing it makes it look like a sales claim.
- Drop shadows on cards. Hairline borders only.
- `border-radius: 9999px` as the default CTA shape.
- "Trusted by" logo strips. We don't have the logos and we wouldn't list them this way if we did.
- Pop-over chat widgets, cookie banners that aren't strictly required, exit-intent modals.

---

## 13. Accessibility

**WCAG 2.2 AA minimum on both themes.** Verified for every text/bg pair in `colors.html`.

- Focus ring: **2px `--accent`, 2px offset, 0 inner.** Visible on every focusable element. No `outline: none` without an explicit replacement.
- Tab order matches reading order. No `tabindex > 0`.
- All form inputs have `<label>`. `aria-describedby` carries help text and errors.
- Icon-only buttons: `aria-label` required.
- Theme toggle: `aria-pressed` reflects state; the toggle itself is keyboard-operable and respects `prefers-color-scheme` on first load.
- Animations gated by `prefers-reduced-motion`.
- Tabular numerics on every data table — predictable column widths help scanning, especially for users on screen magnifiers.
- Code blocks have a copy button with a `Copied` confirmation announced to screen readers via `aria-live="polite"`.
- Minimum touch target: 36×36px (40×40px on mobile-only controls).

---

## 14. Theme toggle

Three states: `system` (default), `light`, `dark`. Persisted in `localStorage` under key `chasqui:theme`. Applied as `data-theme="light|dark"` on `<html>`. First-paint prevention: a tiny inline script at the top of `<head>` reads `localStorage` synchronously and sets the attribute before stylesheets resolve, to avoid the FOUC flash. The toggle UI is a 3-state pill in the top-right of the docs nav, never a single sun-moon icon — explicit `system` matters.

---

## 15. Iconography

Lucide (open-source, MIT, monoline). 1.5px stroke, `currentColor`. Sized 16px (inline / button), 20px (nav), 24px (callout). Never filled, never multicolor. If a use case wants a colored or filled icon, it doesn't ship.

The runner glyph is the one custom asset — treat it like the React or Rust logo: no recoloring, no remixing, no merging into headlines.

---

## 16. Imagery

- **Allowed:** screenshots of real CLI output, real `chasqui` operator screens, real benchmark charts (PNG export from Plotly / cairo).
- **Allowed:** small monoline architecture diagrams in two colors (`--text` + `--accent`). SVG only.
- **Forbidden:** stock illustrations, AI-generated art, photographic backgrounds, isometric server racks.
- All images have `alt`. Decorative images use `alt=""` and `role="presentation"`.

---

## 17. Decisions log

| Date | Decision | Why |
|---|---|---|
| 2026-05-08 | Lock Geist + Geist Mono + Instrument Serif via Bunny Fonts | Three is the budget; serif is the load-bearing flourish; Bunny Fonts because GDPR + no Google CDN dep |
| 2026-05-08 | Light-mode accent `#0066FF` (electric blue) over cyan `#00B4D8` | Cyan AA-fails on body links at body size against white; blue measures 4.83:1 (AA for normal text). Hover deepens to `#0052CC` (≈7.0:1, AAA-equivalent) |
| 2026-05-08 | Dark-mode accent `#58B8FF` (lighter), not same `#0066FF` | `#0066FF` on `#07101C` is 3.8:1 — fails AA. Lift accent for the dark theme |
| 2026-05-09 | Relax §5.5: in-prose links keep `--accent`, "one accent moment" applies to landing/compositional pages only | Demoting prose links broke the "links are blue" web convention and made long-form pages read as unstyled. Caught in /plan-design-review |
| 2026-05-09 | Add §10.7 Navigation chrome (top header + sidebar + mobile drawer) | Plan covered every other component but skipped the two persistent UI elements. Future redesigns need an anchor |
| 2026-05-09 | Override Starlight Aside variants to remove default purple tip color | Default `:::tip` aside used `--sl-color-purple-*`, violating §12 ("no purple anywhere"). Mapped all 4 variants (note/tip/caution/danger) to §5.3 semantic palette with §10.5 hairline-border treatment |
| 2026-05-09 | Document browser baseline: Safari 16.2+, Chrome 111+, Firefox 113+ | We use `color-mix(in oklab)` in badges and hover states. Baseline locked to 2023+ browsers |
| 2026-05-08 | Hero word in Instrument Serif italic, capped at one per page | Adds a human signal; would dilute if reused |
| 2026-05-08 | Code blocks `--radius-none` and inverted on light theme | Reads as terminal output; consistent with Rust Book / Redis docs |
| 2026-05-08 | Spacing scale stops at 96px (`--space-9`) | Anything bigger is theatre; if a section needs more rhythm, the section is too long |
| 2026-05-08 | No accent on in-prose links by default — underlined `--text` | Preserves the "one accent per viewport" rule; matches Linear and Stripe docs |
| 2026-05-08 | Lucide icons only, monoline, 1.5px stroke | Single visual grammar; avoids the icon-set zoo most docs sites accumulate |

---

## 18. How to use this document

1. **Before starting any UI work**, read this file. If a token is missing, add it here, then to `site/src/styles/tokens.css`.
2. **Token names are stable.** Don't rename a token to fit a component; rename the component.
3. **When in doubt, demote.** Smaller, calmer, more monochrome. The product is fast software; the page should feel like a quiet, well-lit room.
4. **Changes to this file go through PR review.** Add a row to §17 Decisions log explaining *why*.

---

## 19. Companion files

- [`preview.html`](./preview.html) — full-page specimen showing every primitive together in light and dark.
- [`colors.html`](./colors.html) — every color pair with computed WCAG contrast ratios.
- [`typography.html`](./typography.html) — type ramp specimen for all three families.

These are static, frameworkless HTML. They render in any browser. They are the visual contract.

---

## 20. Browser baseline

The site uses modern CSS deliberately. Minimum supported browser versions:

| Browser | Minimum | Why |
|---|---|---|
| Safari | 16.2 (Dec 2022) | `color-mix(in oklab)`, `:has()` |
| Chrome / Edge | 111 (Mar 2023) | `color-mix(in oklab)` |
| Firefox | 113 (May 2023) | `color-mix(in oklab)` |

Specific modern features in use:

- `color-mix(in oklab, ...)` — used in badge backgrounds, log-header tints, and destructive-button hover. Falls back gracefully on older browsers (the mixed color is the only fallback, but the unmixed property is never the primary signal — borders and text carry the meaning).
- `:has()` — used to hide Starlight's empty splash hero container on the homepage.
- CSS custom properties — non-negotiable.
- `mask-image` with `currentColor` background — used for the language brand-marks in the segmented control.

We do **not** target IE11, Safari 15, or Chrome 110. Visitors on those browsers see a degraded but readable experience.
