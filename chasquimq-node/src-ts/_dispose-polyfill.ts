// `Symbol.asyncDispose` shipped in Node 20.4 / V8 11.7. The shim's
// `package.json` advertises `"engines": { "node": ">= 18" }`, so on
// Node 18.x the symbol is `undefined` and `class { async [undefined]() }`
// would silently install a dead method on key `"undefined"`. Polyfill
// once at module load so `await using` integration works on all
// supported Node versions. Imported from every `src-ts/` file that
// declares `[Symbol.asyncDispose]` so subpath imports of those files
// (bypassing `index.ts`) still get the shim.
//
// `??=` keeps the platform value when present (Node 20.4+); only Node
// 18 actually picks up our `Symbol.for(...)` shim. The well-known-
// symbol name matches the TC39 spec so two modules that both polyfill
// share the same registered symbol via `Symbol.for`.
;(Symbol as { asyncDispose?: symbol }).asyncDispose ??= Symbol.for(
  "Symbol.asyncDispose",
);
