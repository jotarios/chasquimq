import { describe, it, expect } from 'vitest'
// `engineVersion` is exposed by the native binding — it's the smoke
// test that the napi build chain produced a loadable `.node`. The
// high-level shim re-exports it from the same path; either works.
import { engineVersion } from '../index.js'

describe('smoke', () => {
  it('returns engine version', () => {
    expect(engineVersion()).toMatch(/^\d+\.\d+\.\d+/)
  })
})
