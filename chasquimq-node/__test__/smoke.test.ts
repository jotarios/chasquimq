import { describe, it, expect } from 'vitest'
// `engineVersion` is exposed by the native binding and re-exported
// from the package root. Smoke-tests that the napi build chain
// produced a loadable `.node`.
import { engineVersion } from '../dist/index.js'

describe('smoke', () => {
  it('returns engine version', () => {
    expect(engineVersion()).toMatch(/^\d+\.\d+\.\d+/)
  })
})
