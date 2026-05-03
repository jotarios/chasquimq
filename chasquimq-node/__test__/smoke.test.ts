import { describe, it, expect } from 'vitest'
import { engineVersion } from '..' // typed by index.d.ts after build

describe('smoke', () => {
  it('returns engine version', () => {
    expect(engineVersion()).toMatch(/^\d+\.\d+\.\d+/)
  })
})
