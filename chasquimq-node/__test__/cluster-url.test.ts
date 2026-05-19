import { describe, it, expect } from 'vitest'
import { __urlInternals } from '../dist/queue.js'

const { buildRedisUrl, applyTls } = __urlInternals

describe('applyTls', () => {
  it('passes through when tls is false', () => {
    expect(applyTls('redis://h:6379', false)).toBe('redis://h:6379')
    expect(applyTls('redis-cluster://h:6379', false)).toBe(
      'redis-cluster://h:6379',
    )
    expect(applyTls('h:6379', false)).toBe('h:6379')
  })

  it('upgrades the plain redis scheme', () => {
    expect(applyTls('redis://h:6379', true)).toBe('rediss://h:6379')
  })

  it('leaves an already-TLS scheme untouched', () => {
    expect(applyTls('rediss://h:6379', true)).toBe('rediss://h:6379')
    expect(applyTls('rediss-cluster://h:6379', true)).toBe(
      'rediss-cluster://h:6379',
    )
  })

  it('preserves the cluster scheme when layering TLS (regression)', () => {
    // The old prefix-strip produced "rediss://redis-cluster://...",
    // silently breaking TLS Redis Cluster. fred routes by scheme.
    expect(applyTls('redis-cluster://h:6379', true)).toBe(
      'rediss-cluster://h:6379',
    )
    expect(applyTls('redis-cluster://h:6379?node=h2:6380', true)).toBe(
      'rediss-cluster://h:6379?node=h2:6380',
    )
  })

  it('upgrades valkey aliases', () => {
    expect(applyTls('valkey://h:6379', true)).toBe('valkeys://h:6379')
    expect(applyTls('valkey-cluster://h:6379', true)).toBe(
      'valkeys-cluster://h:6379',
    )
    expect(applyTls('valkeys-cluster://h:6379', true)).toBe(
      'valkeys-cluster://h:6379',
    )
  })

  it('prepends rediss:// for a schemeless host', () => {
    expect(applyTls('my-cluster.cache.amazonaws.com:6379', true)).toBe(
      'rediss://my-cluster.cache.amazonaws.com:6379',
    )
  })

  it('is case-insensitive on the scheme', () => {
    expect(applyTls('REDIS-CLUSTER://h:6379', true)).toBe(
      'rediss-cluster://h:6379',
    )
    expect(applyTls('REDISS://h:6379', true)).toBe('REDISS://h:6379')
  })
})

describe('buildRedisUrl', () => {
  it('builds a single-node url by default', () => {
    expect(buildRedisUrl({ host: 'h', port: 6379 })).toBe('redis://h:6379')
  })

  it('builds a TLS single-node url', () => {
    expect(buildRedisUrl({ host: 'h', port: 6379, tls: true })).toBe(
      'rediss://h:6379',
    )
  })

  it('builds a cluster url when cluster is true', () => {
    expect(buildRedisUrl({ host: 'h', port: 6379, cluster: true })).toBe(
      'redis-cluster://h:6379',
    )
  })

  it('builds a TLS cluster url when cluster and tls are both true', () => {
    expect(
      buildRedisUrl({ host: 'h', port: 6379, cluster: true, tls: true }),
    ).toBe('rediss-cluster://h:6379')
  })

  it('lets an explicit url win over the cluster flag', () => {
    expect(
      buildRedisUrl({ url: 'redis-cluster://seed:6379', cluster: false }),
    ).toBe('redis-cluster://seed:6379')
  })

  it('applies tls to an explicit cluster url without mangling the scheme', () => {
    expect(
      buildRedisUrl({ url: 'redis-cluster://seed:6379', tls: true }),
    ).toBe('rediss-cluster://seed:6379')
  })
})
