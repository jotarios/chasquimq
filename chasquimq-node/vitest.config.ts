import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    setupFiles: ['./__test__/setup.ts'],
  },
})
