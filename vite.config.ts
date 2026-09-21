import { defineConfig } from 'vitest/config';

export default defineConfig({
  server: {
    host: '127.0.0.1',
    port: 5219,
  },
  test: {
    environment: 'node',
    include: ['tests/**/*.test.ts'],
  },
});
