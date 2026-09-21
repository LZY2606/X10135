/// <reference types="vitest/config" />
import { defineConfig } from 'vite';
import { benchApiPlugin, createBenchService } from './server/api';

const benchService = createBenchService();

export default defineConfig({
  root: 'src/web',
  publicDir: false,
  plugins: [benchApiPlugin(benchService)],
  build: {
    outDir: '../../dist',
    emptyOutDir: true,
  },
  server: {
    host: '127.0.0.1',
    port: 5219,
    strictPort: true,
  },
  test: {
    environment: 'node',
    root: __dirname,
    include: ['tests/**/*.test.ts'],
  },
});
