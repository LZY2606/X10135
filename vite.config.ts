import { defineConfig, type Plugin } from "vite";
import { labApiPlugin } from "./src/server/api.js";

export default defineConfig({
  server: {
    host: "127.0.0.1",
    port: 5219,
    strictPort: true,
  },
  plugins: [labApiPlugin() as Plugin],
  test: {
    globals: true,
    include: ["test/**/*.test.ts"],
    testTimeout: 20000,
  },
});
