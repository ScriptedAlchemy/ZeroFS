import { defineConfig } from "vitest/config";

// Page tests render React components in happy-dom. Parser tests stay on the Node test runner.
export default defineConfig({
  plugins: [
    {
      name: "wasm-client-test-module",
      resolveId(id) {
        if (id.endsWith("/generated/zerofs-client/zerofs_client")) return "virtual:zerofs-client";
      },
    },
  ],
  test: {
    environment: "happy-dom",
    include: ["tests/**/*.test.tsx"],
  },
});
