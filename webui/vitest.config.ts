import { defineConfig } from "vitest/config";

// Page tests render React components in happy-dom. Parser tests stay on the Node test runner.
export default defineConfig({
  test: {
    environment: "happy-dom",
    include: ["tests/**/*.test.tsx"],
  },
});
