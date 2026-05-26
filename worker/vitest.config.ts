import { defineWorkersConfig } from "@cloudflare/vitest-pool-workers/config";

export default defineWorkersConfig({
  test: {
    poolOptions: {
      workers: {
        wrangler: { configPath: "./wrangler.toml" },
        // Disable per-test isolated storage. With hibernated WebSockets,
        // abortAllDurableObjects() between tests crashes mid-handler and
        // emits noisy stack traces. Every test in the suite picks a unique
        // room id (room-*-${randomUUID}) so DO state never leaks anyway.
        isolatedStorage: false,
      },
    },
  },
});
