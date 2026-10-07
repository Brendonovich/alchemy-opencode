# alchemy-opencode

Host [OpenCode](https://opencode.ai) (`@opencode/sdk/workerd`) inside an [Alchemy](https://alchemy.run) Cloudflare Durable Object.

```ts
import * as OpenCode from "alchemy-opencode/cloudflare";

export default class Agent extends Cloudflare.DurableObject<Agent>()(
  "Agent",
  Effect.gen(function* () {
    return Effect.gen(function* () {
      const opencode = yield* OpenCode.DurableObject({
        config: { model: "anthropic/claude-sonnet-4-5" },
      });
      return {
        ask: (text: string) =>
          Effect.gen(function* () {
            const session = yield* opencode.createSession();
            yield* opencode.prompt({ sessionID: session.id, text });
            return session.id;
          }),
      };
    });
  }),
) {}
```

`OpenCode.DurableObject` boots OpenCode on the object's SQLite storage, arms a durable per-Session alarm so turns resume after eviction, and closes the host when idle so the object can hibernate.

See [`example/`](./example) for a Worker + Durable Object exposing it over HTTP.

`@opencode/sdk@2.0.24` is built against `effect@4.0.0-rc.112`; see the overrides in `pnpm-workspace.yaml`.
