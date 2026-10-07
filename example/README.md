# Cloudflare OpenCode

One OpenCode host per Durable Object, using `@opencode/sdk/workerd`. Each object's SQLite storage is its OpenCode database, so Sessions, messages and events survive eviction.

```
HTTP ──▶ Api (Worker) ──▶ Agent (Durable Object, one per id)
                           yield* OpenCode.DurableObject(...)
```

- `src/Agent.ts` — the Durable Object. `OpenCode.DurableObject` (from `alchemy-opencode/cloudflare`) hosts OpenCode on the object's storage, keeps running turns alive across eviction, and closes the host when idle. The object's methods (`ask`, `reply`, `result`, `events`) are built on top of it.
- `src/Api.ts` — the HTTP API.

The workerd profile has no filesystem or shell, so tools that touch files or run commands are unavailable.

## Run it

```sh
export ANTHROPIC_API_KEY=sk-ant-...
pnpm dev
```

```sh
curl -X POST $URL/agents/demo -d '{"text":"Write a haiku about Durable Objects"}'
# {"sessionID":"ses_..."}
curl $URL/agents/demo/sessions/ses_...
# {"text":"..."}
curl -N $URL/agents/demo/events
```
