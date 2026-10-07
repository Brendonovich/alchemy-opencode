import * as Cloudflare from "alchemy/Cloudflare";
import * as OpenCode from "alchemy-opencode/cloudflare";
import * as Config from "effect/Config";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";

/**
 * One OpenCode host per Durable Object. The object's SQLite storage is the
 * OpenCode database, so every Session, message and event for this agent
 * lives (and survives eviction) here.
 */
export default class Agent extends Cloudflare.DurableObject<Agent>()(
  "Agent",
  Effect.gen(function* () {
    // Binds the secret into the Worker's env. OpenCode's Anthropic provider
    // reads ANTHROPIC_API_KEY from `process.env`, which nodejs_compat
    // populates from env bindings.
    yield* Config.Redacted("ANTHROPIC_API_KEY").pipe(Effect.orDie);

    return Effect.gen(function* () {
      const opencode = yield* OpenCode.DurableObject({
        config: { model: "anthropic/claude-sonnet-4-5" },
      });

      return {
        /** Start a Session and admit the prompt; the turn runs in the background. */
        ask: (text: string) =>
          Effect.gen(function* () {
            const session = yield* opencode.createSession();
            yield* opencode.prompt({ sessionID: session.id, text });
            return session.id;
          }),
        /** Follow up in an existing Session. */
        reply: (sessionID: string, text: string) =>
          opencode.prompt({ sessionID, text }).pipe(Effect.asVoid),
        /** The last assistant reply, once the Session's current turn finishes. */
        result: (sessionID: string) =>
          Effect.gen(function* () {
            yield* opencode.wait(sessionID);
            const messages = yield* opencode.use((client) =>
              client.sessions.context({ sessionID }),
            );
            const reply = messages.findLast((message) => message.type === "assistant");
            return (reply?.content ?? [])
              .flatMap((part) => (part.type === "text" ? [part.text] : []))
              .join("");
          }),
        /** Live events as JSON, ready to forward to a client. */
        events: () => opencode.events().pipe(Stream.map((event) => JSON.stringify(event))),
      };
    });
  }),
) {}
