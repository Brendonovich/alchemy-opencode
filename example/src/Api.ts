import * as Cloudflare from "alchemy/Cloudflare";
import * as Effect from "effect/Effect";
import { HttpServerRequest } from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import Agent from "./Agent.ts";

const Prompt = Schema.Struct({ text: Schema.String });
const encoder = new TextEncoder();

/**
 *   POST /agents/:id                  { text }  new Session → { sessionID }
 *   POST /agents/:id/sessions/:sid    { text }  follow up
 *   GET  /agents/:id/sessions/:sid              transcript, after the turn finishes
 *   GET  /agents/:id/events                     live events (server-sent)
 */
export default class Api extends Cloudflare.Worker<Api>()(
  "Api",
  { main: import.meta.url },
  Effect.gen(function* () {
    const agents = yield* Agent;

    return {
      fetch: Effect.gen(function* () {
        const request = yield* HttpServerRequest;
        const url = new URL(request.url, "http://api");
        const match = /^\/agents\/([\w-]+)(?:\/(events|sessions\/([\w-]+)))?$/.exec(url.pathname);
        if (!match) return HttpServerResponse.text("Not found", { status: 404 });
        const [, id, action, sessionID] = match;
        const agent = agents.getByName(id!);

        if (action === "events") {
          return HttpServerResponse.stream(
            agent
              .events()
              .pipe(Stream.map((event) => encoder.encode(`data: ${event}\n\n`))),
            { contentType: "text/event-stream" },
          );
        }
        if (sessionID && request.method === "GET") {
          return yield* HttpServerResponse.json({ text: yield* agent.result(sessionID) });
        }
        const { text } = yield* request.json.pipe(Effect.flatMap(Schema.decodeUnknownEffect(Prompt)));
        if (sessionID) {
          yield* agent.reply(sessionID, text);
          return HttpServerResponse.empty({ status: 202 });
        }
        return yield* HttpServerResponse.json({ sessionID: yield* agent.ask(text) });
      }).pipe(
        Effect.catchCause((cause) =>
          Effect.succeed(HttpServerResponse.text(String(cause), { status: 500 })),
        ),
      ),
    };
  }),
) {}
