import type { OpenCodeEvent } from "@opencode/sdk";
import type { OpenCodeWorkerd } from "@opencode/sdk/workerd";
import * as Data from "effect/Data";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import { makeCallback, type Callback, type RuntimeContext } from "alchemy";
import { DurableObjectState } from "alchemy/Cloudflare";

/** An OpenCode SDK call failed. `cause` is the SDK's `ClientError` (or a defect). */
export class OpenCodeError extends Data.TaggedError("OpenCodeError")<{
  readonly cause: unknown;
}> {}

/** The Promise SDK host `OpenCodeWorkerd.create` returns. */
export type Client = OpenCodeWorkerd.Interface;
export type Configuration = OpenCodeWorkerd.Configuration;
export type SessionInfo = Awaited<ReturnType<Client["sessions"]["create"]>>;
export type CreateSessionInput = NonNullable<Parameters<Client["sessions"]["create"]>[0]>;
export type PromptInput = Parameters<Client["sessions"]["prompt"]>[0];
export type PromptOutput = Awaited<ReturnType<Client["sessions"]["prompt"]>>;
export type { OpenCodeEvent };

export interface DurableObjectProps extends Omit<OpenCodeWorkerd.CreateOptions, "storage"> {
  /**
   * Directory new Sessions are placed in. The workerd profile has no
   * filesystem, so this is a logical Location, not a real path.
   * @default "/workspace"
   */
  readonly directory?: string;
  /**
   * Longest a wake alarm waits on a running Session before handing off to
   * the next alarm. Must leave headroom under Cloudflare's alarm wall-clock
   * limit.
   * @default "5 minutes"
   */
  readonly wakeBudget?: Duration.Input;
  /**
   * How long the host stays open after its last use with nothing running.
   * An open host runs background timers that pin the object in memory, so
   * it is closed when idle (allowing hibernation) and reopened on demand.
   * @default "30 seconds"
   */
  readonly idleClose?: Duration.Input;
}

/** OpenCode, hosted inside the current Durable Object. */
export interface OpenCodeDurableObject {
  /**
   * Run `f` against the open host, reopening it if it was closed for idling.
   * The host is not closed while `f` runs.
   */
  readonly use: <A>(f: (client: Client) => Promise<A>) => Effect.Effect<A, OpenCodeError>;
  /** Create a Session in {@link DurableObjectProps.directory} unless `location` is given. */
  readonly createSession: (input?: CreateSessionInput) => Effect.Effect<SessionInfo, OpenCodeError>;
  /**
   * Admit a prompt and arm the Session's wake. The turn runs in the
   * background and survives eviction; see `wait` and `events`.
   */
  readonly prompt: (
    input: PromptInput,
  ) => Effect.Effect<PromptOutput, OpenCodeError, RuntimeContext>;
  /** Resolve when the Session goes idle. */
  readonly wait: (sessionID: string) => Effect.Effect<void, OpenCodeError>;
  /** The host's live event stream, across all Sessions. */
  readonly events: () => Stream.Stream<OpenCodeEvent, OpenCodeError>;
}

/**
 * Host OpenCode (`@opencode/sdk/workerd`) inside the current Durable Object,
 * on the object's own SQLite storage. Yield it in the object's inner
 * (instance) Effect and build the object's methods on top of it.
 *
 * What a bare `OpenCodeWorkerd.create` leaves to you, this handles:
 *
 * - **The wake.** OpenCode resumes a turn cut off by eviction only when the
 *   object boots again, and nothing boots it. `prompt` arms a durable alarm
 *   per Session (`Alchemy.makeCallback`) that waits while the turn runs —
 *   keeping the object awake — and re-arms itself until the Session is idle.
 * - **Hibernation.** The host is closed once idle and reopened on next use.
 *
 * ### Hosting OpenCode in a Durable Object
 * **Example:** One agent per Durable Object
 * ```typescript
 * import * as OpenCode from "alchemy-opencode/cloudflare";
 *
 * export default class Agent extends Cloudflare.DurableObject<Agent>()(
 *   "Agent",
 *   Effect.gen(function* () {
 *     return Effect.gen(function* () {
 *       const opencode = yield* OpenCode.DurableObject({
 *         config: { model: "anthropic/claude-sonnet-4-5" },
 *       });
 *
 *       return {
 *         ask: (text: string) =>
 *           Effect.gen(function* () {
 *             const session = yield* opencode.createSession();
 *             yield* opencode.prompt({ sessionID: session.id, text });
 *             return session.id;
 *           }),
 *         messages: (sessionID: string) =>
 *           opencode.use((client) => client.sessions.context({ sessionID })),
 *       };
 *     });
 *   }),
 * ) {}
 * ```
 */
export const DurableObject = Effect.fn("OpenCode.DurableObject")(function* (
  props: DurableObjectProps = {},
) {
  const {
    directory = "/workspace",
    wakeBudget = "5 minutes",
    idleClose = "30 seconds",
    ...options
  } = props;
  const state = yield* DurableObjectState;
  const lock = yield* Semaphore.make(1);
  // Mutable instance state: the open host (if any) and calls holding it open.
  const host = { client: undefined as Client | undefined, inflight: 0 };

  // Opening the host is what resumes turns an eviction orphaned.
  const open = Semaphore.withPermits(lock, 1)(
    Effect.suspend(() =>
      host.client
        ? Effect.succeed(host.client)
        : Effect.tryPromise({
            // Imported lazily: the stack module is also loaded at plan time,
            // where the workerd build of the SDK cannot resolve.
            try: async () => {
              const { OpenCodeWorkerd } = await import("@opencode/sdk/workerd");
              return OpenCodeWorkerd.create({ ...options, storage: state.raw.storage });
            },
            catch: (cause) => new OpenCodeError({ cause }),
          }).pipe(Effect.tap((client) => Effect.sync(() => (host.client = client)))),
    ),
  );

  const use = <A>(f: (client: Client) => Promise<A>) =>
    Effect.acquireUseRelease(
      Effect.sync(() => host.inflight++).pipe(Effect.andThen(open)),
      (client) => Effect.tryPromise({ try: () => f(client), catch: (cause) => new OpenCodeError({ cause }) }),
      () => Effect.sync(() => host.inflight--),
    );

  const activeSessions = use((client) => client.sessions.active()).pipe(
    Effect.map((active) => new Set(Object.keys(active))),
  );

  const close = yield* makeCallback(
    "opencode.close",
    Effect.fn(function* () {
      const busy = host.inflight > 0 || (yield* activeSessions).size > 0;
      if (busy) return;
      yield* Semaphore.withPermits(lock, 1)(
        Effect.promise(async () => {
          const client = host.client;
          host.client = undefined;
          await client?.close();
        }),
      );
    }),
  );
  const scheduleClose = close
    .schedule("idle", { payload: null, after: idleClose })
    .pipe(Effect.orDie);

  // One wake job per Session. A replacement scheduled while the handler runs
  // survives its acknowledgement, so the first thing a run does is arm the
  // next one: if the object is evicted mid-wait, that alarm boots it again.
  const wake: Callback<string> = yield* makeCallback(
    "opencode.wake",
    Effect.fn(function* (sessionID: string) {
      yield* wake.schedule(sessionID, {
        payload: sessionID,
        after: Duration.sum(Duration.fromInputUnsafe(wakeBudget), Duration.seconds(30)),
      });
      if ((yield* activeSessions).has(sessionID)) {
        yield* use((client) => client.sessions.wait({ sessionID })).pipe(
          Effect.timeout(wakeBudget),
          Effect.ignore,
        );
      }
      if ((yield* activeSessions).has(sessionID)) {
        return yield* wake.schedule(sessionID, { payload: sessionID, after: 0 });
      }
      yield* wake.cancel(sessionID);
      yield* scheduleClose;
    }),
  );
  const armWake = (sessionID: string) =>
    wake.schedule(sessionID, { payload: sessionID, after: 0 }).pipe(Effect.orDie);

  // Boot now, under the instance's blockConcurrencyWhile, and re-arm wakes
  // for any turn the boot just resumed.
  // A host that cannot boot leaves the instance unusable, so it dies (and
  // Cloudflare resets the object) rather than surfacing on every caller.
  yield* Effect.forEach(yield* activeSessions.pipe(Effect.orDie), armWake, { discard: true });

  return {
    use,
    createSession: (input) =>
      use((client) =>
        client.sessions.create({ ...input, location: input?.location ?? { directory } }),
      ),
    prompt: (input) =>
      use((client) => client.sessions.prompt(input)).pipe(
        Effect.tap(() => armWake(input.sessionID)),
      ),
    wait: (sessionID) => use((client) => client.sessions.wait({ sessionID })),
    // Holds the host open for as long as the stream is consumed.
    events: () =>
      Stream.unwrap(
        Effect.sync(() => host.inflight++).pipe(
          Effect.andThen(open),
          Effect.map((client) =>
            Stream.fromAsyncIterable(client.events.subscribe(), (cause) => new OpenCodeError({ cause })),
          ),
        ),
      ).pipe(Stream.ensuring(Effect.sync(() => host.inflight--))),
  } satisfies OpenCodeDurableObject as OpenCodeDurableObject;
});
