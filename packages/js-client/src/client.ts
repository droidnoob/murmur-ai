/**
 * Murmur agent-server client. Mirrors ``packages/murmur-client`` (Python)
 * — same wire envelope, same auth model, same surface.
 *
 * @example
 * ```ts
 * const client = new MurmurClient("http://localhost:8421", {
 *   authToken: process.env.MURMUR_TOKEN,
 * });
 * const result = await client.run("researcher", { input: "..." });
 * if (result.output != null) console.log(result.output);
 * ```
 */

import { parseSse } from "./sse.js";
import { raiseFromResponse, MurmurError } from "./errors.js";
import type {
  TaskSpec,
  AgentResult,
  GroupResult,
  RunStatus,
  RunEvent,
} from "./types.js";

const REQUEST_ID_HEADER = "x-request-id";

export interface MurmurClientOptions {
  /** Static bearer token. When set, every request carries
   *  ``Authorization: Bearer <token>``. Required if the server was
   *  built with ``auth_token=...``. */
  authToken?: string;
  /** Per-request timeout in ms. Default 30s. */
  timeoutMs?: number;
  /** Custom ``fetch`` implementation — for tests or for runtimes where
   *  the global ``fetch`` is missing. Defaults to globalThis.fetch. */
  fetch?: typeof fetch;
}

export interface RunOptions {
  /** Override the auto-generated request id (UUID) for log correlation. */
  requestId?: string;
}

interface RawSubmitResponse {
  run_id: string;
}

/**
 * Async client. All methods return Promises. Construct once per server,
 * reuse across calls. There's no resource to dispose — each ``fetch`` is
 * its own connection unless the runtime keeps an HTTP/1.1 keep-alive pool.
 */
export class MurmurClient {
  private readonly baseUrl: string;
  private readonly authToken: string | undefined;
  private readonly timeoutMs: number;
  private readonly fetchImpl: typeof fetch;

  constructor(baseUrl: string, opts: MurmurClientOptions = {}) {
    this.baseUrl = baseUrl.replace(/\/+$/, "");
    this.authToken = opts.authToken;
    this.timeoutMs = opts.timeoutMs ?? 30_000;
    const f = opts.fetch ?? globalThis.fetch;
    if (typeof f !== "function") {
      throw new Error(
        "no fetch available — pass `fetch` in MurmurClientOptions",
      );
    }
    this.fetchImpl = f.bind(globalThis);
  }

  // ---------------------------------------------------------- discovery

  async health(): Promise<{ status: string }> {
    return this.json("GET", "/health");
  }

  async listAgents(): Promise<string[]> {
    return this.json("GET", "/agents");
  }

  async getAgentSchema(name: string): Promise<Record<string, unknown>> {
    return this.json("GET", `/agents/${encodeURIComponent(name)}/schema`);
  }

  async listGroups(): Promise<string[]> {
    return this.json("GET", "/groups");
  }

  async getGroupTopology(name: string): Promise<Record<string, unknown>> {
    return this.json("GET", `/groups/${encodeURIComponent(name)}/topology`);
  }

  async listTools(): Promise<Record<string, unknown>[]> {
    return this.json("GET", "/tools");
  }

  // ------------------------------------------------------------ telemetry

  async usage(opts: { groupBy?: string } = {}): Promise<Record<string, unknown>> {
    const qs =
      opts.groupBy !== undefined
        ? `?group_by=${encodeURIComponent(opts.groupBy)}`
        : "";
    return this.json("GET", `/usage${qs}`);
  }

  async runtimeStats(): Promise<Record<string, unknown>> {
    return this.json("GET", "/runtime/stats");
  }

  // ---------------------------------------------------------- sync dispatch

  /**
   * Run an agent synchronously, returning the typed result. Mirrors
   * ``runtime.run`` on the server side.
   *
   * The generic parameter ``T`` lets you narrow ``output`` when you know
   * the agent's ``output_type``. The wire is dynamic JSON so this is
   * advisory — runtime validation lives on the server.
   */
  async run<T = unknown>(
    agentName: string,
    task: TaskSpec,
    opts: RunOptions = {},
  ): Promise<AgentResult<T>> {
    const rid = opts.requestId ?? crypto.randomUUID();
    return this.json<AgentResult<T>>(
      "POST",
      `/agents/${encodeURIComponent(agentName)}/run`,
      { task, request_id: rid },
      { [REQUEST_ID_HEADER]: rid },
    );
  }

  async gather<T = unknown>(
    agentName: string,
    tasks: TaskSpec[],
    opts: RunOptions & { maxConcurrency?: number } = {},
  ): Promise<AgentResult<T>[]> {
    const rid = opts.requestId ?? crypto.randomUUID();
    return this.json<AgentResult<T>[]>(
      "POST",
      `/agents/${encodeURIComponent(agentName)}/gather`,
      {
        tasks,
        max_concurrency: opts.maxConcurrency,
        request_id: rid,
      },
      { [REQUEST_ID_HEADER]: rid },
    );
  }

  async runGroup(
    groupName: string,
    task: TaskSpec,
    opts: RunOptions = {},
  ): Promise<AgentResult | GroupResult> {
    const rid = opts.requestId ?? crypto.randomUUID();
    return this.json<AgentResult | GroupResult>(
      "POST",
      `/groups/${encodeURIComponent(groupName)}/run`,
      { task, request_id: rid },
      { [REQUEST_ID_HEADER]: rid },
    );
  }

  // --------------------------------------------------------- async dispatch

  /**
   * Submit a run and get a {@link Run} handle for polling status,
   * fetching the final result, cancelling, or streaming progress events.
   */
  async submit(
    target: string,
    task: TaskSpec,
    opts: RunOptions & { isGroup?: boolean } = {},
  ): Promise<Run> {
    const rid = opts.requestId ?? crypto.randomUUID();
    const body = await this.json<RawSubmitResponse>(
      "POST",
      "/submit",
      {
        target,
        is_group: opts.isGroup ?? false,
        task,
        request_id: rid,
      },
      { [REQUEST_ID_HEADER]: rid },
    );
    return new Run(this, body.run_id, target);
  }

  // ----------------------------------------------------------------- events

  /** Subscribe to the fleet-wide event stream. Server must have been
   *  built with an ``SSEEventEmitter``. The async iterator terminates
   *  when the stream closes or ``signal`` aborts. */
  async *streamEvents(signal?: AbortSignal): AsyncIterableIterator<RunEvent> {
    yield* this.streamSseAs<RunEvent>("/events/stream", signal);
  }

  // ------------------------------------------------------------- internals

  /** @internal — shared status/result/cancel/stream entry-points used by Run. */
  async _runStatus(runId: string): Promise<RunStatus> {
    return this.json("GET", `/runs/${encodeURIComponent(runId)}/status`);
  }

  async _runResult(runId: string): Promise<AgentResult> {
    return this.json("GET", `/runs/${encodeURIComponent(runId)}/result`);
  }

  async _runCancel(runId: string): Promise<void> {
    await this.bare("POST", `/runs/${encodeURIComponent(runId)}/cancel`);
  }

  async *_runStream(
    runId: string,
    signal?: AbortSignal,
  ): AsyncIterableIterator<RunEvent> {
    yield* this.streamSseAs<RunEvent>(
      `/runs/${encodeURIComponent(runId)}/stream`,
      signal,
    );
  }

  private async *streamSseAs<T>(
    path: string,
    signal?: AbortSignal,
  ): AsyncIterableIterator<T> {
    const init: RequestInit = {
      method: "GET",
      headers: this.authHeaders({ accept: "text/event-stream" }),
    };
    if (signal !== undefined) init.signal = signal;
    const response = await this.fetchImpl(this.url(path), init);
    if (!response.ok) await raiseFromResponse(response);
    if (response.body === null) {
      throw new MurmurError("server returned an empty SSE body");
    }
    for await (const record of parseSse(response.body, signal)) {
      try {
        yield JSON.parse(record.data) as T;
      } catch {
        // Malformed JSON — skip rather than break the loop.
      }
    }
  }

  private async json<T>(
    method: string,
    path: string,
    body?: unknown,
    extraHeaders: Record<string, string> = {},
  ): Promise<T> {
    const init: RequestInit = {
      method,
      headers: this.authHeaders({
        ...(body !== undefined ? { "content-type": "application/json" } : {}),
        accept: "application/json",
        ...extraHeaders,
      }),
    };
    const sig = this.timeoutSignal();
    if (sig !== undefined) init.signal = sig;
    if (body !== undefined) {
      init.body = JSON.stringify(body);
    }
    const response = await this.fetchImpl(this.url(path), init);
    if (!response.ok) await raiseFromResponse(response);
    return (await response.json()) as T;
  }

  private async bare(method: string, path: string): Promise<void> {
    const init: RequestInit = {
      method,
      headers: this.authHeaders({}),
    };
    const sig = this.timeoutSignal();
    if (sig !== undefined) init.signal = sig;
    const response = await this.fetchImpl(this.url(path), init);
    if (!response.ok) await raiseFromResponse(response);
  }

  private url(path: string): string {
    return `${this.baseUrl}${path.startsWith("/") ? path : `/${path}`}`;
  }

  private authHeaders(extra: Record<string, string>): Record<string, string> {
    const h: Record<string, string> = { ...extra };
    if (this.authToken !== undefined) {
      h["authorization"] = `Bearer ${this.authToken}`;
    }
    return h;
  }

  private timeoutSignal(): AbortSignal | undefined {
    if (typeof AbortSignal !== "undefined" && "timeout" in AbortSignal) {
      return AbortSignal.timeout(this.timeoutMs);
    }
    return undefined;
  }
}

/**
 * Handle for an asynchronously-dispatched run. Created by
 * {@link MurmurClient.submit}. Use ``await run.result()`` to block until
 * the run finishes, ``run.stream()`` to follow its events.
 */
export class Run {
  constructor(
    private readonly client: MurmurClient,
    public readonly id: string,
    public readonly target: string,
  ) {}

  status(): Promise<RunStatus> {
    return this.client._runStatus(this.id);
  }

  result(): Promise<AgentResult> {
    return this.client._runResult(this.id);
  }

  cancel(): Promise<void> {
    return this.client._runCancel(this.id);
  }

  stream(signal?: AbortSignal): AsyncIterableIterator<RunEvent> {
    return this.client._runStream(this.id, signal);
  }
}
