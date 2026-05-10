import { describe, it, expect } from "vitest";
import {
  MurmurClient,
  UnauthorizedError,
  type AgentResult,
  type RunEvent,
} from "../src/index.js";

/**
 * Build a fake ``fetch`` whose behaviour can be programmed per request.
 * Returns the spy + the recorded request log.
 */
function makeFakeFetch(
  handler: (
    url: string,
    init: RequestInit,
  ) => Response | Promise<Response>,
): { fetch: typeof fetch; calls: { url: string; init: RequestInit }[] } {
  const calls: { url: string; init: RequestInit }[] = [];
  const fakeFetch: typeof fetch = async (input, init) => {
    const url = typeof input === "string" ? input : (input as Request).url;
    const i = init ?? {};
    calls.push({ url, init: i });
    return handler(url, i);
  };
  return { fetch: fakeFetch, calls };
}

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

describe("MurmurClient", () => {
  it("attaches Authorization header when authToken is set", async () => {
    const { fetch: f, calls } = makeFakeFetch(() =>
      jsonResponse(["echo"]),
    );
    const client = new MurmurClient("http://test", {
      fetch: f,
      authToken: "s3cret",
    });
    const agents = await client.listAgents();
    expect(agents).toEqual(["echo"]);
    expect(calls[0]!.url).toBe("http://test/agents");
    const headers = calls[0]!.init.headers as Record<string, string>;
    expect(headers["authorization"]).toBe("Bearer s3cret");
  });

  it("does not attach Authorization when authToken is unset", async () => {
    const { fetch: f, calls } = makeFakeFetch(() =>
      jsonResponse(["echo"]),
    );
    const client = new MurmurClient("http://test", { fetch: f });
    await client.listAgents();
    const headers = calls[0]!.init.headers as Record<string, string>;
    expect(headers["authorization"]).toBeUndefined();
  });

  it("maps 401 → UnauthorizedError", async () => {
    const { fetch: f } = makeFakeFetch(() =>
      jsonResponse(
        { error: "Unauthorized", message: "missing or invalid bearer token", request_id: "req-1" },
        401,
      ),
    );
    const client = new MurmurClient("http://test", { fetch: f });
    await expect(client.listAgents()).rejects.toBeInstanceOf(UnauthorizedError);
  });

  it("run() POSTs the task and returns the AgentResult", async () => {
    const wireResult: AgentResult<{ text: string }> = {
      output: { text: "Tokyo" },
      error: null,
      metadata: {
        duration_ms: 100,
        tokens_used: 50,
        cost_usd: 0,
        backend: "thread",
        trace_id: null,
      },
      agent_name: "geo",
      task_id: "t1",
    };
    const { fetch: f, calls } = makeFakeFetch((url, init) => {
      expect(url).toBe("http://test/agents/geo/run");
      expect(init.method).toBe("POST");
      const body = JSON.parse(init.body as string);
      expect(body.task.input).toBe("Capital of Japan?");
      return jsonResponse(wireResult);
    });
    const client = new MurmurClient("http://test", { fetch: f });
    const r = await client.run<{ text: string }>("geo", {
      input: "Capital of Japan?",
    });
    expect(r.output?.text).toBe("Tokyo");
    expect(r.error).toBeNull();
    expect(calls.length).toBe(1);
  });

  it("submit() returns a Run handle wired to the same client", async () => {
    let phase = "pending";
    const { fetch: f } = makeFakeFetch((url) => {
      if (url.endsWith("/submit")) return jsonResponse({ run_id: "r-1" });
      if (url.endsWith("/runs/r-1/status")) {
        const body = {
          run_id: "r-1",
          phase,
          target: "geo",
          is_group: false,
          created_at: "2026-01-01T00:00:00Z",
          finished_at: null,
        };
        phase = "succeeded";
        return jsonResponse(body);
      }
      throw new Error(`unexpected url ${url}`);
    });
    const client = new MurmurClient("http://test", { fetch: f });
    const run = await client.submit("geo", { input: "x" });
    expect(run.id).toBe("r-1");
    expect((await run.status()).phase).toBe("pending");
    expect((await run.status()).phase).toBe("succeeded");
  });

  it("streamEvents() decodes SSE data lines into RunEvent objects", async () => {
    const event: RunEvent = {
      event_type: "agent_completed",
      agent_name: "geo",
      task_id: "t1",
      trace_id: "tr1",
      parent_trace_id: null,
      timestamp: "2026-01-01T00:00:00Z",
      payload: { tokens_used: 100 },
    };
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        const chunk = new TextEncoder().encode(
          `data: ${JSON.stringify(event)}\n\n`,
        );
        controller.enqueue(chunk);
        controller.close();
      },
    });
    const { fetch: f } = makeFakeFetch(
      () =>
        new Response(stream, {
          status: 200,
          headers: { "content-type": "text/event-stream" },
        }),
    );
    const client = new MurmurClient("http://test", { fetch: f });
    const collected: RunEvent[] = [];
    for await (const ev of client.streamEvents()) {
      collected.push(ev);
    }
    expect(collected).toEqual([event]);
  });
});
