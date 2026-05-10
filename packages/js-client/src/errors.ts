/**
 * Error class hierarchy mirrored from ``murmur.core.errors`` so callers
 * can ``catch (e: MurmurError)`` regardless of which leaf class fired.
 */

export class MurmurError extends Error {
  /** Python error class name (e.g. ``RegistryError``). */
  readonly type: string;
  /** HTTP status code, when raised from a wire response. */
  readonly status: number | undefined;
  /** Server-emitted request id for correlation in logs. */
  readonly requestId: string | undefined;

  constructor(
    message: string,
    opts: { type?: string; status?: number; requestId?: string } = {},
  ) {
    super(message);
    this.name = "MurmurError";
    this.type = opts.type ?? "MurmurError";
    this.status = opts.status;
    this.requestId = opts.requestId;
  }
}

export class UnauthorizedError extends MurmurError {
  constructor(message: string, opts: { requestId?: string } = {}) {
    super(message, { type: "Unauthorized", status: 401, ...opts });
    this.name = "UnauthorizedError";
  }
}

export class RegistryError extends MurmurError {
  constructor(message: string, opts: { status?: number; requestId?: string } = {}) {
    super(message, { type: "RegistryError", status: 404, ...opts });
    this.name = "RegistryError";
  }
}

interface WireError {
  error: string;
  message: string;
  request_id?: string;
}

function isWireError(body: unknown): body is WireError {
  if (typeof body !== "object" || body === null) return false;
  const b = body as Record<string, unknown>;
  return typeof b.error === "string" && typeof b.message === "string";
}

/**
 * Map a non-2xx Response into the right MurmurError subclass.
 * Caller is responsible for narrowing on ``response.ok`` first.
 */
export async function raiseFromResponse(response: Response): Promise<never> {
  let body: unknown = null;
  const ct = response.headers.get("content-type") ?? "";
  if (ct.includes("application/json")) {
    try {
      body = await response.json();
    } catch {
      body = null;
    }
  } else {
    body = await response.text();
  }

  if (response.status === 401) {
    const msg = isWireError(body) ? body.message : "unauthorized";
    const opts: { requestId?: string } = {};
    if (isWireError(body) && body.request_id !== undefined) {
      opts.requestId = body.request_id;
    }
    throw new UnauthorizedError(msg, opts);
  }

  if (isWireError(body)) {
    const ctor = body.error === "RegistryError" ? RegistryError : MurmurError;
    const opts: { status?: number; requestId?: string } = {
      status: response.status,
    };
    if (body.request_id !== undefined) opts.requestId = body.request_id;
    throw new ctor(body.message, opts);
  }

  throw new MurmurError(
    `server returned ${response.status}: ${typeof body === "string" ? body : JSON.stringify(body)}`,
    { status: response.status },
  );
}
