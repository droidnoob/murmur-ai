/**
 * Minimal SSE parser over a ``ReadableStream<Uint8Array>``.
 *
 * Why not ``EventSource``? Because EventSource has no API for custom
 * request headers, so it can't carry an ``Authorization: Bearer …``.
 * We use ``fetch`` and parse the wire format ourselves.
 *
 * The wire format we honour is the subset Murmur emits: one event per
 * blank-line-separated record, each line either ``data: <json>`` or
 * ignored (comments / event-type lines). Multi-line ``data:`` is
 * concatenated with newlines per the SSE spec.
 */

export interface SseRecord {
  /** The accumulated ``data:`` payload (no JSON parsing yet). */
  data: string;
  /** Optional ``event:`` field — Murmur doesn't currently emit one. */
  event?: string;
  /** Optional ``id:`` field for client-side reconnect tracking. */
  id?: string;
}

const decoder = new TextDecoder("utf-8");

/**
 * Yield one {@link SseRecord} per ``\n\n``-terminated block on the
 * underlying stream. Terminates when the stream closes.
 */
export async function* parseSse(
  stream: ReadableStream<Uint8Array>,
  signal?: AbortSignal,
): AsyncIterableIterator<SseRecord> {
  const reader = stream.getReader();
  let buffer = "";

  const onAbort = () => {
    reader.cancel().catch(() => {
      /* swallow — we're tearing down */
    });
  };
  if (signal !== undefined) {
    if (signal.aborted) {
      onAbort();
      return;
    }
    signal.addEventListener("abort", onAbort, { once: true });
  }

  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });

      let sep: number;
      // SSE spec uses ``\n\n`` as record separator. Some servers emit
      // ``\r\n\r\n`` — handle both.
      while ((sep = findSeparator(buffer)) !== -1) {
        const block = buffer.slice(0, sep);
        // Skip past whichever separator we matched.
        buffer = buffer.slice(sep + (buffer[sep] === "\r" ? 4 : 2));
        const record = parseBlock(block);
        if (record !== null) yield record;
      }
    }
    if (buffer.length > 0) {
      const record = parseBlock(buffer);
      if (record !== null) yield record;
    }
  } finally {
    reader.releaseLock();
    if (signal !== undefined) {
      signal.removeEventListener("abort", onAbort);
    }
  }
}

function findSeparator(s: string): number {
  const idxLf = s.indexOf("\n\n");
  const idxCrlf = s.indexOf("\r\n\r\n");
  if (idxLf === -1) return idxCrlf;
  if (idxCrlf === -1) return idxLf;
  return Math.min(idxLf, idxCrlf);
}

function parseBlock(block: string): SseRecord | null {
  const dataLines: string[] = [];
  let event: string | undefined;
  let id: string | undefined;
  for (const rawLine of block.split(/\r?\n/)) {
    if (rawLine.length === 0) continue;
    if (rawLine.startsWith(":")) continue; // comment
    const colon = rawLine.indexOf(":");
    const field = colon === -1 ? rawLine : rawLine.slice(0, colon);
    const value =
      colon === -1
        ? ""
        : rawLine[colon + 1] === " "
          ? rawLine.slice(colon + 2)
          : rawLine.slice(colon + 1);
    switch (field) {
      case "data":
        dataLines.push(value);
        break;
      case "event":
        event = value;
        break;
      case "id":
        id = value;
        break;
      default:
        // ignore unknown fields per spec
        break;
    }
  }
  if (dataLines.length === 0) return null;
  const record: SseRecord = { data: dataLines.join("\n") };
  if (event !== undefined) record.event = event;
  if (id !== undefined) record.id = id;
  return record;
}
