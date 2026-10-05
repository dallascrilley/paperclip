import { TextDecoder } from "node:util";
import { readDurableChildOutput, readDurableChildResult } from "@paperclipai/adapter-utils/durable-child-process";
import { readLegacyAdapterWorkerResult, type LegacyAdapterWorkerReceipt } from "./legacy-adapter-supervisor.js";

export type LegacyAdapterWorkerEvent =
  | { kind: "log"; stream: "stdout" | "stderr"; chunk: string }
  | { kind: "dispatch" }
  | { kind: "spawn"; meta: { pid: number; processGroupId: number | null; startedAt: string } }
  | { kind: "meta"; meta: Record<string, unknown> }
  | { kind: "event"; event: Record<string, unknown> }
  | { kind: "progress"; progress: Record<string, unknown> };

const MAX_BATCH_BYTES = 1024 * 1024;
const record = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

function parseEvent(bytes: Buffer): LegacyAdapterWorkerEvent {
  const value = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes));
  if (!record(value)) throw new Error("Invalid adapter worker event");
  switch (value.kind) {
    case "dispatch": break;
    case "log":
      if (!["stdout", "stderr"].includes(String(value.stream)) || typeof value.chunk !== "string") {
        throw new Error("Invalid adapter worker log");
      }
      break;
    case "spawn": {
      const meta = value.meta;
      if (!record(meta) || !Number.isSafeInteger(meta.pid) || (meta.pid as number) <= 0 ||
          (meta.processGroupId !== null && meta.processGroupId !== meta.pid) ||
          typeof meta.startedAt !== "string" || !Number.isFinite(Date.parse(meta.startedAt))) {
        throw new Error("Invalid adapter worker spawn identity");
      }
      break;
    }
    case "meta": if (!record(value.meta)) throw new Error("Invalid adapter worker metadata"); break;
    case "event": if (!record(value.event)) throw new Error("Invalid adapter worker runtime event"); break;
    case "progress": if (!record(value.progress)) throw new Error("Invalid adapter worker progress"); break;
    default: throw new Error("Unknown adapter worker event");
  }
  return value as LegacyAdapterWorkerEvent;
}

/** Read-only framing. A cursor always denotes an acknowledged newline boundary.
 * Partial UTF-8 or JSON bytes stay on disk for the next controller, not in an
 * unrecoverable decoder. Terminal evidence requires supervisor close as well
 * as the bound adapter result, and rejects any retention loss.
 */
export async function readLegacyAdapterEventBatch(input: {
  directory: string;
  binding: { runId: string; companyId: string; adapterType: string };
  cursor: number;
}): Promise<{
  events: LegacyAdapterWorkerEvent[];
  nextCursor: number;
  terminal: LegacyAdapterWorkerReceipt | null;
}> {
  if (!Number.isSafeInteger(input.cursor) || input.cursor < 0) throw new Error("Invalid adapter event cursor");
  if (input.cursor > 0) {
    const previous = await readDurableChildOutput(input.directory, "stdout", input.cursor - 1, 1);
    if (previous.bytes[0] !== 10) throw new Error("Adapter event cursor is not a frame boundary");
  }
  // Read the terminal receipt first: all output precedes its atomic publication.
  const processResult = await readDurableChildResult(input.directory);
  if (processResult && (processResult.stdoutTruncated || processResult.stderrTruncated)) {
    throw new Error("Adapter worker output retention lost events");
  }
  const output = await readDurableChildOutput(input.directory, "stdout", input.cursor, MAX_BATCH_BYTES);
  const lastNewline = output.bytes.lastIndexOf(10);
  if (lastNewline < 0 && output.bytes.length === MAX_BATCH_BYTES) {
    throw new Error("Adapter worker event exceeds frame limit");
  }
  const events: LegacyAdapterWorkerEvent[] = [];
  let start = 0;
  while (start <= lastNewline) {
    const end = output.bytes.indexOf(10, start);
    events.push(parseEvent(output.bytes.subarray(start, end)));
    start = end + 1;
  }
  const nextCursor = input.cursor + start;
  let terminal: LegacyAdapterWorkerReceipt | null = null;
  if (processResult && output.bytes.length < MAX_BATCH_BYTES) {
    if (start !== output.bytes.length) throw new Error("Adapter worker exited with an incomplete event");
    if (processResult.exitCode !== 0 || processResult.signal || processResult.errorMessage) {
      throw new Error("Adapter worker supervisor reported failure");
    }
    terminal = await readLegacyAdapterWorkerResult(input.directory, input.binding);
    if (!terminal) throw new Error("Adapter worker exited without a bound terminal result");
  }
  return { events, nextCursor, terminal };
}

/** The host implements this as one transaction: verify its current controller
 * lease AND the expected cursor, append events/update runtime, and advance the
 * cursor together. No callback is fired before that fenced commit. Final run
 * state is a separate host operation using the same ownership fence.
 */
export async function consumeLegacyAdapterEventBatch(input: Parameters<typeof readLegacyAdapterEventBatch>[0] & {
  commit: (batch: { expectedCursor: number; nextCursor: number; events: LegacyAdapterWorkerEvent[] }) => Promise<boolean>;
}) {
  const batch = await readLegacyAdapterEventBatch(input);
  if (!await input.commit({ expectedCursor: input.cursor, nextCursor: batch.nextCursor, events: batch.events })) {
    throw new Error("Adapter worker event consumer lost ownership or cursor");
  }
  return batch;
}
