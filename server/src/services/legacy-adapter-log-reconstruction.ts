import { readLegacyAdapterEventBatch } from "./legacy-adapter-events.js";

/** Rebuild child output from the complete bound spool, including frames whose
 * database commit preceded controller death but whose live callback did not.
 * This is a read projection: no cursor update, event insertion or executor.
 */
export async function reconstructLegacyAdapterLogs(input: {
  directory: string;
  binding: Parameters<typeof readLegacyAdapterEventBatch>[0]["binding"];
  committedCursor: number;
  assertOwned: () => Promise<void>;
  append: (stream: "stdout" | "stderr", chunk: string) => Promise<void>;
}) {
  let cursor = 0;
  while (cursor < input.committedCursor) {
    await input.assertOwned();
    const batch = await readLegacyAdapterEventBatch({ directory: input.directory, binding: input.binding, cursor });
    if (batch.nextCursor <= cursor || batch.nextCursor > input.committedCursor) {
      throw new Error("Legacy log reconstruction exceeds committed frame boundary");
    }
    for (const event of batch.events) if (event.kind === "log") await input.append(event.stream, event.chunk);
    cursor = batch.nextCursor;
  }
  if (cursor !== input.committedCursor) throw new Error("Legacy log reconstruction cursor mismatch");
}
