import { pgTable, uuid, text, bigint, timestamp, uniqueIndex } from "drizzle-orm/pg-core";
import { companies } from "./companies.js";
import { agents } from "./agents.js";
import { heartbeatRuns } from "./heartbeat_runs.js";

/** Host custody and replay cursor. Never sourced from adapter result JSON. */
export const legacyAdapterTransports = pgTable("legacy_adapter_transports", {
  runId: uuid("run_id").primaryKey().references(() => heartbeatRuns.id, { onDelete: "cascade" }),
  companyId: uuid("company_id").notNull().references(() => companies.id),
  agentId: uuid("agent_id").notNull().references(() => agents.id),
  adapterType: text("adapter_type").notNull(),
  // Opaque host-assigned namespace; resolve under the configured instance root.
  // No agent-selected path or command is persisted here.
  custodyId: uuid("custody_id").notNull(),
  eventCursor: bigint("event_cursor", { mode: "number" }).notNull().default(0),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  updatedAt: timestamp("updated_at", { withTimezone: true }).notNull().defaultNow(),
}, table => ({ custodyUq: uniqueIndex("legacy_adapter_transports_custody_uq").on(table.custodyId) }));
