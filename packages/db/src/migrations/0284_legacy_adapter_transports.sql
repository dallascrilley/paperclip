CREATE TABLE "legacy_adapter_transports" (
	"run_id" uuid PRIMARY KEY NOT NULL,
	"company_id" uuid NOT NULL,
	"agent_id" uuid NOT NULL,
	"adapter_type" text NOT NULL,
	"custody_id" uuid NOT NULL,
	"event_cursor" bigint DEFAULT 0 NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
ALTER TABLE "legacy_adapter_transports" ADD CONSTRAINT "legacy_adapter_transports_run_id_heartbeat_runs_id_fk" FOREIGN KEY ("run_id") REFERENCES "public"."heartbeat_runs"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "legacy_adapter_transports" ADD CONSTRAINT "legacy_adapter_transports_company_id_companies_id_fk" FOREIGN KEY ("company_id") REFERENCES "public"."companies"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "legacy_adapter_transports" ADD CONSTRAINT "legacy_adapter_transports_agent_id_agents_id_fk" FOREIGN KEY ("agent_id") REFERENCES "public"."agents"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
CREATE UNIQUE INDEX "legacy_adapter_transports_custody_uq" ON "legacy_adapter_transports" USING btree ("custody_id");