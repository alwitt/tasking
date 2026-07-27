-- Create "audit_system_events" table
CREATE TABLE "public"."audit_system_events" (
  "id" text NOT NULL,
  "type" text NOT NULL,
  "metadata" jsonb NULL,
  "created_at" timestamptz NULL,
  "updated_at" timestamptz NULL,
  "broadcast_at" timestamptz NULL,
  CONSTRAINT "uni_audit_system_events_id" PRIMARY KEY ("id")
);
-- Create index "idx_audit_system_events_broadcast_at" to table: "audit_system_events"
CREATE INDEX "idx_audit_system_events_broadcast_at" ON "public"."audit_system_events" ("broadcast_at");
-- Create "tasks" table
CREATE TABLE "public"."tasks" (
  "id" text NOT NULL,
  "name" text NOT NULL,
  "creator" text NULL,
  "schedule_class" text NOT NULL,
  "state" text NOT NULL,
  "parameters" jsonb NULL,
  "metadata" jsonb NULL,
  "target_runtime" timestamptz NULL,
  "deadline" timestamptz NULL,
  "retry_params" bytea NOT NULL,
  "created_at" timestamptz NULL,
  "updated_at" timestamptz NULL,
  CONSTRAINT "uni_tasks_id" PRIMARY KEY ("id")
);
-- Create index "idx_tasks_creator" to table: "tasks"
CREATE INDEX "idx_tasks_creator" ON "public"."tasks" ("creator");
-- Create "task_executions" table
CREATE TABLE "public"."task_executions" (
  "id" text NOT NULL,
  "task_id" text NOT NULL,
  "execution_class" text NOT NULL,
  "state" text NOT NULL,
  "terminal_state" text NULL,
  "terminated_at" timestamptz NULL,
  "execute_at" timestamptz NULL,
  "worker_name" text NULL,
  "retry_parent_id" text NULL,
  "error_msg" text NULL,
  "failure_disposition" text NULL,
  "deadline" timestamptz NULL,
  "created_at" timestamptz NULL,
  "updated_at" timestamptz NULL,
  CONSTRAINT "uni_task_executions_id" PRIMARY KEY ("id"),
  CONSTRAINT "fk_task_executions_parent" FOREIGN KEY ("retry_parent_id") REFERENCES "public"."task_executions" ("id") ON UPDATE NO ACTION ON DELETE SET NULL,
  CONSTRAINT "fk_task_executions_task" FOREIGN KEY ("task_id") REFERENCES "public"."tasks" ("id") ON UPDATE NO ACTION ON DELETE CASCADE
);
-- Create "workflows" table
CREATE TABLE "public"."workflows" (
  "id" text NOT NULL,
  "name" text NOT NULL,
  "creator" text NULL,
  "state" text NOT NULL,
  "metadata" jsonb NULL,
  "deadline" timestamptz NOT NULL,
  "started_at" timestamptz NULL,
  "stopped_at" timestamptz NULL,
  "created_at" timestamptz NULL,
  "updated_at" timestamptz NULL,
  CONSTRAINT "uni_workflows_id" PRIMARY KEY ("id")
);
-- Create index "idx_workflows_creator" to table: "workflows"
CREATE INDEX "idx_workflows_creator" ON "public"."workflows" ("creator");
-- Create "workflow_steps" table
CREATE TABLE "public"."workflow_steps" (
  "id" text NOT NULL,
  "name" text NOT NULL,
  "workflow_id" text NOT NULL,
  "creator" text NULL,
  "type" text NOT NULL,
  "state" text NOT NULL,
  "user_restarted" boolean NOT NULL DEFAULT false,
  "parents" bytea NOT NULL,
  "parameters" jsonb NULL,
  "metadata" jsonb NULL,
  "retry_params" bytea NOT NULL,
  "deadline" timestamptz NOT NULL,
  "started_at" timestamptz NULL,
  "stopped_at" timestamptz NULL,
  "created_at" timestamptz NULL,
  "updated_at" timestamptz NULL,
  CONSTRAINT "uni_workflow_steps_id" PRIMARY KEY ("id"),
  CONSTRAINT "fk_workflow_steps_workflow" FOREIGN KEY ("workflow_id") REFERENCES "public"."workflows" ("id") ON UPDATE NO ACTION ON DELETE CASCADE
);
-- Create index "idx_workflow_steps_creator" to table: "workflow_steps"
CREATE INDEX "idx_workflow_steps_creator" ON "public"."workflow_steps" ("creator");
-- Create index "unique_step_in_workflow" to table: "workflow_steps"
CREATE UNIQUE INDEX "unique_step_in_workflow" ON "public"."workflow_steps" ("name", "workflow_id");
-- Create "workflow_step_dependencies" table
CREATE TABLE "public"."workflow_step_dependencies" (
  "step_id" text NOT NULL,
  "depends_on_id" text NOT NULL,
  "created_at" timestamptz NULL,
  "updated_at" timestamptz NULL,
  PRIMARY KEY ("step_id", "depends_on_id"),
  CONSTRAINT "fk_workflow_step_dependencies_depends_on" FOREIGN KEY ("depends_on_id") REFERENCES "public"."workflow_steps" ("id") ON UPDATE NO ACTION ON DELETE CASCADE,
  CONSTRAINT "fk_workflow_step_dependencies_step" FOREIGN KEY ("step_id") REFERENCES "public"."workflow_steps" ("id") ON UPDATE NO ACTION ON DELETE CASCADE
);
-- Create "workflow_step_runner_tasks" table
CREATE TABLE "public"."workflow_step_runner_tasks" (
  "step_id" text NOT NULL,
  "task_id" text NOT NULL,
  "created_at" timestamptz NULL,
  "updated_at" timestamptz NULL,
  PRIMARY KEY ("step_id", "task_id"),
  CONSTRAINT "fk_workflow_step_runner_tasks_step" FOREIGN KEY ("step_id") REFERENCES "public"."workflow_steps" ("id") ON UPDATE NO ACTION ON DELETE CASCADE,
  CONSTRAINT "fk_workflow_step_runner_tasks_task" FOREIGN KEY ("task_id") REFERENCES "public"."tasks" ("id") ON UPDATE NO ACTION ON DELETE CASCADE
);
