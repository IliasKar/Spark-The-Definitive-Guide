-- Creates the report registry table in Unity Catalog.
-- Run this once per environment (replace 'envprefix' with actual prefix: qa, p, etc.)
CREATE SCHEMA IF NOT EXISTS `envprefix-cdh-delivery`.report_catalog;
CREATE TABLE IF NOT EXISTS `envprefix-cdh-delivery`.report_catalog.report_registry (
    report_name             STRING        NOT NULL COMMENT 'Unique report identifier',
    job_name                STRING        NOT NULL COMMENT 'Databricks job name to trigger',
    frequency               STRING        NOT NULL COMMENT 'Execution frequency: daily, monthly, weekly',
    run_days                STRING                 COMMENT 'Daily: comma-separated weekdays (e.g. "Monday,Tuesday"); empty = every day. Monthly: "Eom" for end-of-month reports.',
    run_after_area          STRING                 COMMENT 'For monthly reports with run_days = "Eom": pipeline_area in pipeline.area_runs whose max business_date must equal CurrentMonthLastWorkingDate for the report to become eligible.',
    trigger_business_day    INT                    COMMENT 'For monthly: Nth business day to trigger. NULL for daily.',
    trigger_calendar_day    INT                    COMMENT 'For monthly: calendar day of month to trigger (1-31). Takes precedence over trigger_business_day. NULL = use trigger_business_day.',
    is_active               BOOLEAN       NOT NULL COMMENT 'Whether this report is currently enabled',
    execution_order         INT           NOT NULL DEFAULT 0 COMMENT 'Order within same frequency group (lower = first)',
    depends_on_report       STRING                 COMMENT 'Report name that must complete before this one starts',
    depends_on_workflow     STRING                 COMMENT 'External workflow job name that must complete today before this report runs',
    skip_email              BOOLEAN       NOT NULL DEFAULT FALSE COMMENT 'If true, skip email notifications for this report',
    IT_notify               BOOLEAN       NOT NULL DEFAULT FALSE COMMENT 'If true, this report feeds a downstream IT flow; exposed in v_reports_today for the other team to poll',
    triggered_at            TIMESTAMP              COMMENT 'Set at orchestrator start (step 1) with the run start time. Paired with updated_at: todays_status = last_run_status when updated_at >= triggered_at, else pending.',
    last_successful_run     TIMESTAMP              COMMENT 'Timestamp of last successful execution',
    last_run_status         STRING                 COMMENT 'Status of last run: SUCCESS, FAILED, SKIPPED',
    last_run_duration_sec   INT                    COMMENT 'Duration of last run in seconds',
    created_at              TIMESTAMP     NOT NULL DEFAULT from_utc_timestamp(current_timestamp(), 'Europe/Athens'),
    updated_at              TIMESTAMP     NOT NULL DEFAULT from_utc_timestamp(current_timestamp(), 'Europe/Athens'),
    notes                   STRING                 COMMENT 'Free-text notes about this report'
)
USING DELTA
COMMENT 'Registry of all reports orchestrated by the Report_Orchestrator job'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported'
);

-- ═══════════════════════════════════════════════════════════════
-- View: v_reports_today
-- ═══════════════════════════════════════════════════════════════
-- Exposes, for every IT_notify report, its status FOR THE CURRENT CYCLE so a
-- downstream team can poll "what should I expect today".
--   triggered_at : stamped on all active reports at orchestrator start (step 1)
--   updated_at   : stamped when the report reaches a terminal status this cycle
-- todays_status:
--   updated_at >= triggered_at  -> the report finished this cycle -> real status
--                                  (SUCCESS / FAILED / SKIPPED)
--   otherwise (or NULLs)        -> 'pending' (started, not finished yet)
-- SKIPPED (for any reason: not scheduled today, dependency/workflow skip) means the
-- report did NOT run -> it still holds yesterday's data; the consumer must not wait.
CREATE OR REPLACE VIEW `envprefix-cdh-delivery`.report_catalog.v_reports_today AS
SELECT
    report_name,
    job_name,
    frequency,
    CASE
        WHEN updated_at IS NOT NULL
         AND triggered_at IS NOT NULL
         AND updated_at >= triggered_at
            THEN last_run_status
        ELSE 'pending'
    END AS todays_status,
    triggered_at,
    updated_at,
    last_run_status,
    last_successful_run,
    last_run_duration_sec
FROM `envprefix-cdh-delivery`.report_catalog.report_registry
WHERE is_active = TRUE
  AND IT_notify = TRUE;

-- ═══════════════════════════════════════════════════════════════
-- Seed data
-- ═══════════════════════════════════════════════════════════════
INSERT INTO `envprefix-cdh-delivery`.report_catalog.report_registry
(report_name, job_name, frequency, trigger_business_day, is_active, execution_order, depends_on_report, depends_on_workflow, skip_email, IT_notify)
VALUES
-- Daily reports
('DetailedPortfolio', 'DetailedPortfolio', 'daily', NULL, TRUE, 10, NULL, 'submit_s2l2l_zone_databricks_job', FALSE, FALSE),
('DialerManualCampaign', 'DialerManualCampaign', 'daily', NULL, TRUE, 10, NULL, 'submit_s2l2l_zone_databricks_job', FALSE, FALSE),
-- Monthly reports
('Backstop', 'Backstop', 'monthly', 8, TRUE, 10, NULL, NULL, FALSE, FALSE),
('MegaDataTape_Master', 'MegaDataTape_Master', 'monthly', 5, TRUE, 10, NULL, NULL, FALSE, FALSE),
('HoistDebtJson', 'HoistDebtJson', 'monthly', 3, TRUE, 10, NULL, NULL, FALSE, FALSE);
