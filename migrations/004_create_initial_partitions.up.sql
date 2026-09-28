-- One partition per month for the next 100 years. An empty partition costs
-- only catalog space, and pre-creating them means nothing has to create
-- partitions at runtime. The archival cron drops them once they are past.
DO $$
DECLARE
    month_start timestamptz;
BEGIN
    FOR i IN 0..1199 LOOP
        month_start := date_trunc('month', now()) + make_interval(months => i);
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF scheduled_emails FOR VALUES FROM (%L) TO (%L)',
            'scheduled_emails_' || to_char(month_start, '"y"YYYY"m"MM'),
            month_start,
            month_start + interval '1 month'
        );
    END LOOP;
END
$$;
