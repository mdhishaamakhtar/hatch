-- Pre-create 1200 monthly partitions on scheduled_emails (100-year runway).
-- Each partition is metadata-only until rows arrive; Postgres prunes to a
-- single partition for the deliver_at range filter used by the hourly poll.
DO $$
DECLARE
    start_month timestamptz := date_trunc('month', now());
    p_start     timestamptz;
    p_end       timestamptz;
    p_name      text;
    i           int;
BEGIN
    FOR i IN 0..1199 LOOP
        p_start := start_month + (i || ' months')::interval;
        p_end   := p_start + interval '1 month';
        p_name  := format('scheduled_emails_y%sm%s',
                          to_char(p_start, 'YYYY'),
                          to_char(p_start, 'MM'));
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF scheduled_emails FOR VALUES FROM (%L) TO (%L)',
            p_name, p_start, p_end
        );
    END LOOP;
END
$$;
