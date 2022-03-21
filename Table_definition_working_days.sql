CREATE TABLE IF NOT EXISTS public.working_days
(
    work_date date NOT NULL,
    parital_day boolean,
    close_time time without time zone,
    CONSTRAINT working_days_pkey PRIMARY KEY (work_date)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.working_days
    OWNER to postgres;