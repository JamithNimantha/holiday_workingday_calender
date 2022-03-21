CREATE TABLE IF NOT EXISTS public.holiday_calendar
(
    holiday_date date NOT NULL,
    description character varying(100) COLLATE pg_catalog."default",
    partial_day boolean,
    CONSTRAINT holiday_calendar_pkey PRIMARY KEY (holiday_date)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.holiday_calendar
    OWNER to postgres;