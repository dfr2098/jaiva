-- Seed del recorrido oficial Estable (Postgres → CSV).
CREATE TABLE IF NOT EXISTS public.stable_items (
    code TEXT PRIMARY KEY,
    name TEXT NOT NULL
);

INSERT INTO public.stable_items (code, name) VALUES
    ('A1', 'Alpha'),
    ('B2', 'Beta'),
    ('C3', 'Gamma')
ON CONFLICT (code) DO NOTHING;
