import { pool } from './pool';

export async function migrate() {
  await pool.query(`
    CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS survey_responses (
      id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
      created_at timestamptz NOT NULL DEFAULT now(),

      role text NOT NULL CHECK (role IN ('doctor', 'patient')),
      name text NOT NULL,
      email text NOT NULL,

      -- doctor fields
      specialty text NULL,
      workplace text NULL,
      experience_years int NULL CHECK (experience_years >= 0 AND experience_years <= 60),
      uses_guidelines text NULL CHECK (uses_guidelines IN ('often', 'sometimes', 'rarely')),
      main_pain text NULL,

      -- patient fields
      symptoms text NULL,
      has_diagnosis text NULL CHECK (has_diagnosis IN ('yes', 'no', 'unknown')),
      treatment_stage text NULL CHECK (treatment_stage IN ('planning', 'in_progress', 'after', 'unknown')),
      wants_explanation text NULL CHECK (wants_explanation IN ('simple', 'detailed')),

      payload jsonb NOT NULL
    );
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_survey_responses_created_at
    ON survey_responses (created_at DESC);
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_survey_responses_role
    ON survey_responses (role);
  `);
}
