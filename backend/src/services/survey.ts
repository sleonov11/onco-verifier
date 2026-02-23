import { pool } from '../db/pool';
import type { SurveyDTO } from '../contracts/survey';

export async function createSurveyResponse(dto: SurveyDTO) {
  const q = `
    INSERT INTO survey_responses (
      role, name, email,
      specialty, workplace, experience_years, uses_guidelines, main_pain,
      symptoms, has_diagnosis, treatment_stage, wants_explanation,
      payload
    )
    VALUES (
      $1, $2, $3,
      $4, $5, $6, $7, $8,
      $9, $10, $11, $12,
      $13
    )
    RETURNING id, created_at
  `;

  const values = [
    dto.role,
    dto.name,
    dto.email,

    dto.specialty ?? null,
    dto.workplace ?? null,
    dto.experience_years ?? null,
    dto.uses_guidelines ?? null,
    dto.main_pain ?? null,

    dto.symptoms ?? null,
    dto.has_diagnosis ?? null,
    dto.treatment_stage ?? null,
    dto.wants_explanation ?? null,

    dto, // payload jsonb
  ];

  const res = await pool.query(q, values);
  return res.rows[0] as { id: string; created_at: string };
}
