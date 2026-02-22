import { z } from 'zod';

export const surveySchema = z.object({
  role: z.enum(['doctor', 'patient']),
  name: z.string().trim().min(2, 'Минимум 2 символа.').max(80),
  email: z.string().trim().email('Некорректный email.').max(120),

  // doctor
  specialty: z.string().trim().max(120).optional(),
  workplace: z.string().trim().max(120).optional(),
  experience_years: z.preprocess(
    (v) => (v === '' || v === null || v === undefined ? undefined : v),
    z.coerce.number().int().min(0).max(60).optional(),
  ),
  uses_guidelines: z.enum(['often', 'sometimes', 'rarely']).optional(),
  main_pain: z.string().trim().max(2000).optional(),

  // patient
  symptoms: z.string().trim().max(2000).optional(),
  has_diagnosis: z.enum(['yes', 'no', 'unknown']).optional(),
  treatment_stage: z.enum(['planning', 'in_progress', 'after', 'unknown']).optional(),
  wants_explanation: z.enum(['simple', 'detailed']).optional(),
});

export type SurveyDTO = z.infer<typeof surveySchema>;
