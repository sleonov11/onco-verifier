import { Router } from 'express';
import { surveySchema } from '../contracts/survey';
import { createSurveyResponse } from '../services/survey';

export const surveyRouter = Router();

surveyRouter.post('/surveys', async (req, res, next) => {
  try {
    const parsed = surveySchema.safeParse(req.body);
    if (!parsed.success) {
      return res.status(400).json({
        error: 'VALIDATION_ERROR',
        details: parsed.error.flatten(),
      });
    }

    const saved = await createSurveyResponse(parsed.data);

    return res.status(201).json({
      ok: true,
      id: saved.id,
      created_at: saved.created_at,
    });
  } catch (e) {
    next(e);
  }
});
