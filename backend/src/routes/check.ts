import { Router, type Request, type Response, type NextFunction } from "express";
import { randomUUID } from "crypto";
import { checkRequestSchema, type FastApiCheckRequest } from "../contracts/check";
import { fastapiCheck } from "../services/fastapi";

export const checkRouter = Router();

checkRouter.post(
  "/check",
  async (req: Request, res: Response, next: NextFunction): Promise<void> => {
    try {
      const parsed = checkRequestSchema.safeParse(req.body);

      if (!parsed.success) {
        res.status(400).json({
          error: "bad_request",
          details: parsed.error.flatten(),
        });
        return;
      }

      const { role, input } = parsed.data;

      const requestId = randomUUID();
      const receivedAt = new Date().toISOString();

      const explainLevel = role === "doctor" ? "detailed" : "simple";

      const fastapiPayload: FastApiCheckRequest = {
        request_id: requestId,
        received_at: receivedAt,
        role,
        input,
        options: {
          guideline_scope: "ru_minzdrav",
          explain_level: explainLevel,
          return_sources: true,
          safety_mode: "strict",
        },
      };

      const { status, data } = await fastapiCheck(fastapiPayload);

      res.status(status).json(data);
    } catch (err) {
      next(err);
    }
  }
);

