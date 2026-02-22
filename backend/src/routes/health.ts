import { Router, type Request, type Response, type NextFunction } from "express";
import { pool } from "../db/pool";

export const healthRouter = Router();

healthRouter.get(
  "/health",
  async (_req: Request, res: Response, next: NextFunction): Promise<void> => {
    try {
      const result = await pool.query<{ ok: number }>("SELECT 1 as ok");

      res.json({
        status: "ok",
        db: result.rows[0]?.ok === 1,
      });
    } catch (error) {
      next(error);
    }
  }
);
