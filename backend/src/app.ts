import express, { type Express, type Request, type Response } from "express";
import { healthRouter } from "./routes/health";
import { errorMiddleware } from "./middlewares/error";
import {checkRouter} from "./routes/check";
import cors from "cors";
import {surveyRouter} from "./routes/survey";

export function createApp(): Express {
  const app = express();

  app.use(
    cors({
      origin: ["http://localhost:5173", "http://localhost:5174"],
      methods: ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
      allowedHeaders: ["Content-Type", "Authorization"],
      credentials: true,
    })
  );

  app.use(express.json({ limit: "1mb" }));

  app.use("/api", healthRouter);
  app.use("/api", checkRouter);
  app.use('/api', surveyRouter);

  app.get("/", (_req: Request, res: Response) => {
    res.send("OK");
  });

  app.use(errorMiddleware);

  return app;
}
