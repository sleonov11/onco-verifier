import type { Request, Response, NextFunction } from "express";

export function errorMiddleware(
  err: unknown,
  _req: Request,
  res: Response,
  _next: NextFunction
): void {
  console.error(err);

  const isDev = process.env.NODE_ENV === "development";

  let message = "Something went wrong";

  if (isDev) {
    if (err instanceof Error) {
      message = err.message;
    } else {
      message = String(err);
    }
  }

  res.status(500).json({
    error: "internal_error",
    message,
  });
}
