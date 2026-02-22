import { z } from "zod";

export const roleSchema = z.enum(["doctor", "patient"]);

export const checkRequestSchema = z.object({
  role: roleSchema,
  locale: z.string().min(2),
  input: z.object({}).passthrough()
});

export type CheckRequest = z.infer<typeof checkRequestSchema>;

// То, что отправляем в FastAPI (бек добавляет поля)
export type FastApiCheckRequest = {
  request_id: string;
  received_at: string;
  role: "doctor" | "patient";
  input: Record<string, unknown>;
  options: {
    guideline_scope: string;
    explain_level: "simple" | "detailed";
    return_sources: boolean;
    safety_mode: "strict" | "default";
  };
};

// То, что ожидаем от FastAPI (минимум)
export type FastApiCheckResponse = {
  request_id: string;
  status: "ok" | "error";
  result?: unknown;
  sources?: unknown[];
  warnings?: unknown[];
  error?: { code?: string; message?: string; details?: unknown };
};
