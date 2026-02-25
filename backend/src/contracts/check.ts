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
// contracts/check.ts

export type ApiStatus = "ok" | "error";

export interface ApiErrorPayload {
    code: string;
    message: string;
    details?: unknown;
}

export interface VerifyResponseOk {
    request_id: string;
    status: "ok";
    result: VerificationResult;
    sources: SourceExcerpt[];
    warnings: WarningItem[];
}

export interface VerifyResponseError {
    request_id: string;
    status: "error";
    error: ApiErrorPayload;
    // на ошибке часто нет result/sources/warnings — сделаем опциональными
    result?: never;
    sources?: never;
    warnings?: never;
}

export type VerifyResponse = VerifyResponseOk | VerifyResponseError;

// ---- твои уже существующие типы ниже (без изменений) ----

export interface VerificationResult {
    is_compliant: boolean;
    summary: string;
    issues: Issue[];
    doctor_explanation: string;
    patient_explanation: string;
    recommended_next_steps: string[];
}

export type IssueCode =
    | "REGIMEN_LINE_MISMATCH"
    | "MISSED_IMMUNOTHERAPY_SUPPORT"
    | string;

export type IssueSeverity = "low" | "medium" | "high" | "critical" | string;

export interface Issue {
    code: IssueCode;
    severity: IssueSeverity;
    title: string;
    details: string;
    suggested_action: string;
    confidence: number;
}

export interface SourceExcerpt {
    source: string;
    section: string;
    text: string;
    doc_id: string;
}

export interface WarningItem {
    code?: string;
    severity?: IssueSeverity;
    title?: string;
    details?: string;
    [k: string]: unknown;
}