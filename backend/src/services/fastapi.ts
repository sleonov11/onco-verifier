import axios, { AxiosError } from "axios";
import { env } from "../config/env";
import type {
  FastApiCheckRequest,
  FastApiCheckResponse,
} from "../contracts/check";

export async function fastapiCheck(
  payload: FastApiCheckRequest
): Promise<{ status: number; data: FastApiCheckResponse }> {
  const url = `${env.fastapiUrl}${env.fastapiCheckPath}`;

  try {
    const resp = await axios.post<FastApiCheckResponse>(url, payload, {
      timeout: 60_000,
      headers: { "Content-Type": "application/json" },
      validateStatus: () => true, // не бросаем исключение на 4xx/5xx
    });

    return { status: resp.status, data: resp.data };
  } catch (error) {
    // Если FastAPI недоступен (connection refused, timeout и т.п.)
    if (error instanceof AxiosError) {
      console.error("FastAPI network error:", error.message);

      return {
        status: 502,
        data: {
          request_id: payload.request_id,
          status: "error",
          error: {
            code: "FASTAPI_UNAVAILABLE",
            message: "FastAPI service is unavailable",
          },
        },
      };
    }

    throw error;
  }
}
