import axios, { AxiosError } from "axios";
import { env } from "../config/env";
import { FastApiCheckRequest, VerifyResponse } from "../contracts/check";

export async function fastapiCheck(
    payload: FastApiCheckRequest
): Promise<{ status: number; data: VerifyResponse }> {
    const url = `${env.fastapiUrl}${env.fastapiCheckPath}`;

    try {
        const resp = await axios.post<VerifyResponse>(url, payload, {
            timeout: 60_000,
            headers: { "Content-Type": "application/json" },
            validateStatus: () => true,
        });

        return { status: resp.status, data: resp.data };
    } catch (error) {
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
                        details: {
                            axiosMessage: error.message,
                            axiosCode: error.code,
                        },
                    },
                },
            };
        }

        throw error;
    }
}

import { FastApiChatRequest, FastApiChatResponse } from "../contracts/chat";

export async function fastapiChat(
    payload: FastApiChatRequest
): Promise<{ status: number; data: FastApiChatResponse }> {
    const url = `${env.fastapiUrl}/chat`;

    try {
        const resp = await axios.post<FastApiChatResponse>(url, payload, {
            timeout: 60_000,
            headers: { "Content-Type": "application/json" },
            validateStatus: () => true,
        });

        return { status: resp.status, data: resp.data };
    } catch (error) {
        if (error instanceof AxiosError) {
            console.error("FastAPI chat network error:", error.message);

            return {
                status: 502,
                data: {
                    request_id: payload.request_id,
                    message: "FastAPI chat service unavailable",
                    history: [],
                },
            };
        }

        throw error;
    }
}