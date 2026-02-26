import { z } from "zod";

export const chatRequestSchema = z.object({
    request_id: z.string(),
    message: z.string().min(1),
    role: z.enum(["doctor", "patient"]),
    history: z.array(
        z.object({
            role: z.enum(["user", "assistant"]),
            content: z.string(),
        })
    ),
    context: z.record(z.string(), z.any()).optional(),
});

export type FastApiChatRequest = z.infer<typeof chatRequestSchema>;

export type FastApiChatResponse = {
    request_id: string;
    message: string;
    history: {
        role: string;
        content: string;
    }[];
};