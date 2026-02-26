import { Router, type Request, type Response, type NextFunction } from "express";
import { chatRequestSchema } from "../contracts/chat";
import { fastapiChat } from "../services/fastapi";

export const chatRouter = Router();

chatRouter.post(
    "/chat",
    async (req: Request, res: Response, next: NextFunction): Promise<void> => {
        try {
            const parsed = chatRequestSchema.safeParse(req.body);

            if (!parsed.success) {
                res.status(400).json({
                    error: "bad_request",
                    details: parsed.error.flatten(),
                });
                return;
            }

            const { status, data } = await fastapiChat(parsed.data);

            res.status(status).json(data);
        } catch (err) {
            next(err);
        }
    }
);