import { makeAutoObservable, reaction } from "mobx";

export type ChatRole = "user" | "assistant";

export interface ChatMessage {
    id: string;
    role: ChatRole;
    content: string;
    createdAt: number;
}

class ChatStore {
    messages: ChatMessage[] = [];

    private STORAGE_KEY = "onco_chat";

    constructor() {
        makeAutoObservable(this);

        this.loadFromLocalStorage();

        reaction(
            () => this.messages.map(m => ({
                id: m.id,
                role: m.role,
                content: m.content,
                createdAt: m.createdAt,
            })),
            (messages) => {
                localStorage.setItem(
                    this.STORAGE_KEY,
                    JSON.stringify(messages)
                );
            }
        );
    }

    addUserMessage(content: string) {
        this.messages.push({
            id: crypto.randomUUID(),
            role: "user",
            content,
            createdAt: Date.now(),
        });
    }

    addAssistantMessage(content: string) {
        this.messages.push({
            id: crypto.randomUUID(),
            role: "assistant",
            content,
            createdAt: Date.now(),
        });
    }

    clear() {
        this.messages = [];
        localStorage.removeItem(this.STORAGE_KEY);
    }

    private loadFromLocalStorage() {
        try {
            const raw = localStorage.getItem(this.STORAGE_KEY);
            if (!raw) return;

            const parsed: ChatMessage[] = JSON.parse(raw);
            this.messages = parsed ?? [];
        } catch {
            console.warn("Failed to load chat from localStorage");
        }
    }
}

export const chatStore = new ChatStore();