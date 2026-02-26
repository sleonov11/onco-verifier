import { makeAutoObservable, reaction } from "mobx";

export type UserRole = "doctor" | "patient" | undefined;

class UserStore {
    name: string = "";
    role: UserRole = undefined;

    private STORAGE_KEY = "onco_user";

    constructor() {
        makeAutoObservable(this);

        this.loadFromLocalStorage();

        reaction(
            () => ({
                name: this.name,
                role: this.role,
            }),
            (data) => {
                localStorage.setItem(this.STORAGE_KEY, JSON.stringify(data));
            }
        );
    }

    setName(name: string) {
        this.name = name;
    }

    setRole(role: UserRole) {
        this.role = role;
    }

    reset() {
        this.name = "";
        this.role = undefined;
        localStorage.removeItem(this.STORAGE_KEY);
    }

    private loadFromLocalStorage() {
        try {
            const raw = localStorage.getItem(this.STORAGE_KEY);
            if (!raw) return;

            const parsed = JSON.parse(raw);

            this.name = parsed.name ?? "";
            this.role = parsed.role ?? null;
        } catch {
            console.warn("Failed to load user from localStorage");
        }
    }
}

export const userStore = new UserStore();