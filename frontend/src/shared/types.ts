export type Role = 'doctor' | 'patient';

export const ROLE_LABEL: Record<Role, string> = {
  doctor: 'Врач',
  patient: 'Пациент',
};

export type ChatRole = "user" | "assistant";

export interface ChatMessage {
    id: string;
    role: ChatRole;
    content: string;
    createdAt: number;
}