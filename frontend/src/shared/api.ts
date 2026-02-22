export const API_BASE_URL = import.meta.env.VITE_API_BASE_URL as string;

export type CheckRequestDto = {
  role: "doctor" | "patient";
  locale: string;
  input: unknown; // можно ужесточить под твою schema
};

export async function postCheck(payload: CheckRequestDto, signal?: AbortSignal) {
  const resp = await fetch(`${API_BASE_URL}/api/check`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
    signal,
  });

  const data = await resp.json().catch(() => null);

  if (!resp.ok) {
    // data может быть { error, details } или что вернул fastapi
    const msg = data?.error?.message || data?.message || data?.error || `HTTP ${resp.status}`;
    throw new Error(msg);
  }

  return data;
}
