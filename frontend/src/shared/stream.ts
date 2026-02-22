type StreamOptions = {
  chunkSize?: number;   // размер порции символов
  delayMs?: number;     // задержка между порциями
  signal?: AbortSignal; // чтобы можно было отменять
};

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** Разбивает строку на части и "стримит" их по времени */
export async function streamText(
  fullText: string,
  onChunk: (chunk: string) => void,
  opts: StreamOptions = {}
) {
  const chunkSize = opts.chunkSize ?? 6;
  const delayMs = opts.delayMs ?? 20;
  const signal = opts.signal;

  for (let i = 0; i < fullText.length; i += chunkSize) {
    if (signal?.aborted) throw new DOMException('Aborted', 'AbortError');
    onChunk(fullText.slice(i, i + chunkSize));
    await sleep(delayMs);
  }
}

/** Заглушка "API": тут потом заменишь на реальный fetch */
export async function fakeAiGenerateAnswer(params: {
  role: 'doctor' | 'patient';
  input: string;
  signal?: AbortSignal;
}): Promise<string> {
  const { role, input, signal } = params;

  // небольшая задержка "как сеть"
  await sleep(250);
  if (signal?.aborted) throw new DOMException('Aborted', 'AbortError');

  const head =
    role === 'doctor'
      ? 'Клинический разбор (черновик):\n'
      : 'Пояснение простыми словами (черновик):\n';

  // Текст-заглушка. В реале сюда придёт ответ с бэка.
  return (
    head +
    `Получен текст длиной ${input.length} символов.\n\n` +
    `1) Что важно уточнить: симптомы, длительность, температура, хронические заболевания.\n` +
    `2) Возможные шаги: оценка рисков, рекомендации по дальнейшим действиям.\n` +
    `3) Предупреждение: это прототип для AI-хакатона, не медицинская консультация.\n`
  );
}
