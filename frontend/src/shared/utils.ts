export function formatResponse(
    resp: any,
    role: "doctor" | "patient" | undefined
) {
    const { result = {}, sources = [] } = resp ?? {};

    if (role === "patient") {
        return `
Ответ по вашему лечению

${result.patient_explanation ?? ""}
`.trim();
    }

    const complianceBlock = result.is_compliant
        ? "Соответствует клиническим рекомендациям."
        : "Обнаружены несоответствия клиническим рекомендациям.";

    const issuesBlock = result.issues?.length
        ? `
Выявленные проблемы:
${result.issues
            .map((i: any, idx: number) => {
                const severityLabel =
                    i.severity === "critical"
                        ? "Критическая проблема"
                        : "Замечание";

                return `
${idx + 1}. ${i.title}
Статус: ${severityLabel}
Подробности: ${i.details}
-----------------------------------`;
            })
            .join("\n")}`
        : "";

    const nextStepsBlock = result.recommended_next_steps?.length
        ? `
Рекомендуемые действия:
${result.recommended_next_steps
            .map((step: string, i: number) => `${i + 1}. ${step}`)
            .join("\n")}`
        : "";

    const sourcesBlock = sources?.length
        ? `
Использованные источники:
${sources
            .map((s: any) => `• ${s.source} (Раздел: ${s.section})`)
            .join("\n")}`
        : "";

    return `
Результат проверки

${complianceBlock}

Объяснение:
${result.doctor_explanation ?? ""}

${issuesBlock}

${nextStepsBlock}

Итог: ${result.summary ?? ""}

${sourcesBlock}
`.trim();
}