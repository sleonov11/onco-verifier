import { useMemo, useRef, useState } from 'react';
import {
  Alert,
  Button,
  Card,
  Collapse,
  Divider,
  Form,
  Input,
  InputNumber,
  Radio,
  Select,
  Space,
  Tag,
  Typography,
  message,
} from 'antd';
import { Controller, useForm, type SubmitHandler } from 'react-hook-form';
import { z } from 'zod';
import { zodResolver } from '@hookform/resolvers/zod';

import type { Role } from '../shared/types';
import { ROLE_LABEL } from '../shared/types';
import { streamText } from '../shared/stream';

import { postCheck } from "../shared/api";
import { Chat } from "../components/Chat/Chat";
import {chatStore} from "../stores/chat.ts";
import {userStore} from "../stores/user.ts";

const { Text, Title } = Typography;

type GuidelineScope = 'ru_minzdrav' | 'intl' | 'mixed';

const GUIDELINE_SCOPE_LABEL: Record<GuidelineScope, string> = {
  ru_minzdrav: 'Минздрав РФ (приоритет)',
  intl: 'NCCN/ESMO (международные)',
  mixed: 'Смешанный (приоритет РФ)',
};

const sexOptions = [
  { label: 'Мужской', value: 'male' },
  { label: 'Женский', value: 'female' },
  { label: 'Другое/не указано', value: 'other' },
];

const markerOptions = [
  { label: 'Положительный', value: 'positive' },
  { label: 'Отрицательный', value: 'negative' },
  { label: 'Неизвестно', value: 'unknown' },
];

const stageOptions = [
  '0',
  'I',
  'IA',
  'IB',
  'II',
  'IIA',
  'IIB',
  'III',
  'IIIA',
  'IIIB',
  'IIIC',
  'IV',
  'IVA',
  'IVB',
].map((s) => ({ label: s, value: s }));

const histologyOptions = [
  { label: 'Аденокарцинома', value: 'Аденокарцинома' },
  { label: 'Плоскоклеточный', value: 'Плоскоклеточный' },
  { label: 'Мелкоклеточный', value: 'Мелкоклеточный' },
  { label: 'Другое', value: 'Другое' },
];

const cycleOptions = [
  { label: 'q1w (еженедельно)', value: 'q1w' },
  { label: 'q2w (раз в 2 недели)', value: 'q2w' },
  { label: 'q3w (раз в 3 недели)', value: 'q3w' },
  { label: 'q4w (раз в 4 недели)', value: 'q4w' },
];

const schema = z.object({
  role: z.enum(['doctor', 'patient']),
  locale: z.string().min(2).default('ru'),

  options: z.object({
    guideline_scope: z.enum(['ru_minzdrav', 'intl', 'mixed']).default('ru_minzdrav'),
    explain_level: z.enum(['short', 'detailed']).default('detailed'),
    return_sources: z.boolean().default(true),
    safety_mode: z.enum(['strict', 'normal']).default('strict'),
  }),

  input: z.object({
    diagnosis: z.object({
      name: z.string().trim().min(2, 'Укажи диагноз (минимум 2 символа).'),
      icd10: z.string().trim(),
      stage: z.string().trim(),
      tnm: z.string().trim(),
      histology: z.string().trim(),
    }),

    molecular_markers: z.object({
      EGFR: z.enum(['positive', 'negative', 'unknown']),
      ALK: z.enum(['positive', 'negative', 'unknown']),
      PD_L1: z.string().trim(),
    }),

    patient_context: z.object({
      age: z.number().int().min(0).max(130),
      sex: z.enum(['male', 'female', 'other']),
      comorbidities: z.array(z.string().trim().min(1)).default([]),
      symptoms: z.array(z.string().trim().min(1)).default([]),
    }),

    treatment: z.object({
      therapy_line: z.number().int().min(1).max(10),
      proposed_regimen: z.array(z.string().trim().min(1)).default([]),
      cycle: z.string().trim(),
      notes: z.string().trim(),
    }),

    free_text: z.object({
      doctor_notes: z.string().trim().default(''),
      patient_message: z.string().trim().default(''),
    }),
  }),
});

type FormValues = z.input<typeof schema>;   // то, что вводит форма (может быть undefined)
type ParsedValues = z.infer<typeof schema>;

/** Простая обвязка для TagsInput через Select mode="tags" */
function TagsField({
                     value,
                     onChange,
                     placeholder,
    disabled,
                   }: {
  value?: string[];
  onChange?: (v: string[]) => void;
  placeholder?: string;
  disabled?: boolean;
}) {
  return (
    <Select
      mode="tags"
      value={value}
      onChange={onChange}
      tokenSeparators={[',', ';']}
      placeholder={placeholder}
      style={{ width: '100%' }}
      maxTagCount="responsive"
      options={[]}
      disabled={disabled}
    />
  );
}

export function AiChatPage() {
  const [_, setAnswer] = useState<string>('');
  const [isStreaming, setIsStreaming] = useState(false);

  const abortRef = useRef<AbortController | null>(null);

  const {
    control,
    handleSubmit,
    watch,
    formState: { errors, isSubmitting },
  } = useForm<FormValues>({
    resolver: zodResolver(schema),
    defaultValues: {
      role: userStore.role,
      locale: 'ru',
      options: {
        guideline_scope: 'ru_minzdrav',
        explain_level: 'detailed',
        return_sources: true,
        safety_mode: 'strict',
      },
      input: {
        diagnosis: { name: '' },
        molecular_markers: {},
        patient_context: { comorbidities: [], symptoms: [] },
        treatment: { proposed_regimen: [] },
        free_text: { doctor_notes: '', patient_message: '' },
      },
    },
    mode: 'onChange',
  });

  const role = watch('role');
  const guidelineScope = watch('options.guideline_scope');

  const roleHint = useMemo(() => {
    const r: Role = role;
    return r === 'doctor'
      ? 'Режим врача: структурированные поля + клиническая терминология.'
      : 'Режим пациента: минимальные поля + объяснение простыми словами.';
  }, [role]);

  const scopeHint = useMemo(() => {
    if (guidelineScope === 'ru_minzdrav') return 'Ответ будет опираться на клинические рекомендации Минздрава РФ.';
    if (guidelineScope === 'intl') return 'Ответ будет опираться на международные гайдлайны (NCCN/ESMO).';
    return 'Ответ может использовать оба источника, приоритет — Минздрав РФ.';
  }, [guidelineScope]);

  const stopStreaming = () => {
    abortRef.current?.abort();
  };

  const onSubmit: SubmitHandler<FormValues> = async (values) => {
    stopStreaming();
    abortRef.current = new AbortController();

    setAnswer('');
    setIsStreaming(true);

    try {
      const data: ParsedValues = schema.parse(values);

      const apiResp = await postCheck(
        {
          role: data.role,
          locale: data.locale,
          input: data.input,
        },
        abortRef.current.signal
      );

      console.log(apiResp)


      const full = JSON.stringify(apiResp, null, 2);

      await streamText(
        full,
        (chunk) => setAnswer((prev) => prev + chunk),
        { chunkSize: 6, delayMs: 18, signal: abortRef.current.signal }
      );
    } catch (e: any) {
      if (e?.name !== 'AbortError') {
        message.error('Ошибка генерации');
      }
    } finally {
      setIsStreaming(false);
    }
  };

  const diagnosisError = (errors.input as any)?.diagnosis?.name?.message;

  return (
    <Space orientation="vertical" size={16} style={{ width: '100%' }}>
      <Card>
        <Title level={3} style={{ marginTop: 0 }}>
          AI-помощник проверки лечения
        </Title>

        <Text type="secondary">
            {role === 'doctor' ? "Для врача доступен структурированный режим — он повышает точность проверки."
            : "Для пациента доступен режим получения отзыва о лечении."
            }
        </Text>

        <Divider />

        <Form layout="vertical" onFinish={handleSubmit(onSubmit)}>
          {/* Диагноз (минимум обязателен всегда) */}
          <Form.Item
            label="Диагноз"
            validateStatus={diagnosisError ? 'error' : undefined}
            help={diagnosisError}
          >
            <Controller
              control={control}
              name="input.diagnosis.name"
              render={({ field }) => (
                <Input
                  {...field}
                  disabled={role === 'patient'}
                  placeholder={role === 'doctor' ? 'Напр.: Немелкоклеточный рак лёгкого' : 'Напр.: рак лёгкого'}
                />
              )}
            />
          </Form.Item>

          {/* Свободный текст — главный для пациента, важный и для врача */}
          <Form.Item
            label={role === 'doctor' ? 'Текст выписки / назначения' : 'Сообщение (что назначили и что беспокоит)'}
          >
            <Controller
              control={control}
              name={role === 'doctor' ? 'input.free_text.doctor_notes' : 'input.free_text.patient_message'}
              render={({ field }) => (
                <Input.TextArea
                  {...field}
                  placeholder={
                    role === 'doctor'
                      ? 'Анамнез, стадия, маркеры, назначения, дозировки, комментарии...'
                      : 'Что сказал врач, какие лекарства/процедуры назначили, симптомы, анализы если есть...'
                  }
                  autoSize={{ minRows: 6, maxRows: 14 }}
                />
              )}
            />
          </Form.Item>

          {/* Структурированный режим: показываем всегда, но подсветим что “для врача полезнее” */}
          {
            role === 'doctor' &&
            <Alert
              type="info"
              showIcon
              title="Структурированные поля повышают точность"
              description={
                  role === 'doctor'
                      ? 'Заполни по возможности стадию, TNM, гистологию, маркеры и схему лечения — так проверка будет точнее.'
                      : 'Если знаешь стадию/лекарства — можешь указать. Если нет — достаточно текста выше.'
              }
              style={{marginBottom: 12}}
            />
          }

          <Collapse
            defaultActiveKey={role === 'doctor' ? ['dx', 'tx'] : []}
            items={[
              {
                key: 'dx',
                label: 'Диагноз (расширенно)',
                children: (
                  <Space orientation="vertical" size={12} style={{ width: '100%' }}>
                    <Space wrap style={{ width: '100%' }}>
                      <Form.Item label="МКБ-10" style={{ marginBottom: 0, minWidth: 220 }}>
                        <Controller
                          control={control}
                          name="input.diagnosis.icd10"
                          render={({ field }) => <Input {...field} placeholder="Напр.: C34" />}
                          disabled={role === 'patient'}
                        />
                      </Form.Item>

                      <Form.Item label="Стадия" style={{ marginBottom: 0, minWidth: 220 }}>
                        <Controller
                          control={control}
                          name="input.diagnosis.stage"
                          render={({ field }) => <Select {...field} allowClear options={stageOptions} placeholder="Выбери" />}
                          disabled={role === 'patient'}
                        />
                      </Form.Item>

                      <Form.Item label="TNM" style={{ marginBottom: 0, minWidth: 220 }}>
                        <Controller
                          control={control}
                          name="input.diagnosis.tnm"
                          render={({ field }) => <Input {...field} placeholder="Напр.: T1N2M0" />}
                          disabled={role === 'patient'}
                        />
                      </Form.Item>

                      <Form.Item label="Гистология" style={{ marginBottom: 0, minWidth: 260 }}>
                        <Controller
                          control={control}
                          name="input.diagnosis.histology"
                          render={({ field }) => <Select {...field} allowClear options={histologyOptions} placeholder="Выбери" />}
                          disabled={role === 'patient'}
                        />
                      </Form.Item>
                    </Space>
                  </Space>
                ),
              },
              {
                key: 'markers',
                label: 'Молекулярные маркеры',
                children: (
                  <Space orientation="vertical" size={12} style={{ width: '100%' }}>
                    <Space wrap style={{ width: '100%' }}>
                      <Form.Item label="EGFR" style={{ marginBottom: 0, minWidth: 220 }}>
                        <Controller
                          control={control}
                          name="input.molecular_markers.EGFR"
                          render={({ field }) => (
                            <Select {...field} allowClear options={markerOptions} placeholder="Выбери" />
                          )}
                          disabled={role === 'patient'}
                        />
                      </Form.Item>

                      <Form.Item label="ALK" style={{ marginBottom: 0, minWidth: 220 }}>
                        <Controller
                          control={control}
                          name="input.molecular_markers.ALK"
                          render={({ field }) => (
                            <Select {...field} allowClear options={markerOptions} placeholder="Выбери" />
                          )}
                          disabled={role === 'patient'}
                        />
                      </Form.Item>

                      <Form.Item label="PD-L1" style={{ marginBottom: 0, minWidth: 220 }}>
                        <Controller
                          control={control}
                          name="input.molecular_markers.PD_L1"
                          render={({ field }) => <Input {...field} placeholder='Напр.: "60%" или ">=50%"' />}
                          disabled={role === 'patient'}
                        />
                      </Form.Item>
                    </Space>
                  </Space>
                ),
              },
              {
                key: 'patient',
                label: 'Контекст пациента',
                children: (
                  <Space orientation="vertical" size={12} style={{ width: '100%' }}>
                    <Space wrap style={{ width: '100%' }}>
                      <Form.Item label="Возраст" style={{ marginBottom: 0, minWidth: 220 }}>
                        <Controller
                          control={control}
                          name="input.patient_context.age"
                          render={({ field }) => (
                            <InputNumber
                              {...field}
                              style={{ width: '100%' }}
                              min={0}
                              max={130}
                              placeholder="Напр.: 61"
                            />
                          )}
                          disabled={role === 'patient'}
                        />
                      </Form.Item>

                      <Form.Item label="Пол" style={{ marginBottom: 0, minWidth: 260 }}>
                        <Controller
                          control={control}
                          name="input.patient_context.sex"
                          render={({ field }) => (
                            <Select {...field} allowClear options={sexOptions} placeholder="Выбери" />
                          )}
                          disabled={role === 'patient'}
                        />
                      </Form.Item>
                    </Space>

                    <Form.Item label="Сопутствующие заболевания" style={{ marginBottom: 0 }}>
                      <Controller
                        control={control}
                        name="input.patient_context.comorbidities"
                        render={({ field }) => (
                          <TagsField
                            value={field.value}
                            onChange={field.onChange}
                            placeholder="Напр.: ХОБЛ, СД2, ИБС..."
                            disabled={role === 'patient'}
                          />
                        )}
                        disabled={role === 'patient'}
                      />
                    </Form.Item>

                    <Form.Item label="Симптомы" style={{ marginBottom: 0 }}>
                      <Controller
                        control={control}
                        name="input.patient_context.symptoms"
                        render={({ field }) => (
                          <TagsField
                            value={field.value}
                            onChange={field.onChange}
                            placeholder="Напр.: кашель, одышка, боль..."
                            disabled={role === 'patient'}
                          />
                        )}
                        disabled={role === 'patient'}
                      />
                    </Form.Item>
                  </Space>
                ),
              },
              {
                key: 'tx',
                label: 'Назначенное лечение',
                children: (
                  <Space orientation="vertical" size={12} style={{ width: '100%' }}>
                    <Space wrap style={{ width: '100%' }}>
                      <Form.Item label="Линия терапии" style={{ marginBottom: 0, minWidth: 220 }}>
                        <Controller
                          control={control}
                          name="input.treatment.therapy_line"
                          render={({ field }) => (
                            <InputNumber {...field} style={{ width: '100%' }} min={1} max={10} placeholder="Напр.: 1" />
                          )}
                          disabled={role === 'patient'}
                        />
                      </Form.Item>

                      <Form.Item label="Цикл/режим" style={{ marginBottom: 0, minWidth: 260 }}>
                        <Controller
                          control={control}
                          name="input.treatment.cycle"
                          render={({ field }) => (
                            <Select
                              {...field}
                              allowClear
                              options={cycleOptions}
                              placeholder="Выбери или оставь пустым"
                              disabled={role === 'patient'}
                            />
                          )}
                          disabled={role === 'patient'}
                        />
                      </Form.Item>
                    </Space>

                    <Form.Item label="Схема (препараты)" style={{ marginBottom: 0 }}>
                      <Controller
                        control={control}
                        name="input.treatment.proposed_regimen"
                        render={({ field }) => (
                          <TagsField
                            value={field.value}
                            onChange={field.onChange}
                            placeholder="Напр.: Пембролизумаб, Пеметрексед, Цисплатин"
                            disabled={role === 'patient'}
                          />
                        )}
                        disabled={role === 'patient'}
                      />
                    </Form.Item>

                    <Form.Item label="Комментарий к назначению" style={{ marginBottom: 0 }}>
                      <Controller
                        control={control}
                        name="input.treatment.notes"
                        render={({ field }) => (
                          <Input.TextArea
                            {...field}
                            placeholder="Дозировки, число циклов, противопоказания, переносимость..."
                            autoSize={{ minRows: 3, maxRows: 8 }}
                            disabled={role === 'patient'}
                          />
                        )}
                        disabled={role === 'patient'}
                      />
                    </Form.Item>
                  </Space>
                ),
              },
              // {
              //   key: 'opts',
              //   label: 'Параметры ответа (LLM options)',
              //   children: (
              //     <Space orientation="vertical" size={12} style={{ width: '100%' }}>
              //       <Form.Item label="Уровень объяснения" style={{ marginBottom: 0 }}>
              //         <Controller
              //           control={control}
              //           name="options.explain_level"
              //           render={({ field }) => (
              //             <Radio.Group
              //               {...field}
              //               options={[
              //                 { label: 'Коротко', value: 'short' },
              //                 { label: 'Подробно', value: 'detailed' },
              //               ]}
              //               optionType="button"
              //               buttonStyle="solid"
              //             />
              //           )}
              //         />
              //       </Form.Item>
              //
              //       <Form.Item label="Возвращать источники" style={{ marginBottom: 0 }}>
              //         <Controller
              //           control={control}
              //           name="options.return_sources"
              //           render={({ field }) => (
              //             <Radio.Group
              //               value={field.value ? 'yes' : 'no'}
              //               onChange={(e) => field.onChange(e.target.value === 'yes')}
              //               options={[
              //                 { label: 'Да', value: 'yes' },
              //                 { label: 'Нет', value: 'no' },
              //               ]}
              //               optionType="button"
              //               buttonStyle="solid"
              //             />
              //           )}
              //         />
              //       </Form.Item>
              //
              //       <Form.Item label="Safety mode" style={{ marginBottom: 0 }}>
              //         <Controller
              //           control={control}
              //           name="options.safety_mode"
              //           render={({ field }) => (
              //             <Radio.Group
              //               {...field}
              //               options={[
              //                 { label: 'Strict', value: 'strict' },
              //                 { label: 'Normal', value: 'normal' },
              //               ]}
              //               optionType="button"
              //               buttonStyle="solid"
              //             />
              //           )}
              //         />
              //       </Form.Item>
              //     </Space>
              //   ),
              // },
            ]}
          />

          <Divider />

          <Space>
            <Button type="primary" htmlType="submit" loading={isSubmitting || isStreaming} disabled={isSubmitting || isStreaming}>
              Сравнить
            </Button>

            <Button onClick={stopStreaming} disabled={!isStreaming} danger>
              Стоп
            </Button>
          </Space>
        </Form>
      </Card>

      {/*<Card title="Ответ" extra={isStreaming ? <Text type="secondary">печатает…</Text> : null}>*/}
      {/*  {answer ? (*/}
      {/*    <pre style={{ margin: 0, whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}>{answer}</pre>*/}
      {/*  ) : (*/}
      {/*    <Text type="secondary">Пока пусто. Заполни поля и нажми «Проверить» — тут появится ответ.</Text>*/}
      {/*  )}*/}
      {/*</Card>*/}
        {chatStore.messages.length > 0 && <Chat
            title="AI-диалог"
            onSend={async (_) => {
                // сюда логика запроса к backend
                const data: ParsedValues = schema.parse(watch());

                const apiResp = await postCheck(
                    {
                        role: data.role,
                        locale: data.locale,
                        input: data.input,
                    }
                );

                chatStore.addAssistantMessage(JSON.stringify(apiResp, null, 2));
            }}
        />}
    </Space>
  );
}
