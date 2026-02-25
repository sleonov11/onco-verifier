'use client';

import { useEffect, useMemo } from 'react';
import { Alert, Button, Card, Divider, Form, Input, Radio, Select, Space, Typography, message, InputNumber } from 'antd';
import { Controller, useForm, type SubmitHandler } from 'react-hook-form';
import { z } from 'zod';
import { zodResolver } from '@hookform/resolvers/zod';

import {userStore} from "../stores/user.ts";
import { observer } from "mobx-react-lite";

const { Text, Title } = Typography;

const usesGuidelinesOptions = [
  { label: 'Часто', value: 'often' },
  { label: 'Иногда', value: 'sometimes' },
  { label: 'Редко', value: 'rarely' },
] as const;

const hasDiagnosisOptions = [
  { label: 'Да', value: 'yes' },
  { label: 'Нет', value: 'no' },
  { label: 'Не уверен(а)', value: 'unknown' },
] as const;

const treatmentStageOptions = [
  { label: 'Планируется лечение', value: 'planning' },
  { label: 'Лечение идёт сейчас', value: 'in_progress' },
  { label: 'Лечение завершено', value: 'after' },
  { label: 'Не знаю', value: 'unknown' },
] as const;

const wantsExplanationOptions = [
  { label: 'Простыми словами', value: 'simple' },
  { label: 'Подробно', value: 'detailed' },
] as const;

const schema = z.object({
  role: z.enum(['doctor', 'patient']),
  name: z.string().trim().min(2, 'Минимум 2 символа.').max(80),
  email: z.string().trim().email('Некорректный email.').max(120),

  // doctor
  specialty: z.string().trim().max(120).optional(),
  workplace: z.string().trim().max(120).optional(),
  experience_years: z.preprocess(
    (v) => (v === '' || v === null || v === undefined ? undefined : v),
    z.coerce.number().int().min(0).max(60).optional(),
  ),
  uses_guidelines: z.enum(['often', 'sometimes', 'rarely']).optional(),
  main_pain: z.string().trim().max(2000).optional(),

  // patient
  symptoms: z.string().trim().max(2000).optional(),
  has_diagnosis: z.enum(['yes', 'no', 'unknown']).optional(),
  treatment_stage: z.enum(['planning', 'in_progress', 'after', 'unknown']).optional(),
  wants_explanation: z.enum(['simple', 'detailed']).optional(),
});

type SurveyValues = z.infer<typeof schema>;

export const SurveyPage = observer(function SurveyPage() {
  const {
    control,
    handleSubmit,
    formState: { errors, isSubmitting },
    reset,
  } = useForm<SurveyValues>({
    resolver: zodResolver(schema) as any,
    shouldUnregister: true, // ✅ важно: удаляет значения размонтированных полей
    defaultValues: {
      role: userStore.role,
      name: userStore.name,
      email: '',

      // doctor
      specialty: '',
      workplace: '',
      experience_years: undefined,
      uses_guidelines: 'often',
      main_pain: '',

      // patient
      symptoms: '',
      has_diagnosis: 'unknown',
      treatment_stage: 'unknown',
      wants_explanation: 'simple',
    },
    mode: 'onChange',
  });

  const role = userStore.role;

  const hint = useMemo(() => {
    return role === 'doctor'
      ? 'Пара вопросов помогут настроить режим для врача (язык, источники, форма результата).'
      : 'Пара вопросов помогут сделать ответы понятнее и полезнее для пациента.';
  }, [role]);

    useEffect(() => {
        const common = {
            role: userStore.role,
            name: userStore.name ?? "",
            email: "",
        };

        if (userStore.role === "doctor") {
            reset(
                {
                    ...common,
                    specialty: "",
                    workplace: "",
                    experience_years: undefined,
                    uses_guidelines: "often",
                    main_pain: "",
                    // patient-поля не задаём (shouldUnregister удалит)
                },
                { keepDirty: false, keepTouched: false }
            );
        } else {
            reset(
                {
                    ...common,
                    has_diagnosis: "unknown",
                    treatment_stage: "unknown",
                    wants_explanation: "simple",
                    symptoms: "",
                    // doctor-поля не задаём
                },
                { keepDirty: false, keepTouched: false }
            );
        }
    }, [userStore.role, userStore.name, reset]);

  const onSubmit: SubmitHandler<SurveyValues> = async (values) => {
    try {
      const res = await fetch(
        `${import.meta.env.VITE_API_BASE_URL}/api/surveys`,
        {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify(values),
        }
      );

      const data = await res.json();

      if (!res.ok) {
        throw new Error(data?.error || 'Ошибка отправки');
      }

      message.success('Опрос успешно отправлен');
      console.log('saved id:', data.id);

      reset({ ...values, name: '', email: '' });

    } catch (err: any) {
      console.error(err);
      message.error(err.message || 'Ошибка сервера');
    }
  };

  return (
    <Card style={{ width: '100%' }}>
      <Title level={3} style={{ marginTop: 0 }}>
        Опрос для участников
      </Title>

      <Text type="secondary">
        Данный опрос поможет стать нам лучше!
      </Text>

      <Divider />

      <Alert type="info" showIcon title="Заметка" description={hint} style={{ marginBottom: 16 }} />

      <Form layout="vertical" onFinish={handleSubmit(onSubmit)}>
        {/* Общие поля */}
        <Form.Item
          label="Имя"
          validateStatus={errors.name ? 'error' : undefined}
          help={errors.name?.message}
        >
          <Controller
            control={control}
            name="name"
            disabled
            defaultValue={userStore.name}
            render={({ field }) => <Input {...field} placeholder="Иван Иванов" disabled />}
          />
        </Form.Item>

        <Form.Item
          label="Email"
          validateStatus={errors.email ? 'error' : undefined}
          help={errors.email?.message}
        >
          <Controller
            control={control}
            name="email"
            render={({ field }) => <Input {...field} placeholder="name@example.com" />}
          />
        </Form.Item>

        <Divider style={{ margin: '12px 0 16px' }} />

        {/* Роль-специфичные блоки */}
        {role === 'doctor' ? (
          <>
            <Title level={5} style={{ margin: 0 }}>
              Блок врача
            </Title>
            <Text type="secondary">Эти поля помогают адаптировать формат ответов и UX под врача.</Text>

            <div style={{ height: 12 }} />

            <Form.Item
              label="Специальность"
              validateStatus={errors.specialty ? 'error' : undefined}
              help={errors.specialty?.message}
            >
              <Controller
                control={control}
                name="specialty"
                render={({ field }) => (
                  <Input {...field} placeholder="Онколог / Хирург / Терапевт / ..." />
                )}
              />
            </Form.Item>

            <Form.Item
              label="Место работы (опционально)"
              validateStatus={errors.workplace ? 'error' : undefined}
              help={errors.workplace?.message}
            >
              <Controller
                control={control}
                name="workplace"
                render={({ field }) => <Input {...field} placeholder="Клиника / отделение" />}
              />
            </Form.Item>

            <Form.Item
              label="Стаж (лет)"
              validateStatus={errors.experience_years ? 'error' : undefined}
              help={errors.experience_years?.message as any}
            >
              <Controller
                control={control}
                name="experience_years"
                render={({ field }) => (
                  <InputNumber
                    style={{ width: '100%' }}
                    min={0}
                    max={60}
                    placeholder="Напр.: 7"
                    value={field.value}
                    onChange={(v) => field.onChange(v ?? undefined)}
                  />
                )}
              />
            </Form.Item>

            <Form.Item
              label="Как часто используете клинические рекомендации?"
              validateStatus={errors.uses_guidelines ? 'error' : undefined}
              help={errors.uses_guidelines?.message}
            >
              <Controller
                control={control}
                name="uses_guidelines"
                render={({ field }) => (
                  <Select
                    value={field.value}
                    onChange={field.onChange}
                    onBlur={field.onBlur}
                    options={usesGuidelinesOptions as any}
                    placeholder="Выберите"
                    style={{ width: '100%' }}
                  />
                )}
              />
            </Form.Item>

            <Form.Item
              label="Главная боль/ожидание от ассистента"
              validateStatus={errors.main_pain ? 'error' : undefined}
              help={errors.main_pain?.message}
            >
              <Controller
                control={control}
                name="main_pain"
                render={({ field }) => (
                  <Input.TextArea
                    {...field}
                    autoSize={{ minRows: 3, maxRows: 8 }}
                    placeholder="Напр.: быстрее проверять назначения по Минздраву, получать объяснение и ссылки на раздел..."
                  />
                )}
              />
            </Form.Item>
          </>
        ) : (
          <>
            <Title level={5} style={{ margin: 0 }}>
              Блок пациента
            </Title>
            <Text type="secondary">Помогает подобрать тональность и полезность ответа для пациента.</Text>

            <div style={{ height: 12 }} />

            <Form.Item
              label="Есть подтверждённый диагноз?"
              validateStatus={errors.has_diagnosis ? 'error' : undefined}
              help={errors.has_diagnosis?.message}
            >
              <Controller
                control={control}
                name="has_diagnosis"
                render={({ field }) => (
                  <Select
                    value={field.value}
                    onChange={field.onChange}
                    onBlur={field.onBlur}
                    options={hasDiagnosisOptions as any}
                    placeholder="Выберите"
                    style={{ width: '100%' }}
                  />
                )}
              />
            </Form.Item>

            <Form.Item
              label="На каком этапе лечение?"
              validateStatus={errors.treatment_stage ? 'error' : undefined}
              help={errors.treatment_stage?.message}
            >
              <Controller
                control={control}
                name="treatment_stage"
                render={({ field }) => (
                  <Select
                    value={field.value}
                    onChange={field.onChange}
                    onBlur={field.onBlur}
                    options={treatmentStageOptions as any}
                    placeholder="Выберите"
                    style={{ width: '100%' }}
                  />
                )}
              />
            </Form.Item>

            <Form.Item
              label="Какой формат объяснения вам удобнее?"
              validateStatus={errors.wants_explanation ? 'error' : undefined}
              help={errors.wants_explanation?.message}
            >
              <Controller
                control={control}
                name="wants_explanation"
                render={({ field }) => (
                  <Radio.Group
                    value={field.value}
                    onChange={(e) => field.onChange(e.target.value)}
                    onBlur={field.onBlur}
                    optionType="button"
                    buttonStyle="solid"
                    options={wantsExplanationOptions as any}
                  />
                )}
              />
            </Form.Item>

            <Form.Item
              label="Кратко: что беспокоит / что назначили"
              validateStatus={errors.symptoms ? 'error' : undefined}
              help={errors.symptoms?.message}
            >
              <Controller
                control={control}
                name="symptoms"
                render={({ field }) => (
                  <Input.TextArea
                    {...field}
                    autoSize={{ minRows: 3, maxRows: 10 }}
                    placeholder="Симптомы, анализы если есть, что назначили, что хотите проверить..."
                  />
                )}
              />
            </Form.Item>
          </>
        )}

        <Divider style={{ margin: '12px 0 16px' }} />

        <Space>
          <Button type="primary" htmlType="submit" loading={isSubmitting}>
            Отправить
          </Button>
          <Button htmlType="button" onClick={() => reset()}>
            Сброс
          </Button>
        </Space>
      </Form>
    </Card>
  );
})
