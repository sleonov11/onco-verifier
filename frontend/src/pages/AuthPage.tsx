import { Button, Card, Form, Input, Radio, Space, Typography, message } from "antd";
import { useNavigate } from "react-router-dom";
import { observer } from "mobx-react-lite";
import { Controller, useForm } from "react-hook-form";
import { z } from "zod";
import { zodResolver } from "@hookform/resolvers/zod";

import { userStore } from "../stores/user.ts";

import emiasLogo from "../assets/Emias Logo.png";
import maxLogo from "../assets/Max_logo_2025.png";

const { Title, Text } = Typography;

const schema = z.object({
    name: z.string().trim().min(5, "Укажите ФИО полностью"),
    role: z.enum(["doctor", "patient"]),
    password: z.string().trim().min(6, "Пароль должен быть длиннее 6 символов")
});

type FormValues = z.infer<typeof schema>;

export const AuthPage = observer(() => {
    const navigate = useNavigate();

    const {
        control,
        handleSubmit,
        formState: { errors, isSubmitting },
    } = useForm<FormValues>({
        resolver: zodResolver(schema),
        defaultValues: {
            name: userStore.name || "",
            role: userStore.role || "doctor",
            password: "",
        },
    });

    const onSubmit = (data: FormValues) => {
        userStore.setName(data.name);
        userStore.setRole(data.role);

        message.success("Авторизация успешна");

        // редирект на главную
        navigate("/");
    };

    return (
        <Space
            align="center"
            style={{ width: "100%", minHeight: "100vh", justifyContent: "center" }}
        >
            <Card style={{ width: 420 }}>
                <Title level={3} style={{ marginTop: 0 }}>
                    Вход в систему
                </Title>

                <Text type="secondary">
                    Укажите ФИО и выберите роль для входа.
                </Text>

                <Form layout="vertical" onFinish={handleSubmit(onSubmit)} style={{ marginTop: 24 }}>
                    <Form.Item
                        label="ФИО"
                        validateStatus={errors.name ? "error" : undefined}
                        help={errors.name?.message}
                    >
                        <Controller
                            control={control}
                            name="name"
                            render={({ field }) => (
                                <Input {...field} placeholder="Иванов Иван Иванович" />
                            )}
                        />
                    </Form.Item>

                    <Form.Item label="Роль">
                        <Controller
                            control={control}
                            name="role"
                            render={({ field }) => (
                                <Radio.Group
                                    {...field}
                                    optionType="button"
                                    buttonStyle="solid"
                                    style={{ width: "100%" }}
                                >
                                    <Radio.Button value="doctor" style={{ width: "50%", textAlign: "center" }}>
                                        Врач
                                    </Radio.Button>
                                    <Radio.Button value="patient" style={{ width: "50%", textAlign: "center" }}>
                                        Пациент
                                    </Radio.Button>
                                </Radio.Group>
                            )}
                        />
                    </Form.Item>

                    <Form.Item
                        label="Пароль"
                        validateStatus={errors.password ? "error" : undefined}
                        help={errors.password?.message}
                    >
                        <Controller
                            control={control}
                            name="password"
                            render={({ field }) => (
                                <Input.Password {...field} placeholder="*****" />
                            )}
                        />
                    </Form.Item>

                    <Button
                        type="primary"
                        htmlType="submit"
                        block
                        loading={isSubmitting}
                        style={{ marginTop: 12 }}
                    >
                        Войти
                    </Button>

                    <div
                        style={{
                            marginTop: 24,
                            display: "flex",
                            justifyContent: "center",
                            alignItems: "center",
                            gap: 24,
                            opacity: 0.8,
                        }}
                    >
                        <img src={maxLogo} alt="MAX" style={{ height: 32 }} />
                        <img src={emiasLogo} alt="EMIAS" style={{ height: 32 }} />
                    </div>
                </Form>
            </Card>
        </Space>
    );
});