import { observer } from "mobx-react-lite";
import { useEffect, useMemo, useRef, useState } from "react";
import { Button, Card, Input, Space, Typography, Popconfirm, Empty } from "antd";
import { DeleteOutlined, SendOutlined } from "@ant-design/icons";

import { chatStore } from "../../stores/chat.ts";

const { Text } = Typography;

type Props = {
    title?: string;
    placeholder?: string;
    height?: number; // высота области сообщений
    onSend?: (text: string) => void | Promise<void>; // если хочешь отправку наружу (API)
    disabled?: boolean;
};

export const Chat = observer(function Chat({
                                               title = "Чат",
                                               placeholder = "Введите сообщение…",
                                               height = 420,
                                               onSend,
                                               disabled = false,
                                           }: Props) {
    const [value, setValue] = useState("");
    const bottomRef = useRef<HTMLDivElement | null>(null);

    const canSend = useMemo(() => value.trim().length > 0 && !disabled, [value, disabled]);

    const scrollToBottom = (smooth = true) => {
        bottomRef.current?.scrollIntoView({ behavior: smooth ? "smooth" : "auto" });
    };

    useEffect(() => {
        scrollToBottom(false);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    useEffect(() => {
        scrollToBottom(true);
    }, [chatStore.messages.length]);

    const send = async () => {
        const text = value.trim();
        if (!text || disabled) return;

        chatStore.addUserMessage(text);
        setValue("");

        if (onSend) {
            await onSend(text);
        }
    };

    return (
        <Card
            title={title}
            bodyStyle={{ display: "flex", flexDirection: "column", gap: 12 }}
            extra={
                <Popconfirm
                    title="Очистить историю?"
                    okText="Очистить"
                    cancelText="Отмена"
                    onConfirm={() => chatStore.clear()}
                    disabled={disabled || chatStore.messages.length === 0}
                >
                    <Button
                        danger
                        icon={<DeleteOutlined />}
                        disabled={disabled || chatStore.messages.length === 0}
                    >
                        Clear
                    </Button>
                </Popconfirm>
            }
        >
            <div
                style={{
                    height,
                    overflowY: "auto",
                    padding: 12,
                    border: "1px solid rgba(0,0,0,0.08)",
                    borderRadius: 8,
                    background: "#fafafa",
                }}
            >
                {chatStore.messages.length === 0 ? (
                    <Empty description="Пока нет сообщений" />
                ) : (
                    <Space direction="vertical" style={{ width: "100%" }} size={10}>
                        {chatStore.messages.map((m) => {
                            const isUser = m.role === "user";
                            return (
                                <div
                                    key={m.id}
                                    style={{
                                        display: "flex",
                                        justifyContent: isUser ? "flex-end" : "flex-start",
                                    }}
                                >
                                    <div
                                        style={{
                                            maxWidth: "78%",
                                            padding: "10px 12px",
                                            borderRadius: 12,
                                            background: isUser ? "#1677ff" : "#ffffff",
                                            color: isUser ? "#fff" : "rgba(0,0,0,0.88)",
                                            border: isUser ? "none" : "1px solid rgba(0,0,0,0.08)",
                                            boxShadow: "0 1px 2px rgba(0,0,0,0.04)",
                                            whiteSpace: "pre-wrap",
                                            wordBreak: "break-word",
                                        }}
                                    >
                                        <Text style={{ color: "inherit" }}>{m.content}</Text>
                                    </div>
                                </div>
                            );
                        })}
                        <div ref={bottomRef} />
                    </Space>
                )}
            </div>

            <Space.Compact style={{ width: "100%" }}>
                <Input
                    value={value}
                    onChange={(e) => setValue(e.target.value)}
                    placeholder={placeholder}
                    disabled={disabled}
                    onPressEnter={() => {
                        if (canSend) void send();
                    }}
                />
                <Button
                    type="primary"
                    icon={<SendOutlined />}
                    disabled={!canSend}
                    onClick={() => void send()}
                >
                    Send
                </Button>
            </Space.Compact>
        </Card>
    );
});