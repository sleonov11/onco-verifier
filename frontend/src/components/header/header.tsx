import { Layout, Space, Typography, Segmented, Tag, Button } from "antd";
import { observer } from "mobx-react-lite";
import { useNavigate } from "react-router-dom";

import { userStore } from "../../stores/user.ts";

const { Header } = Layout;
const { Text } = Typography;

type PageKey = "ai" | "survey";

export const AppHeader = observer(
    ({
         page,
     }: {
        page: PageKey;
    }) => {
        const navigate = useNavigate();

        const handleLogout = () => {
            userStore.reset();
            navigate("/auth");
        };

        const roleLabel =
            userStore.role === "doctor" ? "Врач" : "Пациент";

        const roleColor =
            userStore.role === "doctor" ? "blue" : "green";

        return (
            <Header style={{ display: "flex", alignItems: "center" }}>
                <Space
                    style={{
                        width: "100%",
                        justifyContent: "space-between",
                    }}
                >
                    {/* Левый блок */}
                    <Text style={{ color: "white", fontWeight: 600 }}>
                        AI Project
                    </Text>

                    {/* Центр */}
                    <Segmented
                        value={page}
                        onChange={(v) =>
                            navigate(v === "survey" ? "/survey" : "/")
                        }
                        options={[
                            { label: "Основное", value: "ai" },
                            { label: "Опрос", value: "survey" },
                        ]}
                    />

                    {/* Правый блок */}
                    {userStore.name && userStore.role ? (
                        <Space>
                            <Tag color={roleColor}>{roleLabel}</Tag>

                            <Text style={{ color: "white", marginRight: 100 }}>
                                {userStore.name}
                            </Text>

                            <Button size="small" danger onClick={handleLogout}>
                                Выйти
                            </Button>
                        </Space>
                    ) : (
                        <Button
                            size="small"
                            type="primary"
                            onClick={() => navigate("/auth")}
                        >
                            Войти
                        </Button>
                    )}
                </Space>
            </Header>
        );
    }
);