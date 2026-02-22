import { Layout, Menu, Button, Space, Typography } from "antd";

const { Header } = Layout;
const { Text } = Typography;

export function AppHeader() {
  return (
    <Header
      style={{
        position: "sticky",
        top: 0,
        zIndex: 10,
        display: "flex",
        alignItems: "center",
        gap: 16,
        padding: "0 16px",
        width: "100%",
      }}
    >
      {/* Лого / название */}
      <Space align="center" size={8}>
        <div
          style={{
            width: 28,
            height: 28,
            borderRadius: 8,
            background: "rgba(255,255,255,0.25)",
          }}
        />
        <Text style={{ color: "#fff" }} strong>
          MyApp
        </Text>
      </Space>

      {/* Меню */}
      <Menu
        theme="dark"
        mode="horizontal"
        selectable={false}
        items={[
          { key: "home", label: "Главная" },
          { key: "projects", label: "Проекты" },
          { key: "about", label: "О нас" },
        ]}
        style={{ flex: 1, minWidth: 0 }}
      />

      {/* Правый блок */}
      <Space>
        <Button type="primary">Войти</Button>
      </Space>
    </Header>
  );
}
