import { useMemo } from "react";
import { ConfigProvider, Layout, Typography } from "antd";
import ruRU from "antd/locale/ru_RU";
import { BrowserRouter, Routes, Route, Navigate, useLocation } from "react-router-dom";
import { observer } from "mobx-react-lite";

import { AiChatPage } from "./pages/AiChatPage";
import { SurveyPage } from "./pages/SurveyPage";
import { AuthPage } from "./pages/AuthPage";
import { userStore } from "./stores/user.ts";
import {AppHeader} from "./components/header/header.tsx";

const { Content, Footer } = Layout;
const { Text } = Typography;

type PageKey = "ai" | "survey";

const MainLayout = observer(() => {
    const location = useLocation();

    const page: PageKey =
        location.pathname === "/survey" ? "survey" : "ai";

    const pageNode = useMemo(() => {
        if (page === "survey") return <SurveyPage />;
        return <AiChatPage />;
    }, [page]);

    // 🔐 если нет авторизации — редирект
    if (!userStore.name || !userStore.role) {
        return <Navigate to="/auth" replace />;
    }

    return (
        <Layout style={{ minHeight: "100vh" }}>
            <AppHeader page={page}/>

            <Content style={{ padding: 16 }}>
                <div style={{ maxWidth: 980, margin: "0 auto" }}>
                    {pageNode}
                </div>
            </Content>

            <Footer style={{ textAlign: "center" }}>
                <Text type="secondary">
                    Built by Team "Russian Legend" for Sechenov AI Hackathon 2026
                </Text>
            </Footer>
        </Layout>
    );
});

export default function App() {
    return (
        <BrowserRouter>
            <ConfigProvider locale={ruRU}>
                <Routes>
                    <Route path="/auth" element={<AuthPage />} />
                    <Route path="/survey" element={<MainLayout />} />
                    <Route path="/" element={<MainLayout />} />
                    <Route path="*" element={<Navigate to="/" />} />
                </Routes>
            </ConfigProvider>
        </BrowserRouter>
    );
}