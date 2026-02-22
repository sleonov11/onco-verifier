import { useMemo, useState } from 'react';
import { ConfigProvider, Layout, Segmented, Space, Typography } from 'antd';
import ruRU from 'antd/locale/ru_RU';

import { AiChatPage } from './pages/AiChatPage';
import { SurveyPage } from './pages/SurveyPage';

const { Header, Content, Footer } = Layout;
const { Text } = Typography;

type PageKey = 'ai' | 'survey';

export default function App() {
  const [page, setPage] = useState<PageKey>('ai');

  const pageNode = useMemo(() => {
    if (page === 'survey') return <SurveyPage />;
    return <AiChatPage />;
  }, [page]);

  return (
    <ConfigProvider locale={ruRU}>
      <Layout style={{ minHeight: '100vh' }}>
        <Header style={{ display: 'flex', alignItems: 'center' }}>
          <Space style={{ width: '100%', justifyContent: 'space-between' }}>
            <Text style={{ color: 'white', fontWeight: 600 }}>
              AI Project
            </Text>

            <Segmented
              value={page}
              onChange={(v) => setPage(v as PageKey)}
              options={[
                { label: 'Основное', value: 'ai' },
                { label: 'Опрос', value: 'survey' },
              ]}
            />
          </Space>
        </Header>

        <Content style={{ padding: 16 }}>
          <div style={{ maxWidth: 980, margin: '0 auto' }}>
            {pageNode}
          </div>
        </Content>

        <Footer style={{ textAlign: 'center' }}>
          <Text type="secondary">Made by team "Russian Legend"</Text>
        </Footer>
      </Layout>
    </ConfigProvider>
  );
}
