import { createApp } from "./app";
import { env } from "./config/env";
import type { Server } from "http";
import { migrate } from "./db/migrate";
import { pool } from "./db/pool";

async function bootstrap() {
  try {
    // 1️⃣ Ждём миграцию
    await migrate();

    // 2️⃣ Создаём приложение
    const app = createApp();

    // 3️⃣ Запускаем сервер
    const server: Server = app.listen(env.port, () => {
      console.log(
        `Backend started on http://localhost:${env.port} (${env.nodeEnv})`
      );
    });

    // 4️⃣ Graceful shutdown
    function shutdown(signal: string) {
      console.log(`Received ${signal}. Shutting down gracefully...`);

      server.close(async (err?: Error) => {
        if (err) {
          console.error("Error during server shutdown:", err);
          process.exit(1);
        }

        try {
          await pool.end(); // закрываем pg pool
          console.log("Database pool closed.");
          process.exit(0);
        } catch (e) {
          console.error("Error closing DB pool:", e);
          process.exit(1);
        }
      });
    }

    process.on("SIGINT", () => shutdown("SIGINT"));
    process.on("SIGTERM", () => shutdown("SIGTERM"));

  } catch (err) {
    console.error("Failed to bootstrap application:", err);
    process.exit(1);
  }
}

bootstrap();
