import dotenv from "dotenv";

dotenv.config();

function requireEnv(name: string): string {
  const value = process.env[name];

  if (!value) {
    throw new Error(`Missing env variable: ${name}`);
  }

  return value;
}

const port = Number(process.env.PORT ?? 3000);

if (Number.isNaN(port)) {
  throw new Error("PORT must be a number");
}

const postgresUser = requireEnv("POSTGRES_USER");
const postgresPassword = requireEnv("POSTGRES_PASSWORD");
const postgresDb = requireEnv("POSTGRES_DB");
const postgresHost = requireEnv("POSTGRES_HOST");
const postgresPort = requireEnv("POSTGRES_PORT");
const nodeEnv = requireEnv("NODE_ENV")

const fastapiUrl = requireEnv("FASTAPI_URL")
const fastapiCheckPath = requireEnv("FASTAPI_CHECK_PATH")

export const env = {
  port,
  nodeEnv,
  postgresUser,
  postgresPassword,
  postgresDb,
  postgresHost,
  postgresPort,

  fastapiUrl,
  fastapiCheckPath,

  databaseUrl: `postgres://${postgresUser}:${postgresPassword}@${postgresHost}:${postgresPort}/${postgresDb}`,
} as const;

export type Env = typeof env;
