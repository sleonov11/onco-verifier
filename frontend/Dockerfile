# ====== Build stage ======
FROM node:22.12-alpine AS build

WORKDIR /app

# Сначала зависимости (кешируется лучше)
COPY package*.json ./
RUN npm ci

# Потом исходники
COPY . .

# Важно: Vite кладёт сборку в /dist
RUN npm run build


# ====== Run stage (nginx) ======
FROM nginx:1.27-alpine

# Удалим дефолтный конфиг и добавим свой (чтобы SPA роутинг работал)
RUN rm /etc/nginx/conf.d/default.conf
COPY nginx.conf /etc/nginx/conf.d/default.conf

# Скопируем статику
COPY --from=build /app/dist /usr/share/nginx/html

EXPOSE 80

# Healthcheck (опционально)
HEALTHCHECK --interval=10s --timeout=3s --retries=5 CMD wget -qO- http://localhost/ || exit 1
