# Imagen única para el simulador, los cinco consumidores, kafka-init y las herramientas.
# Cada contenedor la usa con un comando distinto (ver docker-compose.yml).
FROM node:22-alpine

ENV NODE_ENV=production
WORKDIR /app

COPY package.json ./
RUN npm install --omit=dev --no-audit --no-fund

COPY shared ./shared
COPY services ./services
COPY scripts ./scripts

CMD ["node", "scripts/lag.js"]
