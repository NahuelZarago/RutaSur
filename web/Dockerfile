# Interfaz Next.js. El contexto de build es la raíz del proyecto para poder copiar el catálogo.
FROM node:22-alpine AS build
ENV NEXT_TELEMETRY_DISABLED=1
WORKDIR /app
COPY web/package.json ./
RUN npm install --no-audit --no-fund
COPY web/ ./
COPY shared/catalogo.json ./lib/catalogo.json
RUN npm run build

FROM node:22-alpine
ENV NODE_ENV=production NEXT_TELEMETRY_DISABLED=1
WORKDIR /app
COPY --from=build /app ./
EXPOSE 3000
CMD ["npm", "start"]
