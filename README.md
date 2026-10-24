# RutaSur — seguimiento de envíos orientado a eventos

Trabajo práctico de Programación Avanzada (Licenciatura en Sistemas de Información, 2026).

Plataforma de seguimiento en tiempo real para un operador de última milla ficticio. Cada cambio de estado de un envío es un evento en Kafka; cinco servicios independientes reaccionan a esos eventos y una interfaz en Next.js muestra la operación en vivo.

**Stack:** Apache Kafka 3.8 (KRaft, un broker) · Node.js 22 (kafkajs) · MongoDB 7 (replica set de un nodo) · Next.js 14 · Docker Compose.

---

## Arranque

Requisito: Docker Desktop (o Docker Engine con Compose v2). Nada más: no hace falta Node instalado.

```bash
docker compose up -d --build
```

La primera vez tarda unos minutos en construir las imágenes. Después:

| Qué | Dónde |
| --- | --- |
| Torre de control | http://localhost:3000 |
| Analítica | http://localhost:3000/analitica |
| Panel de demo | http://localhost:3000/demo |
| Tracking público | http://localhost:3000/tracking |
| API del simulador | http://localhost:4000/estado |
| Kafka desde la máquina | `localhost:29092` |
| MongoDB (Compass) | `mongodb://localhost:27017/?directConnection=true` |

Al arrancar por primera vez, el simulador genera 48 horas simuladas de historia (unos 480 envíos) y después sigue en tiempo real a 20 envíos por minuto, con 1 hora simulada = 30 segundos. Por eso los tableros tienen datos desde el primer minuto.

Para empezar de cero: `docker compose down` y de nuevo `docker compose up -d`. Kafka, MongoDB y el reloj del simulador viven dentro de sus contenedores, así que se descartan juntos y quedan siempre consistentes entre sí.

---

## Arquitectura

```
                 ┌──────────────────────── Kafka (único punto de contacto) ────────────────────────┐
 Simulador ────► │ envios.estados (3 part.)   envios.promesas (compactado)                          │
                 │ envios.reprogramaciones    alertas.sla    notificaciones.enviadas    *.dlq       │
                 └───┬──────────────┬──────────────┬──────────────┬──────────────┬─────────────────┘
                     │              │              │              │              │
                 proyector    notificaciones  reprogramación  monitor-sla     analítica
                     │              └──────► Kafka ◄─────┘          │              │
                     ▼                                       (alertas.sla)         ▼
              envios · alertas · notificaciones                              metricas_diarias
                     │                MongoDB (vistas derivadas)                   │
                     └──────── change streams ──► Next.js (SSE) ◄──────────────────┘
```

Ningún servicio llama a otro: se comunican solo por Kafka. MongoDB guarda vistas derivadas que se pueden borrar y reconstruir desde el log. Next.js solo lee MongoDB.

| Componente | Grupo de consumidores | Lee | Publica / escribe |
| --- | --- | --- | --- |
| Simulador | — | — | `envios.estados`, `envios.promesas` |
| Proyector | `proyector` | los cinco topics de negocio | `envios`, `alertas`, `notificaciones` |
| Notificaciones | `notificaciones` | `envios.estados`, `alertas.sla` | `notificaciones.enviadas` |
| Reprogramación | `reprogramacion` | `envios.estados` | `envios.reprogramaciones` |
| Monitor de SLA | `monitor-sla` | `envios.estados`, `envios.promesas` | `alertas.sla` |
| Analítica | `analitica` | `envios.estados`, `envios.promesas` | `metricas_diarias` |
| Tiempos de hub (4.º indicador) | `analitica-tiempos-hub` | `envios.estados` | `metricas_tiempo_hub` |

### Estructura del repositorio

```
shared/              Código común
  dominio/           Reglas de negocio puras (sin Kafka ni Mongo): proyección, reprogramación,
                     notificaciones, SLA, analítica y recuento manual
  consumidor.js      Ejecutor genérico: validación + DLQ, idempotencia, offsets manuales, logs
  esquemas.js        Validación de esquema de cada topic
  eventos.js         Catálogo de topics, tipos, grupos y suscripciones
services/
  simulador/         Motor determinista con semilla, escenarios y API HTTP
  proyector/  notificaciones/  reprogramacion/  monitor-sla/  analitica/
scripts/             Topics, lag, DLQ, verificación de analítica, reconstrucción
web/                 Next.js (App Router): torre, detalle, tracking, analítica, demo, SSE
tests/               Pruebas de dominio y de la tubería completa en memoria
docs/                Decisiones de diseño, supuestos y criterios de aceptación
```

---

## Guion de la demo (≈ 15 minutos)

Antes de empezar, conviene abrir en pestañas: la torre de control, el panel de demo y una terminal en la carpeta del proyecto.

1. **Arranque.** `docker compose up -d`. La torre muestra los contadores del recorrido moviéndose y el feed de eventos. Arriba a la derecha se ve la latencia evento → pantalla (debe estar por debajo de 2 s).
2. **Envío feliz.** En el panel de demo, *Envío feliz*. Abrí el enlace *Tracking público* y seguilo hasta *Entregado* (≈ 45 s). *Concepto: productor, consumidor y proyección.*
3. **Ausente.** *Ausente*. En el detalle del envío aparecen casi a la vez la entrega fallida, el aviso al destinatario y la reprogramación al día hábil siguiente en la franja opuesta; en analítica sube el contador de fallos. *Concepto: tres grupos de consumidores sobre un mismo evento.*
4. **Demorado.** *Demorado en hub*. El paquete queda detenido y, a los ~3 minutos reales, aparece en la cola una alerta **alta** de *Sin salida a reparto*, antes del vencimiento. Después sale y, al entregarse, la alerta se cierra sola. *Concepto: procesamiento con estado y tiempo.*
   > Sugerencia: dispará este escenario justo después del paso 2 y volvé a mirarlo en este punto.
5. **Caída de un servicio.**
   ```bash
   docker compose stop notificaciones
   ```
   En el panel de demo el grupo `notificaciones` figura *Detenido* y su lag crece; tracking y alertas siguen funcionando. Luego:
   ```bash
   docker compose start notificaciones
   ```
   El lag vuelve a cero y no se duplica ningún aviso (cada notificación tiene un `eventId` determinista). *Concepto: aislamiento, offsets e idempotencia.*
6. **Evento inválido.** *Evento malformado*. El contador de DLQ del panel sube y el flujo sigue. Para ver el motivo:
   ```bash
   docker compose run --rm herramientas node scripts/dlq.js
   ```
   *Concepto: manejo de errores con DLQ.*
7. **Analítica.** Abrí `/analitica`. El recuadro superior señala la zona y franja con más fallos en el primer intento (con la semilla por defecto, *Las Lomas, mañana*, con mayoría de ausencias) y propone la mejora: ofrecer una franja vespertina en esa zona.
8. **Reprocesamiento.**
   ```bash
   docker compose --profile replay up -d analitica-tiempos
   ```
   Un grupo nuevo lee `envios.estados` desde el inicio y, al recargar la analítica, el cuarto indicador (tiempo medio en hub) aparece completo, incluidos los días anteriores a su creación. No se reinició ningún otro servicio. *Concepto: retención y replay.*

Los pasos 5 y 8 son los que distinguen esta solución de una cola tradicional o de llamadas REST encadenadas: el log conserva los eventos y cada grupo avanza con sus propios offsets.

---

## Verificación de los criterios de aceptación

`docs/criterios.md` detalla cómo se comprueba cada criterio. En resumen:

**Pruebas automáticas (sin infraestructura).** Recorren la tubería completa en memoria con las mismas funciones de dominio que usan los servicios, incluyendo duplicados, eventos fuera de orden, mensajes malformados y demoras de sincronización:

```bash
npm test                                   # con Node 20+ instalado (no requiere npm install)
docker run --rm -v "$PWD":/w -w /w node:22-alpine node --test "tests/**/*.test.js"   # sin Node
```

**Sobre el entorno levantado:**

```bash
# Lag de cada grupo y mensajes en la DLQ
docker compose run --rm herramientas node scripts/lag.js

# Los tres indicadores coinciden con un recuento manual hecho desde los eventos crudos
docker compose run --rm herramientas node scripts/verificar-analitica.js

# Borrar las vistas y reprocesar Kafka deja exactamente el mismo resultado (hash SHA-256)
bash scripts/reconstruccion.sh          # Linux / macOS / Git Bash
./scripts/reconstruccion.ps1            # Windows PowerShell
```

Para una corrida reproducible con semilla fija (criterio de analítica), creá un `.env` a partir de `.env.example` con, por ejemplo, `SIM_SEMILLA=7` y `SIM_MAX_ENVIOS=600`, y levantá el entorno desde cero.

---

## Observabilidad

- Cada servicio escribe una línea JSON por evento con `eventId`, `topic`, `particion`, `offset` y el resultado (`aplicado`, `duplicado`, `descartado-fuera-de-orden`, …):
  ```bash
  docker compose logs -f proyector
  docker compose logs proyector | grep RS-XXXX-E001
  ```
- Lag por grupo: panel de demo, `scripts/lag.js`, o Kafka UI con `docker compose --profile herramientas up -d kafka-ui` (http://localhost:8080).

## Parámetros del simulador

Se configuran por variables de entorno (`.env`) o en vivo con `POST /parametros` en la API del simulador.

| Parámetro | Valor por defecto |
| --- | --- |
| Envíos por minuto (real) | 20 |
| Factor de aceleración | 120 (1 h simulada = 30 s) |
| Tasa de fallo por zona | entre 5 % y 30 % (`shared/catalogo.json`) |
| Motivos de fallo | 60 % ausente, 20 % dirección, 15 % rechazo, 5 % zona |
| Demora en hub / duplicado / fuera de orden / malformado | 10 % / 2 % / 2 % / 1 % |
| Semilla | 42 |

Escenarios disponibles en el panel de demo: envío feliz, ausente, demorado, tercer fallo, ráfaga (500 envíos en 5 s), y además sin ingreso, vencido, fallo en el día límite, dirección incorrecta, rechazado, zona inaccesible, duplicado, fuera de orden y malformado.

## Documentación

- [`docs/decisiones.md`](docs/decisiones.md): decisiones de diseño, alternativas descartadas y supuestos sobre el enunciado.
- [`docs/criterios.md`](docs/criterios.md): cada criterio de aceptación y cómo se verifica.
