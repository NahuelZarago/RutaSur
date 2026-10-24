# Aclaraciones sobre el trabajo

## Uso de inteligencia artificial

Quiero ser transparente: este trabajo lo hice con mucha ayuda de un asistente de IA (Claude). El TP me resultó difícil, sobre todo la parte de Kafka y del procesamiento orientado a eventos, y me apoyé en la IA para sacarlo adelante.
Concretamente, la IA generó la mayor parte del código (el simulador, los cinco servicios, la interfaz en Next.js y los scripts), la configuración de Docker Compose y la documentación.

Lo que hice yo:
- Levanté el entorno completo en mi máquina con `docker compose up -d --build` y comprobé que todos los servicios arrancaran.
- Recorrí la interfaz y los escenarios del panel de demo.
- [Completar: qué más revisaste, probaste o cambiaste vos. Por ejemplo: "corrí los scripts de verificación", "probé detener el servicio de notificaciones", "modifiqué X".]

## Conceptos clave del diseño
**La clave de partición es el `shipmentId`.** Kafka garantiza el orden solo dentro de una partición, no en todo el topic. Todos los mensajes con la misma clave caen en la misma partición, así que al usar el `shipmentId` como clave, los eventos de un mismo paquete se leen en el orden en que se publicaron. Distintos envíos pueden repartirse entre las tres particiones y procesarse en paralelo. Si la clave fuera otra (o ninguna), un `ENTREGADO` podría procesarse antes que el `EN_REPARTO` del mismo envío. Como el orden por partición no cubre los errores del origen, cada evento lleva además un número de `secuencia`: si llega uno con secuencia menor o igual a la ya aplicada, se descarta y el estado no retrocede.

**Cómo se evita aplicar dos veces un evento duplicado.** El sistema usa entrega "al menos una vez", así que un mismo evento puede llegar más de una vez (por un reintento o porque el simulador lo duplica a propósito). Cada consumidor registra en la colección `eventos_procesados` el `eventId` de cada evento que ya aplicó, con un índice único por consumidor y `eventId`. Si el evento vuelve a llegar, se encuentra la marca y no se aplica. En los servicios que escriben en MongoDB, la marca y el efecto (por ejemplo, un `$inc` en las métricas) se guardan en la misma transacción, así nunca queda uno sin el otro. Los servicios que publican en Kafka (notificaciones, reprogramación, monitor) generan eventos con `eventId` determinista, derivado del evento que los causó: el aviso por el fallo `RS-X-05` siempre es `notif:RS-X-05`. Si el servicio cae y reprocesa, publica el mismo `eventId` y el proyector lo descarta como duplicado.

**Por qué los offsets se confirman a mano, después de escribir en MongoDB.** El offset le indica a Kafka hasta dónde leyó un grupo. Con la confirmación automática, kafkajs confirma en segundo plano cada cierto tiempo, aunque el evento todavía no se haya guardado: si el proceso cae justo entre la confirmación y la escritura, ese evento se pierde. Con la confirmación manual, el offset se confirma recién después de escribir el resultado. Si el proceso cae antes, al reiniciar vuelve a leer desde el último offset confirmado y reprocesa el evento. Puede repetirse, pero eso lo resuelve la idempotencia; lo que nunca pasa es que se pierda.

**Cómo el monitor de SLA detecta una alerta por la ausencia de un evento.** La alerta "sin salida" no la dispara un evento, sino que pase el tiempo sin que llegue `EN_REPARTO`. Por eso el monitor guarda una tabla con el último estado y el vencimiento de cada envío, cruzando `envios.estados` con `envios.promesas`, y evalúa las reglas en dos momentos: al recibir cada evento y en una pasada periódica sobre todos los envíos abiertos. Para medir el tiempo no usa el reloj de la máquina, porque el simulador acelera el tiempo (1 hora simulada = 30 segundos). Usa el reloj simulado: el mayor `ocurridoEn` que vio en los eventos. Así, si faltan menos de 4 horas simuladas para el vencimiento y el envío no salió a reparto, emite la alerta de prioridad alta. Si se pausa el simulador, el tiempo del monitor también se detiene y no aparecen alertas falsas. Cada regla alerta una sola vez por envío, y cuando llega `ENTREGADO` el monitor emite el cierre.

**Cómo un grupo de consumidores nuevo reprocesa el histórico.** Kafka no borra los mensajes cuando alguien los lee: los conserva durante la retención del topic (30 días en `envios.estados`). Cada grupo de consumidores lleva sus propios offsets. Un grupo nuevo, como `analitica-tiempos-hub`, no tiene offsets guardados, así que empieza a leer desde el principio del topic y calcula el cuarto indicador con todo el histórico, incluidos los días anteriores a su creación. Los demás grupos no se enteran: siguen leyendo desde sus propios offsets. Esto es lo que lo diferencia de una cola tradicional, donde un mensaje leído desaparece y no se puede volver a procesar.

Estoy disponible para explicar cualquier parte del código en la defensa.

## Decisiones y supuestos

Donde el enunciado no fijaba una regla se tomó una decisión; están todas justificadas en [`docs/decisiones.md`](docs/decisiones.md). Las principales:

- `ZONA_INACCESIBLE` queda retenido en el hub, porque el enunciado no le asigna una regla de reprogramación.
- El proyector y la analítica también leen `envios.promesas`: sin la promesa no se puede mostrar la fecha estimada ni calcular el cumplimiento de SLA.
- Solo las alertas de prioridad alta o crítica generan un aviso al destinatario.
- En el escenario "tercer fallo" los reintentos están acelerados para que entre en el tiempo de la demo.

## Cómo se verificó

- Pruebas automáticas (`npm test`): 31 pruebas que recorren toda la tubería en memoria, con duplicados, eventos fuera de orden y mensajes malformados.
- Sobre el entorno real: [completar con lo que hayas corrido, por ejemplo `scripts/lag.js`, `scripts/verificar-analitica.js`, `scripts/reconstruccion.ps1`, y si dieron bien.]
