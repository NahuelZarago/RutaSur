# Decisiones de diseño y supuestos

Este documento reúne lo que conviene poder justificar en la defensa: las decisiones pedidas por el enunciado, cómo quedaron implementadas y los puntos donde el enunciado no fijaba una regla y hubo que tomar una decisión.

## Decisiones del enunciado y dónde están en el código

| Decisión | Elección | Implementación |
| --- | --- | --- |
| Garantía de entrega | Al menos una vez, con consumidores idempotentes | `shared/consumidor.js`: cada `eventId` aplicado se registra en `eventos_procesados` (índice único `consumidor + eventId`, TTL 7 días) |
| Confirmación de offsets | Manual, después de escribir en MongoDB | `autoCommit: false` y `commitOffsets(offset + 1)` al final de cada mensaje |
| Particiones de `envios.estados` | 3 | `shared/eventos.js` (`DEFINICION_TOPICS`); los demás topics también tienen 3 para quedar co-particionados por `shipmentId` |
| Formato de eventos | JSON con validación en cada consumidor | `shared/esquemas.js`; lo inválido va a `envios.estados.dlq` con el motivo |
| Monitor de SLA | Consumidor propio, estado en memoria con respaldo en MongoDB | `services/monitor-sla`, reglas en `shared/dominio/sla.js` |
| Kafka en local | Un broker en modo KRaft | `docker-compose.yml`, imagen `apache/kafka:3.8.0` |
| Tiempo real en la interfaz | Change streams + SSE | `web/app/api/stream/route.js` |

## Cómo se logra "al menos una vez" sin efectos duplicados

Hay dos tipos de consumidores y cada uno resuelve la idempotencia de forma distinta:

**Los que escriben en MongoDB** (proyector, analítica, tiempos de hub) usan una transacción que incluye, a la vez, la verificación en `eventos_procesados`, los efectos (reemplazo del documento, `$inc` de métricas) y la marca de procesado. Si el proceso cae antes de confirmar el offset, el evento vuelve a llegar, se encuentra la marca y no se aplica dos veces. Sin la transacción, un `$inc` repetido sumaría dos veces.

**Los que publican en Kafka** (notificaciones, reprogramación, monitor de SLA) generan eventos con `eventId` determinista derivado de la causa: `notif:<eventId del fallo>`, `reprog:<eventId del fallo>`, `alerta:<envío>:<regla>`. Si caen entre publicar y marcar, al reprocesar publican el mismo `eventId` y el proyector lo descarta. Es *exactamente una vez* en el efecto observable, sin transacciones de Kafka.

## Orden y duplicados (CU1)

- La clave de partición es siempre el `shipmentId`: todos los eventos de un envío caen en la misma partición y se leen en orden.
- Regla de orden: un evento con `secuencia` menor o igual a la última aplicada se descarta (`aplicarEstado` en `shared/dominio/proyeccion.js`). El estado nunca retrocede y el historial no se repite. El documento cuenta los descartes en `descartadosFueraDeOrden`.
- Regla de duplicados: además de la secuencia, el ejecutor reconoce el `eventId` ya registrado.

## Reconstrucción de las vistas

El proyector consume cinco topics. Un mismo documento de `envios` recibe cambios de varios de ellos, y al reprocesar desde el inicio Kafka no garantiza en qué orden se intercalan topics distintos. Para que "borrar y reprocesar" dé siempre el mismo resultado:

- cada topic escribe campos disjuntos del documento (estados → estado e historial; promesas → `promesa`; reprogramaciones → `reprogramaciones`; alertas → `alertas`; notificaciones → `notificaciones`);
- las combinaciones son conmutativas: la promesa se queda con la versión más alta, las reprogramaciones se ordenan por intento, `estadoVisible` se recalcula siempre a partir de los campos;
- las vistas no guardan nada que dependa del momento del procesamiento (como `new Date()`), solo datos de los eventos y el timestamp de Kafka, que es parte del log.

El test `reconstrucción` de `tests/tuberia.test.js` reprocesa con dos intercalados distintos y compara las tres vistas campo por campo; `scripts/reconstruir.js` hace lo mismo sobre el entorno real con un hash SHA-256.

## Tiempo de evento y reloj simulado (CU3)

El monitor nunca usa el reloj de la máquina. Su "ahora" es la marca de agua de tiempo de evento: el máximo `ocurridoEn` observado en `envios.estados` y `envios.promesas`, persistido en `monitor_sla_meta`. Consecuencias:

- el factor de aceleración afecta a todo el sistema por igual;
- si se pausa el simulador, el monitor también se detiene: no aparecen alertas espurias;
- tras un reinicio, el monitor retoma con la misma marca de agua.

Cada evento muestra dos tiempos: `ocurridoEn` (lo fija el origen) y el timestamp de Kafka (cuándo llegó). El simulador agrega el header `x-reloj-simulado` con la hora simulada de publicación; cuando un repartidor "estuvo sin señal", el detalle del envío muestra la demora de sincronización.

La evaluación ocurre al recibir cada evento y en una pasada periódica cada segundo real (2 minutos simulados) sobre los envíos abiertos. Se usa la ausencia de eventos: la alerta *Sin salida* aparece porque pasa el tiempo y no llega `EN_REPARTO`.

## Analítica incremental (CU4)

`metricas_diarias` tiene una fila por `(fecha, zona, franja)` y se actualiza solo con `$inc`. El tablero agrega esas filas con pipelines de agregación; nunca recorre los envíos.

El cumplimiento de SLA necesita cruzar cada entrega con su promesa, que viaja en otro topic. El servicio guarda un join simétrico en `analitica_promesas`: cuenta la entrega cuando conoce ambos lados, llegue primero el que llegue. El cuarto indicador usa la misma técnica para cruzar `INGRESADO_HUB` con `EN_REPARTO`.

El criterio "los indicadores coinciden con un recuento manual" se verifica con `shared/dominio/recuento.js`, un segundo camino de cálculo hecho desde los eventos crudos sin `$inc` ni joins incrementales.

## Supuestos y desvíos respecto del enunciado

| Tema | Enunciado | Decisión tomada |
| --- | --- | --- |
| `ZONA_INACCESIBLE` | No tiene regla de reprogramación | Se retiene en el hub hasta que la zona vuelva a ser accesible, igual que una dirección incorrecta. Es coherente con "los demás motivos lo sacan del circuito" |
| Promesas en el proyector | El catálogo dice que `envios.promesas` lo consume solo el monitor | El proyector también lo lee: la colección `envios` debe contener la promesa e indexar `promesa.venceEn`, y el tracking muestra la fecha estimada |
| Promesas en la analítica | Ídem | La analítica también lo lee: el cumplimiento de SLA es imposible de calcular sin la promesa |
| Notificaciones por alertas | "Avisos ante fallos y alertas" | Se notifica al destinatario cuando se abre una alerta **alta o crítica**. Una alerta media es interna: todavía hay margen para rescatar el envío sin alarmar al cliente |
| Alertas de envíos fuera de circuito | No especificado | Un envío retenido o en devolución no genera *Sin ingreso*, *Sin salida* ni *Vencido*: ya está en un proceso de excepción. Sí genera *Fallo en el día límite*, que describe el propio fallo |
| Escalamiento de alertas | No especificado | Cada regla es independiente: un envío puede tener *Sin salida* (alta) y luego *Vencido* (crítica). La cola ordena por prioridad. Todas se cierran al entregar |
| Franjas horarias | "Franja opuesta" | Dos franjas: mañana (8 a 13 h) y tarde (13 a 19 h). La franja vespertina es justamente la mejora que se propone en el paso 7 |
| Promesa de entrega | No especificada | Creado antes de las 14 h: día hábil siguiente a las 20 h; después de las 14 h, el subsiguiente. No se consideran feriados |
| Escenario "tercer fallo" | — | Los reintentos se aceleran a 45 minutos simulados para que la demo no dure dos días simulados. El servicio de reprogramación igual decide y publica "día hábil siguiente": la regla se ve aplicada aunque el simulador adelante la visita |
| Reloj del monitor | "Usar el reloj simulado que viaja en los eventos" | Marca de agua de tiempo de evento, sin extrapolar: si el simulador se pausa, el tiempo del sistema se detiene |
| Historia inicial | — | El simulador genera 48 horas simuladas al arrancar por primera vez (`SIM_PRECARGA_HORAS`), así la analítica y el reprocesamiento tienen datos para mostrar |
| Lag en el panel de demo | — | Lo consulta la API del simulador con el cliente de administración de Kafka (no consume mensajes). La interfaz sigue sin hablar con Kafka |

## Alternativas descartadas

- **Exactamente una vez con transacciones de Kafka:** obliga a que productor y consumidor compartan transacción y no cubre la escritura en MongoDB. La combinación "al menos una vez + idempotencia" es más simple y cubre todo el camino.
- **Confirmación automática de offsets:** kafkajs confirmaría offsets en segundo plano, aunque el evento no se haya escrito; un corte a mitad de camino perdería eventos.
- **Una sola partición:** no permitiría mostrar reparto de carga ni rebalanceo.
- **Avro con Schema Registry, Kafka Streams o ksqlDB:** quedan como extensiones; la lógica escrita a mano se puede explicar línea por línea.
- **Consumir Kafka desde Next.js:** acoplaría la interfaz al broker. Con change streams, la interfaz depende solo de las vistas.
- **WebSocket:** el flujo es de un solo sentido (servidor a navegador); SSE no requiere infraestructura adicional y el navegador reconecta solo.
