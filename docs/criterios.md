# Criterios de aceptación y cómo se verifican

Cada criterio tiene una verificación automática en memoria (`npm test`, sin infraestructura) y una verificación sobre el entorno levantado con Docker.

## Ciclo de vida

| Criterio | Prueba automática | Sobre el entorno |
| --- | --- | --- |
| Un envío simulado aparece en la torre y en el tracking público con su estado correcto | `tuberia › ciclo de vida` compara el estado proyectado de 400 envíos con la mayor secuencia del log | Panel de demo → *Envío feliz* → enlaces *Detalle* y *Tracking público* |
| Un evento duplicado no agrega una entrada repetida al historial | `dominio › CU1: duplicado o fuera de orden`, `tuberia › ciclo de vida` (historial sin `eventId` repetidos) | Escenario *Evento duplicado*: el detalle muestra un solo *Ingresó al hub* y el aviso de eventos descartados |
| Un evento fuera de orden no hace retroceder el estado | Ídem; además verifica que las secuencias del historial sean crecientes | Escenario *Evento fuera de orden* |
| Un evento malformado termina en la DLQ con su motivo | `tuberia › DLQ`, `dominio › esquemas`, `simulador › malformado` | Escenario *Evento malformado* y `scripts/dlq.js` |

## Entrega fallida

| Criterio | Prueba automática | Sobre el entorno |
| --- | --- | --- |
| Un `ENTREGA_FALLIDA` genera exactamente una notificación y una reprogramación | `tuberia › entrega fallida` (con duplicados aleatorios en el flujo) | Escenario *Ausente* → detalle del envío |
| Cada motivo aplica su regla de reprogramación | `dominio › CU2: cada motivo aplica su regla` | Escenarios *Ausente*, *Dirección incorrecta*, *Rechazado*, *Zona inaccesible* |
| El tercer fallo marca el envío para devolución | `dominio › CU2`, `simulador › tercer fallo`, `tuberia › entrega fallida` | Escenario *Tercer fallo*: estado *En devolución* |
| Detener un servicio no afecta a los otros dos | Por diseño: cada servicio es un proceso y un grupo distinto | `docker compose stop notificaciones`: reprogramación y analítica siguen; al reiniciar, el lag vuelve a 0 sin avisos duplicados |

## Riesgo de SLA

| Criterio | Prueba automática | Sobre el entorno |
| --- | --- | --- |
| Cada una de las cuatro reglas genera su alerta con la prioridad definida | `dominio › CU3: cada una de las cuatro reglas`, `tuberia › riesgo de SLA` | Escenarios *Sin ingreso* (media), *Demorado* (alta), *Fallo en el día límite* (alta), *Vencido* (crítica) |
| Una misma regla no alerta dos veces sobre el mismo envío | `dominio › CU3: no alerta dos veces`, `tuberia › riesgo de SLA` | Cola de alertas: una fila por envío y regla (`_id = envío:regla`) |
| La alerta se cierra cuando el envío se entrega | `dominio › CU3`, `tuberia › riesgo de SLA`, `tuberia › escenario demorado` | Escenario *Demorado*: la alerta desaparece de la cola al entregarse |
| Tras reiniciar el monitor no se pierden ni se repiten alertas | `tuberia › recuperación`: reinicia el monitor a mitad de corrida desde su respaldo y compara con una corrida sin caída | `docker compose restart monitor-sla` durante el escenario *Demorado* |

## Analítica y reprocesamiento

| Criterio | Prueba automática | Sobre el entorno |
| --- | --- | --- |
| Los tres indicadores coinciden con un recuento manual sobre una corrida con semilla fija | `tuberia › analítica` | `scripts/verificar-analitica.js` (pausa el simulador, recuenta desde Kafka y compara con `metricas_diarias`) |
| El cuarto indicador se completa con datos históricos sin reiniciar los demás servicios | `tuberia › reprocesamiento` | `docker compose --profile replay up -d analitica-tiempos` |
| Borrar envíos y reprocesar deja la colección en el mismo estado | `tuberia › reconstrucción` (dos intercalados distintos) | `scripts/reconstruccion.sh` / `.ps1`: compara hashes SHA-256 |

## Entorno

| Criterio | Verificación |
| --- | --- |
| Todo el sistema se levanta con un solo comando en una máquina limpia | `docker compose up -d --build` (las dependencias se resuelven con healthchecks: Kafka → creación de topics → servicios; Mongo con replica set inicializado) |
| La latencia entre evento y pantalla es menor a 2 segundos | La torre de control mide, para cada actualización, la diferencia entre el timestamp de Kafka y la llegada al navegador, y muestra la mediana y el máximo |
