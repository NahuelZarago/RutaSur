#!/usr/bin/env bash
# Demostración completa de reconstrucción de la vista de lectura desde el log de Kafka.
# Uso (desde la raíz del proyecto, con el entorno levantado):  bash scripts/reconstruccion.sh
set -euo pipefail

herramienta() { docker compose run --rm herramientas node scripts/reconstruir.js "$@"; }

echo "== 1/4 Instantánea de envios, alertas y notificaciones (pausa el simulador)"
herramienta instantanea

echo "== 2/4 Deteniendo el proyector"
docker compose stop proyector

echo "== 3/4 Borrando las vistas y rebobinando los offsets del grupo proyector"
herramienta reiniciar

echo "== 4/4 Reiniciando el proyector y comparando"
docker compose start proyector
herramienta comparar
