# Demostración de reconstrucción para Windows (PowerShell).
# Uso (desde la raíz del proyecto, con el entorno levantado):  ./scripts/reconstruccion.ps1
$ErrorActionPreference = "Stop"

function Herramienta($paso) {
  docker compose run --rm herramientas node scripts/reconstruir.js $paso
  if ($LASTEXITCODE -ne 0) { throw "Falló el paso $paso" }
}

Write-Host "== 1/4 Instantánea de envios, alertas y notificaciones (pausa el simulador)"
Herramienta "instantanea"

Write-Host "== 2/4 Deteniendo el proyector"
docker compose stop proyector

Write-Host "== 3/4 Borrando las vistas y rebobinando los offsets del grupo proyector"
Herramienta "reiniciar"

Write-Host "== 4/4 Reiniciando el proyector y comparando"
docker compose start proyector
Herramienta "comparar"
