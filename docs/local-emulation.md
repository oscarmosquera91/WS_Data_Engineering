## Emulación local del entorno (Fabric ↔ Local)

Este documento muestra pasos mínimos para homologar el comportamiento de un Warehouse y un Lakehouse
en un entorno local de desarrollo. Incluye un `docker-compose` de ejemplo para levantar dependencias
habituales (SQL Server y MinIO) y pasos para ejecutar pruebas smoke.

Requisitos
- Docker y Docker Compose instalados.
- Python 3.9+ (para ejecutar tests locales y scripts auxiliares).

1) Estructura de carpetas recomendada

  project-root/
  ├─ docs/config.yml
  ├─ local_lakehouse/   <-- mount para archivos locales
  └─ sample_data/

2) docker-compose mínimo (ejecutar en el root del repo)

Guarda el siguiente contenido temporal como `docker-compose.local.yml`:

```yaml
version: '3.8'
services:
  mssql:
    image: mcr.microsoft.com/mssql/server:2019-latest
    environment:
      SA_PASSWORD: "Your_Strong!Passw0rd"
      ACCEPT_EULA: "Y"
    ports:
      - "1433:1433"
    healthcheck:
      test: ["CMD-SHELL", " /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P Your_Strong!Passw0rd -Q \"SELECT 1\""]
      interval: 10s
      retries: 10

  minio:
    image: minio/minio:latest
    command: server /data
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    ports:
      - "9000:9000"
    volumes:
      - ./local_lakehouse:/data

```

Ejecutar:

```cmd
docker compose -f docker-compose.local.yml up -d
```

3) Variables de entorno para el modo `local`

En Windows (cmd.exe) puedes exportar:

```cmd
set ENV=local
set LOCAL_WAREHOUSE_USER=sa
set LOCAL_WAREHOUSE_PASS=Your_Strong!Passw0rd
set LOCAL_LAKEHOUSE_PATH=./local_lakehouse
```

Si usas PowerShell:

```powershell
$env:ENV = "local"
$env:LOCAL_WAREHOUSE_USER = "sa"
$env:LOCAL_WAREHOUSE_PASS = "Your_Strong!Passw0rd"
$env:LOCAL_LAKEHOUSE_PATH = "./local_lakehouse"
```

4) Cargar datos de ejemplo

- Coloca CSVs o Parquet en `sample_data/` y usa un script simple para insertar datos de prueba en SQL Server
  y copiar ficheros a `local_lakehouse/`.

Ejemplo rápido con `sqlcmd` (desde contenedor o host si tienes cliente):

```cmd
docker exec -it %COMPOSE_PROJECT_NAME%_mssql_1 /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P Your_Strong!Passw0rd -Q "CREATE DATABASE warehouse_local;"
```

5) Ejecutar smoke tests

- Añade tests de integración ligeros en `tests/integration/` que:
  - Conecten al `LOCAL` warehouse usando `LOCAL_WAREHOUSE_USER`/`LOCAL_WAREHOUSE_PASS`.
  - Lean/escriban un fichero en `local_lakehouse`.

Ejecutar (desde repo):

```cmd
python -m pytest tests/integration -q
```

6) Mapeo a `docs/config.yml`

- En `docs/config.yml` la sección `local` apunta a los endpoints locales. Asegúrate de haber exportado
  las variables referenciadas (p. ej. `LOCAL_WAREHOUSE_USER`). Si prefieres, puedes añadir `secret_ref`
  con prefijo `kv://` y configurar el resolver en producción para Key Vault.

7) Notas y buenas prácticas
- Mantener los datos de ejemplo pequeños (<<100MB) para pruebas rápidas.
- Evitar hardcodear contraseñas; usar variables de entorno para local o un archivo `.env` (no commitear).
- Implementar tests de contrato para comparar schemas entre local y snapshots esperados.

Si quieres, implemento un script `scripts/bootstrap_local.py` y un test de ejemplo para integrar este flujo.
