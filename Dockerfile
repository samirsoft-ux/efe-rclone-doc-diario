# syntax=docker/dockerfile:1

ARG PYTHON_VERSION=3.9
FROM python:${PYTHON_VERSION}-slim as base

# Evita que Python escriba archivos pyc
ENV PYTHONDONTWRITEBYTECODE=1

# Evita que Python almacene en buffer stdout y stderr
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Crear un usuario no privilegiado para ejecutar la aplicación
ARG UID=10001
RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/app" \
    --uid "${UID}" \
    appuser

RUN chown appuser:appuser /app

# Instalar rclone
RUN apt-get update && apt-get install -y rclone && rm -rf /var/lib/apt/lists/*

# Actualiza pip e instala dependencias de Python
# Asegúrate de tener un archivo requirements.txt en tu directorio de proyecto
# Si optas por usar requirements.txt, no olvides agregar ibm-cloud-sdk-core y ibm-secrets-manager-sdk a este archivo
RUN pip install --upgrade pip && \
    pip install --no-cache-dir psycopg2-binary ibm-cos-sdk ibm-cloud-sdk-core ibm-secrets-manager-sdk requests

# Cambiar al usuario no privilegiado para ejecutar la aplicación
USER appuser

# Copiar tu script Python en el contenedor
COPY rclone.py .

# Exponer el puerto que utiliza la aplicación, si es necesario
# EXPOSE 5000

# Ejecutar la aplicación
CMD ["python", "rclone.py"]
