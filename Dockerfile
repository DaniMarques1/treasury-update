FROM python:3.12-slim

# Dependências do sistema para compilação de pacotes Python e PostgreSQL
RUN apt-get update && apt-get install -y \
    build-essential \
    python3-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Instalação das dependências dos scripts Postgres
COPY postgres/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copia os arquivos do projeto
COPY . /app

# Define diretório de trabalho na pasta dos scripts postgres
WORKDIR /app/postgres
ENV PYTHONPATH=/app/postgres

CMD ["python", "-u", "run.py"]
