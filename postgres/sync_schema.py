#!/usr/bin/env python3
"""
==============================================================================
Script de Sincronização do Schema PostgreSQL -> schema.sql
==============================================================================
Este script conecta ao PostgreSQL da VPS (com ou sem túnel SSH automático)
e faz o dump/introspecção da estrutura DDL atual (tabelas, colunas, chaves primárias,
índices e views) para o arquivo schema.sql em treasury-update.
"""

import os
import sys
from dotenv import load_dotenv
import psycopg2

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Carrega variáveis
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
load_dotenv(os.path.join(BASE_DIR, ".env"))

SCHEMA_SQL_PATH = os.path.join(BASE_DIR, "schema.sql")

def get_connection():
    ssh_host = os.getenv("SSH_HOST")
    db_host = os.getenv("DB_HOST", "127.0.0.1")
    db_port = int(os.getenv("DB_PORT", 5432))
    db_name = os.getenv("DB_NAME", "axie_treasury")
    db_user = os.getenv("DB_USER", "owner_axie_treasury")
    db_pass = os.getenv("DB_PASSWORD")

    if ssh_host:
        from sshtunnel import SSHTunnelForwarder
        tunnel = SSHTunnelForwarder(
            (ssh_host, int(os.getenv("SSH_PORT", 22))),
            ssh_username=os.getenv("SSH_USER", "root"),
            ssh_password=os.getenv("SSH_PASSWORD"),
            remote_bind_address=(db_host, db_port),
            allow_agent=False,
            host_pkey_directories=[]
        )
        tunnel.start()
        conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_pass,
            host="127.0.0.1",
            port=tunnel.local_bind_port
        )
        return conn, tunnel
    else:
        conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_pass,
            host=db_host,
            port=db_port
        )
        return conn, None

def sync_schema():
    print("⏳ Conectando ao PostgreSQL para ler o schema...")
    conn, tunnel = None, None
    try:
        conn, tunnel = get_connection()
        cur = conn.cursor()

        # Obter tabelas públicas
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            ORDER BY table_name;
        """)
        tables = [row[0] for row in cur.fetchall()]

        ddl_output = []
        ddl_output.append("-- ==============================================================================")
        ddl_output.append("-- AXIE TREASURY DATABASE SCHEMA (PostgreSQL)")
        ddl_output.append(f"-- Sincronizado automaticamente")
        ddl_output.append("-- ==============================================================================\n")
        ddl_output.append('CREATE EXTENSION IF NOT EXISTS "pgcrypto";\n')

        for table in tables:
            ddl_output.append(f"-- ------------------------------------------------------------------------------")
            ddl_output.append(f"-- Tabela: {table}")
            ddl_output.append(f"-- ------------------------------------------------------------------------------")
            
            # Colunas
            cur.execute("""
                SELECT 
                    column_name, 
                    data_type, 
                    character_maximum_length,
                    numeric_precision,
                    numeric_scale,
                    is_nullable, 
                    column_default
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s
                ORDER BY ordinal_position;
            """, (table,))
            columns = cur.fetchall()

            # Chave primária
            cur.execute("""
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                WHERE tc.constraint_type = 'PRIMARY KEY'
                    AND tc.table_schema = 'public'
                    AND tc.table_name = %s;
            """, (table,))
            pk_cols = [r[0] for r in cur.fetchall()]

            col_defs = []
            for col in columns:
                c_name, c_type, c_max_len, c_num_prec, c_num_scale, c_null, c_def = col
                
                # Tipo SQL
                if c_type == 'character varying':
                    sql_type = f"VARCHAR({c_max_len})" if c_max_len else "VARCHAR"
                elif c_type == 'numeric':
                    if c_num_prec and c_num_scale:
                        sql_type = f"NUMERIC({c_num_prec}, {c_num_scale})"
                    elif c_num_prec:
                        sql_type = f"NUMERIC({c_num_prec})"
                    else:
                        sql_type = "NUMERIC"
                elif c_type == 'double precision':
                    sql_type = "DOUBLE PRECISION"
                elif c_type == 'timestamp with time zone':
                    sql_type = "TIMESTAMPTZ"
                elif c_type == 'timestamp without time zone':
                    sql_type = "TIMESTAMP"
                elif c_type == 'bigint':
                    sql_type = "BIGINT"
                elif c_type == 'integer':
                    sql_type = "INT"
                elif c_type == 'boolean':
                    sql_type = "BOOLEAN"
                elif c_type == 'jsonb':
                    sql_type = "JSONB"
                elif c_type == 'json':
                    sql_type = "JSON"
                else:
                    sql_type = c_type.upper()

                line = f'    "{c_name}" {sql_type}'
                if c_name in pk_cols and len(pk_cols) == 1:
                    line += " PRIMARY KEY"
                if c_null == 'NO' and (c_name not in pk_cols or len(pk_cols) > 1):
                    line += " NOT NULL"
                if c_def:
                    line += f" DEFAULT {c_def}"
                
                col_defs.append(line)

            if len(pk_cols) > 1:
                pks = ', '.join([f'"{pk}"' for pk in pk_cols])
                col_defs.append(f'    PRIMARY KEY ({pks})')

            ddl_output.append(f'CREATE TABLE IF NOT EXISTS "{table}" (\n' + ',\n'.join(col_defs) + '\n);\n')

            # Índices
            cur.execute("""
                SELECT indexname, indexdef
                FROM pg_indexes
                WHERE schemaname = 'public' 
                  AND tablename = %s
                  AND indexname NOT LIKE '%%_pkey';
            """, (table,))
            indexes = cur.fetchall()
            for idx_name, idx_def in indexes:
                ddl_output.append(f"{idx_def};")
            
            if indexes:
                ddl_output.append("")

        # Views
        cur.execute("""
            SELECT table_name, view_definition
            FROM information_schema.views
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        views = cur.fetchall()
        if views:
            ddl_output.append("-- ==============================================================================")
            ddl_output.append("-- VIEWS DE CONVENIÊNCIA")
            ddl_output.append("-- ==============================================================================")
            for v_name, v_def in views:
                v_def_clean = v_def.strip().rstrip(";")
                ddl_output.append(f'CREATE OR REPLACE VIEW {v_name} AS {v_def_clean};')
            ddl_output.append("")

        final_content = "\n".join(ddl_output)
        with open(SCHEMA_SQL_PATH, "w", encoding="utf-8") as f:
            f.write(final_content)

        print(f"✅ Schema sincronizado com sucesso em: {SCHEMA_SQL_PATH}")
        print(f"📊 {len(tables)} tabelas e {len(views)} views documentadas.")

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        if tunnel:
            tunnel.stop()

if __name__ == "__main__":
    sync_schema()
