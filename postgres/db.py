"""
==============================================================================
Módulo de Conexão Híbrida PostgreSQL
==============================================================================
Gerencia conexões com o PostgreSQL suportando:
1. Modo Local: Cria túnel SSH criptografado caso SSH_HOST esteja definido.
2. Modo Servidor / VPS: Conecta diretamente em 127.0.0.1:5432.
"""

import os
import atexit
from contextlib import contextmanager
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

# Carrega as variáveis de ambiente
load_dotenv()

_SSH_TUNNEL = None

def get_tunnel():
    global _SSH_TUNNEL
    ssh_host = os.getenv("SSH_HOST")
    if not ssh_host:
        return None

    if _SSH_TUNNEL is None or not _SSH_TUNNEL.is_active:
        from sshtunnel import SSHTunnelForwarder
        ssh_port = int(os.getenv("SSH_PORT", 22))
        ssh_user = os.getenv("SSH_USER", "root")
        ssh_pass = os.getenv("SSH_PASSWORD")
        db_host = os.getenv("DB_HOST", "127.0.0.1")
        db_port = int(os.getenv("DB_PORT", 5432))

        _SSH_TUNNEL = SSHTunnelForwarder(
            (ssh_host, ssh_port),
            ssh_username=ssh_user,
            ssh_password=ssh_pass,
            remote_bind_address=(db_host, db_port),
            allow_agent=False,
            host_pkey_directories=[]
        )
        _SSH_TUNNEL.start()
    return _SSH_TUNNEL

def close_tunnel():
    global _SSH_TUNNEL
    if _SSH_TUNNEL is not None and _SSH_TUNNEL.is_active:
        _SSH_TUNNEL.stop()
        _SSH_TUNNEL = None

atexit.register(close_tunnel)

def get_connection_params():
    tunnel = get_tunnel()
    if tunnel:
        host = "127.0.0.1"
        port = tunnel.local_bind_port
    else:
        host = os.getenv("DB_HOST", "127.0.0.1")
        port = int(os.getenv("DB_PORT", 5432))

    return {
        "dbname": os.getenv("DB_NAME", "axie_treasury"),
        "user": os.getenv("DB_USER", "owner_axie_treasury"),
        "password": os.getenv("DB_PASSWORD"),
        "host": host,
        "port": port,
        "connect_timeout": 15
    }

def get_connection():
    params = get_connection_params()
    return psycopg2.connect(**params)

@contextmanager
def get_cursor(commit=True, cursor_factory=None):
    conn = get_connection()
    try:
        cur = conn.cursor(cursor_factory=cursor_factory)
        yield cur
        if commit:
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

@contextmanager
def get_dict_cursor(commit=True):
    with get_cursor(commit=commit, cursor_factory=RealDictCursor) as cur:
        yield cur
