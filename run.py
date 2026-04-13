import subprocess
import time
from datetime import datetime, timedelta, timezone

scripts = [
    "buildDailyData.py",
    "overviewData.py",
    "typeData.py",
    "sumAXS.py",
    "sumBAXS.py"
]


# Helper para garantir que pegamos sempre a hora atual em UTC
def get_now_utc():
    return datetime.now(timezone.utc)


def run_scripts():
    now_utc = get_now_utc()
    print(f"\n[{now_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC] Iniciando ciclo de scripts...")

    for script in scripts:
        print(f"[{get_now_utc().strftime('%H:%M:%S')} UTC] Executando {script}...")
        try:
            result = subprocess.run(
                ["python", script],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace"
            )
            # Imprime os resultados removendo quebras de linha em branco extras
            if result.stdout:
                print(result.stdout.strip())
            if result.stderr:
                print(f"AVISO/ERRO ({script}):\n{result.stderr.strip()}")
        except Exception as e:
            print(f"Falha crítica ao tentar rodar {script}: {e}")

        time.sleep(2)  # Mantida a sua pausa de 2s entre os scripts

    print(f"[{get_now_utc().strftime('%Y-%m-%d %H:%M:%S')} UTC] Ciclo completo.")


def run_scheduler():
    print("--- Orquestrador de Scripts (UTC 0) Iniciado ---")

    # 1. EXECUÇÃO IMEDIATA AO RODAR O ORQUESTRADOR
    run_scripts()

    # 2. LOOP DE AGENDAMENTO
    while True:
        now = get_now_utc()

        # Define o alvo como 00:00:00 do DIA SEGUINTE
        tomorrow = now + timedelta(days=1)
        target = tomorrow.replace(hour=0, minute=0, second=0, microsecond=0)

        # Calcula a diferença exata em segundos
        wait_seconds = (target - now).total_seconds()

        print(
            f"\n[{now.strftime('%H:%M:%S')} UTC] Aguardando próximo ciclo programado em: {target.strftime('%d/%m/%Y %H:%M:%S')} UTC")
        print(f"Tempo de espera: {timedelta(seconds=int(wait_seconds))}")

        time.sleep(wait_seconds)

        # Roda o ciclo novamente
        run_scripts()


if __name__ == "__main__":
    run_scheduler()