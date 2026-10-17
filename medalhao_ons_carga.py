"""
Arquitetura medalhão (bronze -> silver -> gold) com a Carga de Energia Diária do ONS,
com logs estruturados e rastreabilidade ponta a ponta.

Uso:
    pip install -r requirements.txt
    python medalhao_ons_carga.py CARGA_ENERGIA_2026.parquet
    python medalhao_ons_carga.py CARGA_ENERGIA_2026.parquet --lakehouse lakehouse --sem-falhar

Boas práticas demonstradas (procure estes termos no código):
  1. run_id           identificador único de cada execução, presente em logs, dados e manifesto
  2. fingerprint      SHA-256 do arquivo de origem: prova de qual dado entrou
  3. bronze imutável  append-only, uma pasta por execução; nunca sobrescreve histórico
  4. idempotência     rodar duas vezes o mesmo arquivo não duplica a silver (dedup por chave)
  5. quality gate     regras de qualidade que BARRAM a publicação (silver/gold) se falharem
  6. escrita atômica  grava em arquivo temporário e renomeia: ninguém lê arquivo pela metade
  7. manifesto        JSON por execução com contagens, qualidade, hashes e versões
  8. logs             formato único, horário em UTC, duração de cada etapa
"""
import argparse
import hashlib
import json
import logging
import platform
import sys
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import duckdb

# ------------------------------------------------------------------ logging ---
class FiltroRunId(logging.Filter):
    """Injeta o run_id em TODA linha de log, para filtrar uma execução específica."""
    def __init__(self, run_id: str):
        super().__init__()
        self.run_id = run_id

    def filter(self, record: logging.LogRecord) -> bool:
        record.run_id = self.run_id
        return True


def configurar_log(run_id: str, pasta_logs: Path) -> logging.Logger:
    pasta_logs.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | run=%(run_id)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
    fmt.converter = time.gmtime  # horário em UTC: evita ambiguidade de fuso e horário de verão
    log = logging.getLogger("medalhao")
    log.setLevel(logging.INFO)
    log.handlers.clear()
    for h in (logging.StreamHandler(sys.stdout), logging.FileHandler(pasta_logs / f"{run_id}.log", encoding="utf-8")):
        h.setFormatter(fmt)
        h.addFilter(FiltroRunId(run_id))
        log.addHandler(h)
    return log


@contextmanager
def etapa(log: logging.Logger, nome: str):
    """Registra início, fim e duração de cada etapa; em erro, loga o traceback completo."""
    log.info("início | etapa=%s", nome)
    t0 = time.perf_counter()
    try:
        yield
    except Exception:
        log.exception("falha | etapa=%s", nome)
        raise
    log.info("fim | etapa=%s | duracao_s=%.3f", nome, time.perf_counter() - t0)


# ---------------------------------------------------------------- utilidades ---
def sha256_arquivo(caminho: Path) -> str:
    """Hash em blocos de 1 MB: funciona com arquivos grandes sem carregar tudo na memória."""
    h = hashlib.sha256()
    with caminho.open("rb") as f:
        for bloco in iter(lambda: f.read(1024 * 1024), b""):
            h.update(bloco)
    return h.hexdigest()


def gravar_parquet_atomico(con, consulta: str, destino: Path) -> None:
    """Escrita atômica: grava em .tmp e renomeia. O rename é atômico no mesmo disco."""
    destino.parent.mkdir(parents=True, exist_ok=True)
    tmp = destino.with_suffix(".parquet.tmp")
    con.execute(f"COPY ({consulta}) TO '{tmp}' (FORMAT parquet, COMPRESSION zstd)")
    tmp.replace(destino)


def contar(con, consulta: str) -> int:
    return con.execute(f"SELECT count(*) FROM ({consulta})").fetchone()[0]


# ------------------------------------------------------------------- pipeline ---
def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("entrada", type=Path, help="arquivo Parquet de carga diária do ONS")
    ap.add_argument("--lakehouse", type=Path, default=Path("lakehouse"))
    ap.add_argument("--sem-falhar", action="store_true",
                    help="publica mesmo com falha de qualidade (não recomendado em produção)")
    args = ap.parse_args()

    inicio = datetime.now(timezone.utc)
    run_id = inicio.strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]
    raiz = args.lakehouse
    log = configurar_log(run_id, raiz / "logs")
    con = duckdb.connect()

    log.info("execução iniciada | python=%s | duckdb=%s", platform.python_version(), duckdb.__version__)

    # 1) FINGERPRINT da origem: qual arquivo exatamente entrou nesta execução
    with etapa(log, "fingerprint_origem"):
        sha = sha256_arquivo(args.entrada)
        tamanho = args.entrada.stat().st_size
        log.info("origem | arquivo=%s | bytes=%d | sha256=%s", args.entrada.name, tamanho, sha[:16] + "…")

    manifesto = {
        "run_id": run_id, "inicio_utc": inicio.isoformat(), "status": "EM_ANDAMENTO",
        "origem": {"arquivo": args.entrada.name, "bytes": tamanho, "sha256": sha},
        "versoes": {"python": platform.python_version(), "duckdb": duckdb.__version__},
        "camadas": {}, "qualidade": [],
    }

    # 2) BRONZE: dado como chegou + metadados. Append-only: uma pasta por execução.
    bronze_atual = raiz / "bronze" / "carga_energia_di" / f"run_id={run_id}" / "data.parquet"
    with etapa(log, "bronze"):
        ts = inicio.strftime("%Y-%m-%d %H:%M:%S")
        gravar_parquet_atomico(con, f"""
            SELECT *,
                   CAST('{ts}' AS TIMESTAMP) AS _ingestao_ts_utc,
                   '{run_id}'                AS _run_id,
                   '{args.entrada.name}'     AS _arquivo_origem,
                   '{sha}'                   AS _sha256_arquivo,
                   md5(CAST(t AS VARCHAR))   AS _hash_linha      -- detecta mudança de valor entre cargas
            FROM read_parquet('{args.entrada}') AS t""", bronze_atual)
        n_bronze = contar(con, f"SELECT * FROM read_parquet('{bronze_atual}')")
        log.info("bronze gravado | linhas=%d | destino=%s", n_bronze, bronze_atual)
        manifesto["camadas"]["bronze"] = {"linhas": n_bronze, "arquivo": str(bronze_atual)}

    # 3) SILVER (em memória até passar no quality gate)
    # Lê TODAS as execuções do bronze e deduplica pela chave de negócio, mantendo a ingestão
    # mais recente. É isso que torna o pipeline idempotente.
    with etapa(log, "silver_transformacao"):
        glob_bronze = f"{raiz}/bronze/carga_energia_di/*/data.parquet"
        n_bronze_total = contar(con, f"SELECT * FROM read_parquet('{glob_bronze}')")
        con.execute(f"""
            CREATE TABLE silver AS
            SELECT id_subsistema, nom_subsistema,
                   CAST(din_instante AS DATE)            AS data_referencia,
                   CAST(val_cargaenergiamwmed AS DOUBLE) AS carga_mwmed,
                   _run_id, _ingestao_ts_utc, _arquivo_origem
            FROM read_parquet('{glob_bronze}')
            QUALIFY row_number() OVER (
                PARTITION BY id_subsistema, CAST(din_instante AS DATE)
                ORDER BY _ingestao_ts_utc DESC, _run_id DESC) = 1""")
        n_silver = contar(con, "SELECT * FROM silver")
        log.info("silver | linhas_bronze_total=%d | linhas_silver=%d | removidas_por_dedup=%d",
                 n_bronze_total, n_silver, n_bronze_total - n_silver)

    # 4) QUALITY GATE: cada regra tem valor, esperado e severidade
    with etapa(log, "quality_gate"):
        q = lambda sql: con.execute(sql).fetchone()[0]
        chaves_dup_origem = q(f"""SELECT count(*) - count(DISTINCT (id_subsistema, CAST(din_instante AS DATE)))
                                  FROM read_parquet('{bronze_atual}')""")
        regras = [
            # (nome, valor medido, esperado, severidade se falhar)
            ("chaves_duplicadas_na_origem", chaves_dup_origem, 0, "AVISO"),   # dedup resolve, mas é bom saber
            ("nulos_em_carga", q("SELECT count(*) FROM silver WHERE carga_mwmed IS NULL"), 0, "ERRO"),
            ("cargas_nao_positivas", q("SELECT count(*) FROM silver WHERE carga_mwmed <= 0"), 0, "ERRO"),
            ("subsistemas_distintos", q("SELECT count(DISTINCT id_subsistema) FROM silver"), 4, "ERRO"),
            ("dias_sem_os_4_subsistemas",
             q("SELECT count(*) FROM (SELECT 1 FROM silver GROUP BY data_referencia HAVING count(*) <> 4)"), 0, "ERRO"),
            ("lacunas_de_data",
             q("SELECT (max(data_referencia) - min(data_referencia) + 1) - count(DISTINCT data_referencia) FROM silver"),
             0, "ERRO"),
        ]
        falhas = 0
        for nome, valor, esperado, sev in regras:
            ok = valor == esperado
            manifesto["qualidade"].append({"regra": nome, "valor": valor, "esperado": esperado,
                                           "severidade": sev, "ok": ok})
            if ok:
                log.info("qualidade OK | regra=%s | valor=%s", nome, valor)
            elif sev == "AVISO":
                log.warning("qualidade AVISO | regra=%s | valor=%s | esperado=%s", nome, valor, esperado)
            else:
                falhas += 1
                log.error("qualidade FALHA | regra=%s | valor=%s | esperado=%s", nome, valor, esperado)

    # 5) Decisão do gate: com falha, NÃO publica silver nem gold. O bronze fica preservado.
    if falhas and not args.sem_falhar:
        log.error("publicação BLOQUEADA | falhas=%d | bronze preservado em %s", falhas, bronze_atual)
        manifesto.update(status="FALHA_QUALIDADE", fim_utc=datetime.now(timezone.utc).isoformat())
        (raiz / "manifests").mkdir(parents=True, exist_ok=True)
        (raiz / "manifests" / f"{run_id}.json").write_text(json.dumps(manifesto, indent=2, ensure_ascii=False))
        return 1

    # 6) Publicação: silver e gold
    with etapa(log, "publicacao_silver_gold"):
        silver_path = raiz / "silver" / "carga_energia_di.parquet"
        gravar_parquet_atomico(con, "SELECT * FROM silver", silver_path)
        gold_mensal = raiz / "gold" / "carga_media_mensal.parquet"
        gold_sin = raiz / "gold" / "carga_sin_diaria.parquet"
        gravar_parquet_atomico(con, """
            SELECT date_trunc('month', data_referencia)::DATE AS mes, id_subsistema,
                   round(avg(carga_mwmed), 1) AS carga_media_mwmed
            FROM silver GROUP BY 1, 2 ORDER BY 1, 2""", gold_mensal)
        # carga do SIN = soma dos 4 subsistemas; só dias completos entram
        gravar_parquet_atomico(con, """
            SELECT data_referencia, round(sum(carga_mwmed), 1) AS carga_sin_mwmed
            FROM silver GROUP BY 1 HAVING count(*) = 4 ORDER BY 1""", gold_sin)
        for nome, p in [("silver", silver_path), ("gold_mensal", gold_mensal), ("gold_sin_diaria", gold_sin)]:
            n = contar(con, f"SELECT * FROM read_parquet('{p}')")
            log.info("publicado | camada=%s | linhas=%d | bytes=%d | destino=%s", nome, n, p.stat().st_size, p)
            manifesto["camadas"][nome] = {"linhas": n, "bytes": p.stat().st_size,
                                          "sha256": sha256_arquivo(p), "arquivo": str(p)}

    fim = datetime.now(timezone.utc)
    manifesto.update(status="SUCESSO" if not falhas else "PUBLICADO_COM_FALHAS",
                     fim_utc=fim.isoformat(), duracao_s=round((fim - inicio).total_seconds(), 3))
    (raiz / "manifests").mkdir(parents=True, exist_ok=True)
    (raiz / "manifests" / f"{run_id}.json").write_text(json.dumps(manifesto, indent=2, ensure_ascii=False))
    log.info("execução concluída | status=%s | duracao_s=%.3f | manifesto=%s",
             manifesto["status"], manifesto["duracao_s"], raiz / "manifests" / f"{run_id}.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
