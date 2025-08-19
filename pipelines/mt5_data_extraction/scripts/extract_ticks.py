import os
from datetime import datetime
from pathlib import Path
import pandas as pd
import duckdb

# ---------- CONFIG ----------
PAIRS = [("EURUSD", 1.0), ("GBPUSD", 1.0)]
START_DATE = datetime(2024, 1, 1)
END_DATE   = datetime(2025, 1, 31)
OUTPUT_DIR  = "parquet_output"
DUCKDB_PATH = "mt5_data.duckdb"
TMP_DATA_DIR = Path("histdata_cache")  # cache/local

# ---------- UTILS ----------
def ym_str(d: datetime) -> str:
    return f"{d.year}-{d.month:02d}"

def compute_spread_proxy(row, contract=1.0):
    return float(contract) * (float(row["HIGH"]) - float(row["LOW"]))

def _ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

# ---------- HISTDATACOM (M1 e TICK) ----------
def fetch_histdata_m1(symbol: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """
    1-minute OHLCV (HistData.com via histdatacom) -> TIME(UTC), OPEN,HIGH,LOW,CLOSE[,TICK_VOLUME?]
    """
    import histdatacom
    from histdatacom.options import Options

    opt = Options()
    opt.api_return_type   = "pandas"          # se faltar 'datatable', mude para 'arrow'
    opt.formats           = {"ascii"}
    opt.timeframes        = {"1-minute-bar-quotes"}
    opt.pairs             = {symbol.lower()}
    opt.start_yearmonth   = ym_str(start_date)
    opt.end_yearmonth     = ym_str(end_date)
    opt.cpu_utilization   = "high"
    opt.data_directory    = str(TMP_DATA_DIR / symbol / "M1")

    data = histdatacom(opt)
    if isinstance(data, list):
        data = data[0]["data"]

    cols = {c.lower(): c for c in data.columns}
    df = data.rename(columns={cols.get("datetime","datetime"): "TIME_MS"}).copy()
    if "open"  in cols: df.rename(columns={cols["open"]:  "OPEN"},  inplace=True)
    if "high"  in cols: df.rename(columns={cols["high"]:  "HIGH"},  inplace=True)
    if "low"   in cols: df.rename(columns={cols["low"]:   "LOW"},   inplace=True)
    if "close" in cols: df.rename(columns={cols["close"]: "CLOSE"}, inplace=True)
    if "vol"   in cols: df.rename(columns={cols["vol"]:   "TICK_VOLUME"}, inplace=True)

    df["TIME"] = pd.to_datetime(df["TIME_MS"], unit="ms", utc=True)

    keep = ["TIME","OPEN","HIGH","LOW","CLOSE"]
    if "TICK_VOLUME" in df.columns:
        keep.append("TICK_VOLUME")
    df = df[keep]
    df = df[(df["TIME"] >= pd.Timestamp(START_DATE, tz="UTC")) &
            (df["TIME"] <= pd.Timestamp(END_DATE, tz="UTC"))].reset_index(drop=True)
    return df

def fetch_histdata_tick_aggs(symbol: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """
    Ticks Bid/Ask -> por minuto:
      - SPREAD_TICK = média(ASK) - média(BID)
      - TICK_COUNT  = nº de ticks no minuto  (usaremos isso em TICK_VOLUME)
      - TICK_VOL_SUM (se houver coluna 'vol' no tick)
    """
    import histdatacom
    from histdatacom.options import Options

    opt = Options()
    opt.api_return_type   = "pandas"
    opt.formats           = {"ascii"}                 # Tick Ask/Bid só em ASCII
    opt.timeframes        = {"tick-data-quotes"}
    opt.pairs             = {symbol.lower()}
    opt.start_yearmonth   = ym_str(start_date)
    opt.end_yearmonth     = ym_str(end_date)
    opt.cpu_utilization   = "high"
    opt.data_directory    = str(TMP_DATA_DIR / symbol / "TICK")

    data = histdatacom(opt)
    if isinstance(data, list):
        data = data[0]["data"]

    cols = {c.lower(): c for c in data.columns}
    ticks = data.rename(columns={
        cols.get("datetime","datetime"): "TIME_MS",
        cols.get("bid","bid"): "BID",
        cols.get("ask","ask"): "ASK"
    }).copy()
    if "vol" in cols:
        ticks.rename(columns={cols["vol"]: "VOL"}, inplace=True)

    ticks["TIME"] = pd.to_datetime(ticks["TIME_MS"], unit="ms", utc=True)
    ticks["MINUTE"] = ticks["TIME"].dt.floor("min")

    agg = ticks.groupby("MINUTE").agg(
        AVG_BID=("BID","mean"),
        AVG_ASK=("ASK","mean"),
        TICK_COUNT=("TIME","size"),
        TICK_VOL_SUM=("VOL","sum") if "VOL" in ticks.columns else ("TIME","size")
    ).reset_index()
    agg["SPREAD_TICK"] = agg["AVG_ASK"] - agg["AVG_BID"]
    # só retornamos o que vamos usar
    return agg[["MINUTE","SPREAD_TICK","TICK_COUNT","TICK_VOL_SUM"]]

# ---------- FALLBACK: DUKASCOPY (opcional) ----------
def fetch_dukascopy_tick_aggs(symbol: str, start_date: datetime, end_date: datetime) -> pd.DataFrame | None:
    try:
        import dukascopy_python as dkp
    except Exception:
        return None

    dl = dkp.Downloader()
    try:
        df_ticks = dl.get_history(instrument=symbol, start_date=start_date, end_date=end_date, timeframe="tick")
    except Exception:
        return None

    if "datetime" in df_ticks.columns:
        df_ticks["TIME"] = pd.to_datetime(df_ticks["datetime"], utc=True)
    else:
        df_ticks["TIME"] = pd.to_datetime(df_ticks.iloc[:,0], utc=True)

    if "bid" not in df_ticks.columns or "ask" not in df_ticks.columns:
        return None

    df_ticks["MINUTE"] = df_ticks["TIME"].dt.floor("min")
    agg = (df_ticks.groupby("MINUTE")
                  .agg(AVG_BID=("bid","mean"), AVG_ASK=("ask","mean"), TICK_COUNT=("TIME","size"))
                  .reset_index())
    agg["TICK_VOL_SUM"] = agg["TICK_COUNT"]
    agg["SPREAD_TICK"] = agg["AVG_ASK"] - agg["AVG_BID"]
    return agg[["MINUTE","SPREAD_TICK","TICK_COUNT","TICK_VOL_SUM"]]

# ---------- PIPELINE PRINCIPAL ----------
def fetch_public_symbol_data_with_spread(start_date: datetime, end_date: datetime, pairs, output_dir=OUTPUT_DIR):
    _ensure_dir(Path(output_dir))
    _ensure_dir(TMP_DATA_DIR)

    for symbol, contract in pairs:
        print(f"=== {symbol} | baixando público (HistData) ===")

        # M1
        try:
            m1 = fetch_histdata_m1(symbol, start_date, end_date)
        except Exception as e:
            print(f"[WARN] M1 falhou para {symbol}: {e}")
            m1 = pd.DataFrame(columns=["TIME","OPEN","HIGH","LOW","CLOSE"])

        if m1.empty:
            print(f"[WARN] Sem M1 para {symbol} no período.")
            continue

        # Ticks (preferencial)
        try:
            ticks = fetch_histdata_tick_aggs(symbol, start_date, end_date)
            if ticks is None or ticks.empty:
                raise RuntimeError("tick vazio")
        except Exception as e:
            print(f"[WARN] Tick (HistData) falhou {symbol}: {e} | tentando Dukascopy…")
            ticks = fetch_dukascopy_tick_aggs(symbol, start_date, end_date)

        # -------- Merge + spreads + TICK_VOLUME por contagem de ticks ----------
        m1 = m1.copy()
        m1["MINUTE"] = m1["TIME"].dt.floor("min")

        if ticks is not None and not ticks.empty:
            # evita duplicidade por minuto em cada lado
            m1 = m1.sort_values("MINUTE").drop_duplicates(subset=["MINUTE"], keep="last")
            ticks = ticks.sort_values("MINUTE").drop_duplicates(subset=["MINUTE"], keep="last")

            df = m1.merge(ticks, how="left", on="MINUTE", validate="one_to_one")

            # spread: preferir tick; se faltar, proxy OHLC
            df["SPREAD"] = (df["SPREAD_TICK"] * float(contract))
            df["SPREAD"] = df["SPREAD"].where(df["SPREAD"].notna(),
                                              df.apply(lambda r: compute_spread_proxy(r, contract), axis=1))

            # >>> TICK_VOLUME pela contagem de ticks <<<
            # prioridade: se M1 já trouxe TICK_VOLUME, preserva; senão usa TICK_COUNT; senão 0.
            if "TICK_VOLUME" not in df.columns:
                df["TICK_VOLUME"] = pd.NA
            df["TICK_VOLUME"] = df["TICK_VOLUME"].fillna(df["TICK_COUNT"]).fillna(0)

        else:
            # Sem ticks: calcula spread por proxy e marca TICK_VOLUME=0
            df = m1.copy()
            df["SPREAD"] = df.apply(lambda r: compute_spread_proxy(r, contract), axis=1)
            df["TICK_VOLUME"] = 0

        # tipagem segura
        try:
            df["TICK_VOLUME"] = df["TICK_VOLUME"].astype("int64")
        except Exception:
            df["TICK_VOLUME"] = pd.to_numeric(df["TICK_VOLUME"], errors="coerce").fillna(0).astype(int)

        df["SYMBOL"] = symbol
        df["TIME_STR"] = df["TIME"].dt.strftime('%Y.%m.%d %H:%M:%S')
        final_cols = ["SYMBOL","TIME_STR","OPEN","HIGH","LOW","CLOSE","TICK_VOLUME","SPREAD"]
        df_out = df[final_cols].rename(columns={"TIME_STR":"TIME"})

        # --------- PERSISTÊNCIA INCREMENTAL (somente novos) ---------
        parquet_path = os.path.abspath(os.path.join(output_dir, f"{symbol.lower()}_spread_data.parquet"))
        table_name = f"spread_data_{symbol.lower()}"

        with duckdb.connect(DUCKDB_PATH) as con:
            # cria tabela se não existir (schema fixo)
            con.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                  SYMBOL       VARCHAR,
                  TIME         VARCHAR,   -- 'YYYY.MM.DD HH:MM:SS'
                  OPEN         DOUBLE,
                  HIGH         DOUBLE,
                  LOW          DOUBLE,
                  CLOSE        DOUBLE,
                  TICK_VOLUME  BIGINT,
                  SPREAD       DOUBLE
                );
            """)

            # índice único garante unicidade SYMBOL+TIME
            con.execute(f"""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_{table_name}_sym_time
                ON {table_name} (SYMBOL, TIME);
            """)

            # registra df_out temporariamente
            con.register("df_new", df_out)

            # 1) quantas novas antes do insert (anti-join)
            new_count = con.execute(f"""
                SELECT COUNT(*)
                FROM df_new n
                WHERE NOT EXISTS (
                  SELECT 1 FROM {table_name} e
                  WHERE e.SYMBOL = n.SYMBOL AND e.TIME = n.TIME
                );
            """).fetchone()[0]

            # 2) insere só as novas
            con.execute(f"""
                INSERT INTO {table_name}
                SELECT * FROM df_new n
                WHERE NOT EXISTS (
                  SELECT 1 FROM {table_name} e
                  WHERE e.SYMBOL = n.SYMBOL AND e.TIME = n.TIME
                );
            """)

            # 3) dedupe defensivo (sem trazer rn para a tabela final)
            con.execute(f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT SYMBOL, TIME, OPEN, HIGH, LOW, CLOSE, TICK_VOLUME, SPREAD
                FROM (
                  SELECT *,
                         ROW_NUMBER() OVER (PARTITION BY SYMBOL, TIME ORDER BY TIME) AS rn
                  FROM {table_name}
                )
                WHERE rn = 1;
            """)

            # 4) regrava o Parquet a partir da tabela (ordena por TIME)
            path_sql = parquet_path.replace("\\", "/")
            con.execute(f"""
                COPY (SELECT * FROM {table_name} ORDER BY TIME)
                TO '{path_sql}' (FORMAT 'parquet');
            """)

            print(f"✔ DuckDB: {table_name} recebeu {new_count} linhas novas")
            print(f"✔ Parquet atualizado: {parquet_path}")

# ---------- RUN ----------
if __name__ == "__main__":
    fetch_public_symbol_data_with_spread(START_DATE, END_DATE, PAIRS, OUTPUT_DIR)
