import pandas as pd
from pathlib import Path
import unicodedata
from difflib import get_close_matches

# -------------------------------
# Rutas
# -------------------------------
INPUT_FILE = Path(r"C:\Users\iazuaz\OneDrive - Ripley Corp\Documentos\PyCharmMiscProject\Banco_Ripley\Cierre\SAC\input\sac_octubre1.xlsx")
DOTACION_FILE = INPUT_FILE.parent / r"C:\Users\iazuaz\OneDrive - Ripley Corp\Documentos\PyCharmMiscProject\Banco_Ripley\Cierre\dotacion_db\dotacion-bbdd.xlsx"
OUTPUT_FILE = INPUT_FILE.parent / r"C:\Users\iazuaz\OneDrive - Ripley Corp\Documentos\PyCharmMiscProject\Banco_Ripley\Cierre\SAC\output\sac_octubre1.xlsx"

# -------------------------------
# Mapeo (TABLA 1) actividad -> categoría consolidada (por día)
# -------------------------------
CATEGORIES_ORDER = [
    "Ausencia",
    "Capacitación Jornada Completa",
    "Capacitación sin Conexión Jornada Completa",
    "Corte de Luz Jornada Completa",
    "Festivo",
    "Libre",
    "Licencia Médica",
    "Permiso Especial Diario",
    "Presente",
    "Sin Internet Jornada Completa",
    "Sin Equipos Jornada Completa",
    "Vacaciones",
    "Desvinculación",
]

CATEGORIES_MAP_EXACT = {
    "en la cola": "Presente",
    "descanso 15 min": "Presente",
    "tiempo libre": "Libre",
    "festivo": "Festivo",
    "no se presenta": "Ausencia",
    "dia libre": "Libre",
    "descanso vf": "Libre",
    "comida full": "Presente",
    "problemas técnicos (internet)": "Sin Internet Jornada Completa",
    "vacaciones": "Vacaciones",
    "problemas técnicos (equipo)": "Sin Equipos Jornada Completa",
    "problemas técnicos (corte de luz)": "Corte de Luz Jornada Completa",
    "licencia médica": "Licencia Médica",
    "descanso vf banco": "Libre",
    "permiso especial por horas": "Permiso Especial Diario",
    "permiso con devolución de horas": "Permiso Especial Diario",
    "devolución horas": "Permiso Especial Diario",
    "vive tu momentos": "Libre",
    "desvinculación": "Desvinculación",
    "capacitación jornada completa": "Capacitación Jornada Completa",
    "capacitación": "Presente",
    "permiso con descuento": "Permiso Especial Diario",
    "vacaciones en día libre": "Vacaciones",
    "licencia médica en día libre": "Licencia Médica",
    "problemas técnicos (bloqueo/reseteo cuenta)": "Sin Equipos Jornada Completa",
    "permiso especial diario": "Permiso Especial Diario",
    "fuero maternal": "Permiso Especial Diario",
}

REGLAS_CONTAINS = [
    ("capacitación sin conexión", "Capacitación sin Conexión Jornada Completa"),
    ("sin equipos", "Sin Equipos Jornada Completa"),
    ("sin equipo", "Sin Equipos Jornada Completa"),
    ("corte de luz", "Corte de Luz Jornada Completa"),
    ("licencia médica", "Licencia Médica"),
    ("vacacion", "Vacaciones"),
    ("descanso", "Libre"),
    ("día libre", "Libre"),
    ("dia libre", "Libre"),
    ("no se presenta", "Ausencia"),
    ("internet", "Sin Internet Jornada Completa"),
    ("sin internet", "Sin Internet Jornada Completa"),
]

DEFAULT_CATEGORY = "Presente"

# -------------------------------
# Mapeo (TABLA 2) SOLO estas 5 categorías de minutos
# -------------------------------
MINUTES_CATS = [
    "Capacitación",
    "Permiso con Descuento",
    "Problemas Equipo",
    "Problemas Internet",
    "Vive tu momentos",
]

# ---------- utilidades de nombres ----------
def strip_accents(s: str) -> str:
    if not isinstance(s, str):
        return ""
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in nfkd if not unicodedata.combining(ch))

def normalize_name(s: str) -> str:
    s = strip_accents(str(s)).lower().strip()
    s = "".join(ch if (ch.isalnum() or ch.isspace()) else " " for ch in s)
    s = " ".join(s.split())
    return s

def name_tokens(s: str):
    return {t for t in normalize_name(s).split() if len(t) >= 2}

def best_match(agent: str, candidates: list[str]) -> str | None:
    norm_agent = normalize_name(agent)
    tokens_agent = name_tokens(agent)

    # exacto
    norm_map = {normalize_name(c): c for c in candidates}
    if norm_agent in norm_map:
        return norm_map[norm_agent]

    # subset de tokens
    for cand in candidates:
        t_c = name_tokens(cand)
        if not t_c:
            continue
        if tokens_agent.issubset(t_c) or t_c.issubset(tokens_agent):
            return cand

    # similitud
    from difflib import get_close_matches
    close = get_close_matches(norm_agent, list(norm_map.keys()), n=1, cutoff=0.82)
    if close:
        return norm_map[close[0]]
    return None

# ---------- lectura y normalización ----------
def normaliza_col(col: str) -> str:
    return str(col).strip().replace("\n", " ").replace("\r", " ").replace("  ", " ")

def leer_base(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path)
    df = df.rename(columns=lambda x: x.strip().lower())
    col_map = {
        "nombre del agente": "Nombre del agente",
        "nombre del código de actividad": "Nombre del código de actividad",
        "es pagado": "Es Pagado",
        "duración en minutos": "Duración en minutos",
        "fecha": "Fecha",
    }
    faltantes = [k for k in col_map if k not in df.columns]
    if faltantes:
        raise ValueError(f"Faltan columnas requeridas: {faltantes}. Disponibles: {list(df.columns)}")
    df = df.rename(columns={k: v for k, v in col_map.items()})
    df["Fecha"] = pd.to_datetime(df["Fecha"]).dt.date
    df["Duración en minutos"] = pd.to_numeric(df["Duración en minutos"], errors="coerce").fillna(0)
    df["Nombre del agente"] = df["Nombre del agente"].astype(str).map(normaliza_col)
    df["Nombre del código de actividad"] = df["Nombre del código de actividad"].astype(str).map(normaliza_col)
    return df

def leer_dotacion(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path)
    cols = {c.lower(): c for c in df.columns}
    rut_col = cols.get("rut", df.columns[0])
    agente_col = cols.get("agente", df.columns[1] if len(df.columns) > 1 else df.columns[0])
    jornada_col = cols.get("jornada", df.columns[2] if len(df.columns) > 2 else None)
    area_col = cols.get("area", df.columns[3] if len(df.columns) > 3 else None)

    sel = [rut_col, agente_col]
    if jornada_col: sel.append(jornada_col)
    if area_col: sel.append(area_col)
    df = df[sel].rename(columns={
        rut_col: "RUT",
        agente_col: "AGENTE",
        jornada_col: "JORNADA" if jornada_col else "JORNADA",
        area_col: "AREA" if area_col else "AREA",
    })
    df["AGENTE"] = df["AGENTE"].astype(str).map(normaliza_col)
    for c in ["RUT", "JORNADA", "AREA"]:
        if c in df.columns:
            df[c] = df[c].astype(str).map(normaliza_col)
        else:
            df[c] = pd.NA
    return df

def enriquecer_con_dotacion(df_agents: pd.DataFrame, dot_df: pd.DataFrame) -> pd.DataFrame:
    names_dot = dot_df["AGENTE"].tolist()
    rows = []
    for agent in df_agents["Nombre del agente"]:
        best = best_match(agent, names_dot)
        if best is None:
            rows.append({"Nombre del agente": agent, "RUT": pd.NA, "JORNADA": pd.NA, "AREA": pd.NA})
        else:
            row = dot_df.loc[dot_df["AGENTE"] == best].iloc[0]
            rows.append({
                "Nombre del agente": agent,
                "RUT": row.get("RUT", pd.NA),
                "JORNADA": row.get("JORNADA", pd.NA),
                "AREA": row.get("AREA", pd.NA),
            })
    return pd.DataFrame(rows)

# ---------- clasificaciones ----------
def clasifica_categoria_tabla1(nombre_actividad: str) -> str:
    if not isinstance(nombre_actividad, str):
        return DEFAULT_CATEGORY
    s = nombre_actividad.strip().lower()
    if s in CATEGORIES_MAP_EXACT:
        return CATEGORIES_MAP_EXACT[s]
    for needle, target_cat in REGLAS_CONTAINS:
        if needle in s:
            return target_cat
    return DEFAULT_CATEGORY

def clasifica_categoria_minutos(nombre_actividad: str):
    if not isinstance(nombre_actividad, str):
        return None
    s = nombre_actividad.strip().lower()
    if "capacita" in s:
        return "Capacitación"
    if "permiso con descuento" in s:
        return "Permiso con Descuento"
    if "internet" in s or "sin internet" in s:
        return "Problemas Internet"
    if any(k in s for k in ["equipo", "sin equipo", "sin equipos", "bloqueo", "reseteo", "cuenta"]):
        return "Problemas Equipo"
    if "vive tu momentos" in s:
        return "Vive tu momentos"
    return None

# ---------- tablas ----------
def construir_resumen_categoria(df: pd.DataFrame):
    df["Categoría"] = df["Nombre del código de actividad"].apply(clasifica_categoria_tabla1)
    df_sorted = df.sort_values(["Nombre del agente", "Fecha", "Duración en minutos"],
                               ascending=[True, True, False])
    top_by_day = df_sorted.groupby(["Nombre del agente", "Fecha"], as_index=False).first()

    pivot = (top_by_day.pivot_table(index="Nombre del agente",
                                    columns="Categoría",
                                    values="Fecha",
                                    aggfunc="count",
                                    fill_value=0)
             .reindex(columns=CATEGORIES_ORDER, fill_value=0)
             .astype("Int64"))
    pivot["Total general"] = pivot.sum(axis=1)
    total_row = pd.DataFrame([pivot.sum(numeric_only=True)], index=["TOTAL"])
    resumen = pd.concat([pivot, total_row], axis=0)
    resumen = resumen.replace(0, pd.NA)  # 0 -> NA
    resumen.index.name = "Nombre del agente"  # <- FIX
    return resumen, top_by_day

def construir_resumen_minutos(df: pd.DataFrame):
    df2 = df.copy()
    df2["CatMin"] = df2["Nombre del código de actividad"].apply(clasifica_categoria_minutos)
    df2 = df2[df2["CatMin"].notna()]
    if df2.empty:
        empty = pd.DataFrame(columns=["Nombre del agente"] + MINUTES_CATS + ["Total Minutos", "Total Horas"])
        empty.set_index("Nombre del agente", inplace=True)
        empty.index.name = "Nombre del agente"
        return empty

    pivot_min = (df2.pivot_table(index="Nombre del agente",
                                 columns="CatMin",
                                 values="Duración en minutos",
                                 aggfunc="sum",
                                 fill_value=0)
                 .reindex(columns=MINUTES_CATS, fill_value=0)
                 .astype("Int64"))
    pivot_min["Total Minutos"] = pivot_min.sum(axis=1)
    pivot_min["Total Horas"] = (pivot_min["Total Minutos"] / 60).round(2)
    # solo agentes con datos en las 5 categorías
    mask_has_data = pivot_min[MINUTES_CATS].sum(axis=1) > 0
    pivot_min = pivot_min[mask_has_data]
    if not pivot_min.empty:
        total_row = pd.DataFrame([pivot_min.sum(numeric_only=True)], index=["TOTAL"])
        total_row.loc["TOTAL", "Total Horas"] = round(total_row.loc["TOTAL", "Total Minutos"] / 60.0, 2)
        resumen_min = pd.concat([pivot_min, total_row], axis=0)
    else:
        resumen_min = pivot_min
    resumen_min = resumen_min.replace(0, pd.NA)
    resumen_min.index.name = "Nombre del agente"  # <- FIX
    return resumen_min

# ---------- export ----------
def main():
    print(f"Leyendo base: {INPUT_FILE}")
    df = leer_base(INPUT_FILE)
    print(f"Leyendo dotación: {DOTACION_FILE}")
    dot_df = leer_dotacion(DOTACION_FILE)

    resumen_cat, detalle_top_dia = construir_resumen_categoria(df)
    resumen_min = construir_resumen_minutos(df)

    # Enriquecer con RUT/JORNADA/AREA (sin TOTAL)
    agentes_cat = pd.DataFrame({"Nombre del agente": [i for i in resumen_cat.index if i != "TOTAL"]})
    meta_cat = enriquecer_con_dotacion(agentes_cat, dot_df)

    agentes_min = pd.DataFrame({"Nombre del agente": [i for i in resumen_min.index if i != "TOTAL"]})
    meta_min = (enriquecer_con_dotacion(agentes_min, dot_df)
                if not agentes_min.empty else
                pd.DataFrame(columns=["Nombre del agente", "RUT", "JORNADA", "AREA"]))

    # Resumen (categorías)
    resumen_cat_out = resumen_cat.reset_index().merge(meta_cat, on="Nombre del agente", how="left")
    first_cols = ["Nombre del agente", "RUT", "JORNADA", "AREA"]
    other_cols = [c for c in resumen_cat_out.columns if c not in first_cols]
    resumen_cat_out = resumen_cat_out[first_cols + other_cols]
    total_row = resumen_cat_out[resumen_cat_out["Nombre del agente"] == "TOTAL"]
    resumen_cat_out = pd.concat([resumen_cat_out[resumen_cat_out["Nombre del agente"] != "TOTAL"], total_row], axis=0)

    # Resumen_minutos
    resumen_min_out = resumen_min.reset_index()
    if not resumen_min_out.empty:
        resumen_min_out = resumen_min_out.merge(meta_min, on="Nombre del agente", how="left")
        first_cols2 = ["Nombre del agente", "RUT", "JORNADA", "AREA"]
        other_cols2 = [c for c in resumen_min_out.columns if c not in first_cols2]
        resumen_min_out = resumen_min_out[first_cols2 + other_cols2]
        total_row2 = resumen_min_out[resumen_min_out["Nombre del agente"] == "TOTAL"]
        resumen_min_out = pd.concat(
            [resumen_min_out[resumen_min_out["Nombre del agente"] != "TOTAL"], total_row2], axis=0
        )

    # Exportar con na_rep="" (no usamos ws.write celda a celda)
    with pd.ExcelWriter(OUTPUT_FILE, engine="xlsxwriter") as writer:
        resumen_cat_out.to_excel(writer, sheet_name="Resumen", index=False, na_rep="")
        resumen_min_out.to_excel(writer, sheet_name="Resumen_minutos", index=False, na_rep="")
        detalle_top_dia.to_excel(writer, sheet_name="Detalle_top_por_día", index=False)

        # Autoancho básico
        for sheet_name, data in {
            "Resumen": resumen_cat_out,
            "Resumen_minutos": resumen_min_out if not resumen_min_out.empty else pd.DataFrame(columns=["Nombre del agente"]),
            "Detalle_top_por_día": detalle_top_dia,
        }.items():
            ws = writer.sheets[sheet_name]
            for i, col in enumerate(list(data.columns)):
                ws.set_column(i, i, max(12, len(str(col)) + 2))

    print(f"✅ Archivo generado: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
