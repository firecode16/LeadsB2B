#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════╗
║       Script 3: setup_postgresql.py — Base de Datos de Leads B2B        ║
║       Nicho: Salud Mental / Psicólogos / Clínicas — CDMX                ║
╠══════════════════════════════════════════════════════════════════════════╣
║  Funciones:                                                             ║
║    1. setup    → Crea tablas, índices y vistas en PostgreSQL            ║
║    2. importar → Inserta leads_verificados.json (upsert por teléfono)   ║
║    3. exportar → Genera CSV listo para CRM desde PostgreSQL             ║
║    4. exportar-json → Genera CSV directamente desde JSON (sin BD)       ║
║    5. stats    → Resumen de la base de datos por nicho/fuente/estado    ║
║    6. limpiar  → Elimina duplicados y registros vacíos                  ║
╠══════════════════════════════════════════════════════════════════════════╣
║  Uso:                                                                    ║
║    python setup_postgresql.py setup                                     ║
║    python setup_postgresql.py importar                                  ║
║    python setup_postgresql.py importar --input leads_verificados.json   ║
║    python setup_postgresql.py exportar --output leads_crm.csv           ║
║    python setup_postgresql.py exportar --todos                          ║
║    python setup_postgresql.py exportar-json --input leads_verificados.json ║
║    python setup_postgresql.py stats                                     ║
║    python setup_postgresql.py limpiar                                   ║
╠══════════════════════════════════════════════════════════════════════════╣
║  Variables de entorno (o .env):                                         ║
║    DB_HOST     → localhost                                              ║
║    DB_PORT     → 5432                                                   ║
║    DB_NAME     → leads_b2b                                              ║
║    DB_USER     → postgres                                               ║
║    DB_PASSWORD → tu_password                                            ║
╚══════════════════════════════════════════════════════════════════════════╝

Dependencias:
    pip install psycopg2-binary python-dotenv
"""

import argparse
import csv
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("postgresql")


# ════════════════════════════════════════════════════════════════════════════
# 0. CONFIGURACIÓN
# ════════════════════════════════════════════════════════════════════════════

DEFAULT_INPUT  = "leads_verificados.json"
DEFAULT_OUTPUT = f"leads_crm_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"

# Columnas del CSV final (orden pensado para importar a CRM)
COLUMNAS_CSV = [
    "id", "nicho", "empresa", "nombre_contacto", "cargo",
    "telefono", "email", "delegacion", "colonia", "sitio_web",
    "linkedin", "fuente", "whatsapp_valido", "fecha_verificacion",
    "crm_estado", "expo_id",
]


def get_db_config() -> dict:
    return {
        "host":     os.getenv("DB_HOST",     "localhost"),
        "port":     int(os.getenv("DB_PORT", "5432")),
        "dbname":   os.getenv("DB_NAME",     "leads_b2b"),
        "user":     os.getenv("DB_USER",     "postgres"),
        "password": os.getenv("DB_PASSWORD", ""),
    }


def conectar():
    """Abre y devuelve una conexión a PostgreSQL."""
    try:
        import psycopg2
        import psycopg2.extras
    except ImportError:
        log.error("❌  psycopg2 no instalado: pip install psycopg2-binary")
        sys.exit(1)

    cfg = get_db_config()
    try:
        conn = psycopg2.connect(**cfg)
        conn.autocommit = False
        return conn
    except Exception as exc:
        log.error(f"❌  No se pudo conectar a PostgreSQL: {exc}")
        log.error(f"   Config: host={cfg['host']} port={cfg['port']} db={cfg['dbname']} user={cfg['user']}")
        log.error("   Verifica las variables DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD")
        sys.exit(1)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Gestión de base de datos PostgreSQL para leads B2B",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = p.add_subparsers(dest="comando", required=True)

    # setup
    sub.add_parser("setup", help="Crear tablas, índices y vistas (idempotente)")

    # importar
    imp = sub.add_parser("importar", help="Insertar leads desde JSON verificado")
    imp.add_argument("--input",  default=DEFAULT_INPUT,
                     help=f"Archivo JSON de entrada (default: {DEFAULT_INPUT})")
    imp.add_argument("--nicho",  default="salud_mental",
                     help="Identificador del nicho (default: salud_mental)")
    imp.add_argument("--expo",   default=None,
                     help="ID de la expo/campaña (opcional)")

    # exportar (desde BD)
    exp = sub.add_parser("exportar", help="Exportar leads a CSV para CRM desde PostgreSQL")
    exp.add_argument("--output", default=DEFAULT_OUTPUT,
                     help=f"Archivo CSV de salida (default: con timestamp)")
    exp.add_argument("--todos",  action="store_true",
                     help="Exportar todos los leads (no solo WhatsApp válidos)")
    exp.add_argument("--nicho",  default=None,
                     help="Filtrar por nicho específico")
    exp.add_argument("--expo",   default=None,
                     help="Filtrar por expo/campaña específica")

    # exportar-json (desde archivo JSON)
    exp_json = sub.add_parser("exportar-json", help="Exportar leads a CSV directamente desde JSON (sin BD)")
    exp_json.add_argument("--input",  default=DEFAULT_INPUT,
                          help=f"Archivo JSON de entrada (default: {DEFAULT_INPUT})")
    exp_json.add_argument("--output", default=DEFAULT_OUTPUT,
                          help=f"Archivo CSV de salida (default: con timestamp)")
    exp_json.add_argument("--todos",  action="store_true",
                          help="Incluir todos los leads (no solo WhatsApp válidos)")
    exp_json.add_argument("--nicho",  default=None,
                          help="Filtrar por nicho específico")
    exp_json.add_argument("--expo",   default=None,
                          help="Filtrar por expo/campaña específica")

    # stats
    sub.add_parser("stats", help="Ver estadísticas de la base de datos")

    # limpiar
    sub.add_parser("limpiar", help="Eliminar duplicados y registros inválidos")

    p.add_argument("--debug", action="store_true", help="Logging detallado")
    return p.parse_args()


# ════════════════════════════════════════════════════════════════════════════
# 1. SETUP — Crear estructura de la base de datos
# ════════════════════════════════════════════════════════════════════════════

SQL_SETUP = """
-- ─────────────────────────────────────────────────────────────────────────
-- Extensiones
-- ─────────────────────────────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS "unaccent";   -- búsquedas sin acentos
CREATE EXTENSION IF NOT EXISTS "pg_trgm";    -- búsqueda full-text aproximada

-- ─────────────────────────────────────────────────────────────────────────
-- Tabla principal de leads
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS leads (
    -- Identificadores
    id                  SERIAL          PRIMARY KEY,
    nicho               VARCHAR(80)     NOT NULL DEFAULT 'salud_mental',
    expo_id             VARCHAR(80),

    -- Datos de la empresa
    empresa             TEXT,
    sitio_web           TEXT,
    colonia             TEXT,
    delegacion          TEXT,

    -- Datos de contacto
    telefono            VARCHAR(25)     UNIQUE,          -- E.164 +52XXXXXXXXXX
    email               TEXT,
    nombre_contacto     TEXT,
    cargo               TEXT,
    linkedin            TEXT,

    -- Verificación WhatsApp
    whatsapp_valido     BOOLEAN         DEFAULT NULL,    -- NULL = no verificado
    whatsapp_estado     VARCHAR(30),                     -- valido | invalido | sin_telefono | pendiente
    fecha_verificacion  DATE,

    -- Metadatos
    fuente              VARCHAR(40)     NOT NULL DEFAULT 'Desconocida',
    fecha_extraccion    DATE,
    fecha_importacion   TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    fecha_actualizacion TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    -- CRM tracking
    crm_estado          VARCHAR(30)     DEFAULT 'nuevo',     -- nuevo | contactado | interesado | descartado
    crm_notas           TEXT,
    crm_fecha_contacto  DATE
);

-- ─────────────────────────────────────────────────────────────────────────
-- Índices para consultas frecuentes
-- ─────────────────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_leads_nicho
    ON leads (nicho);

CREATE INDEX IF NOT EXISTS idx_leads_whatsapp
    ON leads (whatsapp_valido)
    WHERE whatsapp_valido = TRUE;

CREATE INDEX IF NOT EXISTS idx_leads_fuente
    ON leads (fuente);

CREATE INDEX IF NOT EXISTS idx_leads_delegacion
    ON leads (delegacion);

CREATE INDEX IF NOT EXISTS idx_leads_crm_estado
    ON leads (crm_estado);

CREATE INDEX IF NOT EXISTS idx_leads_empresa_trgm
    ON leads USING GIN (empresa gin_trgm_ops);

-- ─────────────────────────────────────────────────────────────────────────
-- Trigger: actualizar fecha_actualizacion automáticamente
-- ─────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION actualizar_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.fecha_actualizacion = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_leads_timestamp ON leads;
CREATE TRIGGER trg_leads_timestamp
    BEFORE UPDATE ON leads
    FOR EACH ROW EXECUTE FUNCTION actualizar_timestamp();

-- ─────────────────────────────────────────────────────────────────────────
-- Vista: leads listos para CRM (WhatsApp válido + datos mínimos)
-- ─────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_leads_crm AS
SELECT
    id,
    nicho,
    expo_id,
    empresa,
    COALESCE(nombre_contacto, '—')   AS nombre_contacto,
    COALESCE(cargo, '—')             AS cargo,
    telefono,
    COALESCE(email, '—')             AS email,
    COALESCE(delegacion, '—')        AS delegacion,
    COALESCE(colonia, '—')           AS colonia,
    sitio_web,
    fuente,
    crm_estado,
    fecha_verificacion
FROM leads
WHERE whatsapp_valido = TRUE
  AND telefono IS NOT NULL
ORDER BY nicho, delegacion, empresa;

-- ─────────────────────────────────────────────────────────────────────────
-- Vista: resumen estadístico por nicho y fuente
-- ─────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_stats AS
SELECT
    nicho,
    fuente,
    COUNT(*)                                                      AS total,
    COUNT(*) FILTER (WHERE whatsapp_valido = TRUE)                AS whatsapp_validos,
    COUNT(*) FILTER (WHERE whatsapp_valido = FALSE)               AS whatsapp_invalidos,
    COUNT(*) FILTER (WHERE whatsapp_valido IS NULL)               AS sin_verificar,
    COUNT(*) FILTER (WHERE email IS NOT NULL)                     AS con_email,
    COUNT(*) FILTER (WHERE nombre_contacto IS NOT NULL)           AS con_contacto,
    ROUND(
        COUNT(*) FILTER (WHERE whatsapp_valido = TRUE)::NUMERIC
        / NULLIF(COUNT(*) FILTER (WHERE whatsapp_valido IS NOT NULL), 0) * 100, 1
    )                                                             AS tasa_whatsapp_pct
FROM leads
GROUP BY nicho, fuente
ORDER BY nicho, total DESC;

-- ─────────────────────────────────────────────────────────────────────────
-- Tabla de log de ejecuciones (para n8n)
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ejecuciones (
    id              SERIAL          PRIMARY KEY,
    tipo            VARCHAR(30)     NOT NULL,   -- extraccion | verificacion | importacion | exportacion
    nicho           VARCHAR(80),
    archivo         TEXT,
    total_procesados INTEGER        DEFAULT 0,
    total_insertados INTEGER        DEFAULT 0,
    total_errores    INTEGER        DEFAULT 0,
    duracion_seg     NUMERIC(10,2),
    fecha            TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    notas            TEXT
);
"""


def cmd_setup():
    log.info("🔧  Ejecutando setup de base de datos...")
    conn = conectar()
    cur  = conn.cursor()
    try:
        cur.execute(SQL_SETUP)
        conn.commit()
        log.info("   ✅  Tablas, índices, vistas y triggers creados (o ya existían).")
        log.info("   Objetos creados:")
        log.info("     • Tabla   : leads")
        log.info("     • Tabla   : ejecuciones")
        log.info("     • Vista   : v_leads_crm")
        log.info("     • Vista   : v_stats")
        log.info("     • Trigger : trg_leads_timestamp")
        log.info("     • Índices : 6 índices de consulta")
    except Exception as exc:
        conn.rollback()
        log.error(f"❌  Error en setup: {exc}")
        sys.exit(1)
    finally:
        cur.close()
        conn.close()


# ════════════════════════════════════════════════════════════════════════════
# 2. IMPORTAR — Insertar leads desde JSON verificado
# ════════════════════════════════════════════════════════════════════════════

def cmd_importar(input_path: str, nicho: str, expo_id: str | None):
    log.info(f"📥  Importando leads desde: {input_path}")
    log.info(f"   Nicho   : {nicho}")
    log.info(f"   Expo ID : {expo_id or '(ninguno)'}")

    # Cargar JSON
    path = Path(input_path)
    if not path.exists():
        log.error(f"❌  Archivo no encontrado: {input_path}")
        sys.exit(1)

    raw = json.loads(path.read_text(encoding="utf-8"))
    leads = raw["leads"] if isinstance(raw, dict) and "leads" in raw else raw

    if not leads:
        log.warning("⚠️  El archivo no contiene leads.")
        return

    log.info(f"   {len(leads)} leads en el archivo.")

    conn = conectar()
    import psycopg2.extras

    cur          = conn.cursor()
    insertados   = 0
    actualizados = 0
    omitidos     = 0
    errores      = 0
    inicio       = datetime.now()

    # SQL de upsert: si el teléfono ya existe, actualizar los campos vacíos
    SQL_UPSERT = """
        INSERT INTO leads (
            nicho, expo_id, empresa, sitio_web, colonia, delegacion,
            telefono, email, nombre_contacto, cargo, linkedin,
            whatsapp_valido, whatsapp_estado, fecha_verificacion,
            fuente, fecha_extraccion
        ) VALUES (
            %(nicho)s, %(expo_id)s, %(empresa)s, %(sitio_web)s,
            %(colonia)s, %(delegacion)s, %(telefono)s, %(email)s,
            %(nombre_contacto)s, %(cargo)s, %(linkedin)s,
            %(whatsapp_valido)s, %(whatsapp_estado)s, %(fecha_verificacion)s,
            %(fuente)s, %(fecha_extraccion)s
        )
        ON CONFLICT (telefono) DO UPDATE SET
            -- Solo actualizar si el campo existente está vacío (no sobreescribir datos buenos)
            empresa             = COALESCE(NULLIF(leads.empresa, ''),           EXCLUDED.empresa),
            email               = COALESCE(leads.email,                         EXCLUDED.email),
            nombre_contacto     = COALESCE(leads.nombre_contacto,               EXCLUDED.nombre_contacto),
            cargo               = COALESCE(leads.cargo,                         EXCLUDED.cargo),
            sitio_web           = COALESCE(leads.sitio_web,                     EXCLUDED.sitio_web),
            linkedin            = COALESCE(leads.linkedin,                      EXCLUDED.linkedin),
            colonia             = COALESCE(leads.colonia,                       EXCLUDED.colonia),
            delegacion          = COALESCE(leads.delegacion,                    EXCLUDED.delegacion),
            -- Siempre actualizar verificación (puede haberse re-verificado)
            whatsapp_valido     = EXCLUDED.whatsapp_valido,
            whatsapp_estado     = EXCLUDED.whatsapp_estado,
            fecha_verificacion  = EXCLUDED.fecha_verificacion,
            fecha_actualizacion = NOW()
        RETURNING (xmax = 0) AS es_insercion  -- TRUE=insert, FALSE=update
    """

    try:
        for lead in leads:
            # Omitir leads completamente vacíos
            if not lead.get("empresa") and not lead.get("telefono"):
                omitidos += 1
                continue

            params = {
                "nicho":              nicho,
                "expo_id":            expo_id,
                "empresa":            (lead.get("empresa") or "").strip() or None,
                "sitio_web":          lead.get("sitio_web"),
                "colonia":            lead.get("colonia"),
                "delegacion":         lead.get("delegacion"),
                "telefono":           lead.get("telefono"),
                "email":              (lead.get("email") or "").strip() or None,
                "nombre_contacto":    lead.get("nombre_contacto"),
                "cargo":              lead.get("cargo"),
                "linkedin":           lead.get("linkedin"),
                "whatsapp_valido":    lead.get("whatsapp_valido"),
                "whatsapp_estado":    lead.get("whatsapp_estado", "pendiente"),
                "fecha_verificacion": lead.get("fecha_verificacion"),
                "fuente":             lead.get("fuente", "Desconocida"),
                "fecha_extraccion":   lead.get("fecha_extraccion"),
            }

            try:
                cur.execute(SQL_UPSERT, params)
                row = cur.fetchone()
                if row and row[0]:
                    insertados += 1
                else:
                    actualizados += 1
            except Exception as exc:
                log.debug(f"   Error en lead '{lead.get('empresa')}': {exc}")
                conn.rollback()
                # Re-abrir cursor tras rollback parcial
                cur = conn.cursor()
                errores += 1
                continue

        conn.commit()

    except KeyboardInterrupt:
        conn.rollback()
        log.warning("   ⚠️  Importación interrumpida. Cambios revertidos.")
        sys.exit(1)
    finally:
        cur.close()

    duracion = (datetime.now() - inicio).total_seconds()

    # Registrar en log de ejecuciones
    _registrar_ejecucion(conn, {
        "tipo":             "importacion",
        "nicho":            nicho,
        "archivo":          input_path,
        "total_procesados": len(leads),
        "total_insertados": insertados + actualizados,
        "total_errores":    errores,
        "duracion_seg":     duracion,
        "notas":            f"nuevos={insertados} actualizados={actualizados} omitidos={omitidos}",
    })
    conn.close()

    print("\n" + "═" * 52)
    print("  📥  RESUMEN DE IMPORTACIÓN")
    print("═" * 52)
    print(f"  Leads en archivo       : {len(leads)}")
    print(f"  ✅  Insertados (nuevos) : {insertados}")
    print(f"  🔄  Actualizados        : {actualizados}")
    print(f"  ⏭️   Omitidos (vacíos)  : {omitidos}")
    print(f"  ❌  Errores             : {errores}")
    print(f"  ⏱️   Duración            : {duracion:.1f}s")
    print("═" * 52 + "\n")


# ════════════════════════════════════════════════════════════════════════════
# 3. EXPORTAR — Generar CSV para CRM (desde PostgreSQL)
# ════════════════════════════════════════════════════════════════════════════

def cmd_exportar(output_path: str, solo_validos: bool, nicho: str | None, expo_id: str | None):
    log.info(f"📤  Exportando leads desde PostgreSQL a CSV: {output_path}")

    conn = conectar()
    cur  = conn.cursor()

    # Construir query con filtros opcionales
    condiciones = []
    params      = []

    if solo_validos:
        condiciones.append("whatsapp_valido = TRUE")
    if nicho:
        condiciones.append("nicho = %s")
        params.append(nicho)
    if expo_id:
        condiciones.append("expo_id = %s")
        params.append(expo_id)

    where = ("WHERE " + " AND ".join(condiciones)) if condiciones else ""

    cols_sql = ", ".join(COLUMNAS_CSV)
    SQL = f"""
        SELECT {cols_sql}
        FROM leads
        {where}
        ORDER BY nicho, delegacion, empresa
    """

    try:
        cur.execute(SQL, params)
        filas = cur.fetchall()
    except Exception as exc:
        log.error(f"❌  Error al consultar leads: {exc}")
        sys.exit(1)
    finally:
        cur.close()
        conn.close()

    if not filas:
        log.warning("⚠️  No se encontraron leads con los filtros aplicados.")
        return

    # Escribir CSV
    path = Path(output_path)
    with path.open("w", newline="", encoding="utf-8-sig") as f:  # utf-8-sig para Excel en Windows
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(COLUMNAS_CSV)  # encabezados
        writer.writerows(filas)

    con_wa = sum(1 for r in filas if r[COLUMNAS_CSV.index("whatsapp_valido")] is True)

    print("\n" + "═" * 52)
    print("  📤  RESUMEN DE EXPORTACIÓN (desde PostgreSQL)")
    print("═" * 52)
    print(f"  Total leads exportados : {len(filas)}")
    print(f"  Con WhatsApp válido    : {con_wa}")
    print(f"  Archivo CSV            : {output_path}")
    print(f"  Encoding               : UTF-8 BOM (compatible Excel)")
    print("═" * 52)
    print("  ✅  Listo para importar a HubSpot, Pipedrive, Notion, etc.")
    print("═" * 52 + "\n")


# ════════════════════════════════════════════════════════════════════════════
# 4. EXPORTAR DESDE JSON (nuevo comando)
# ════════════════════════════════════════════════════════════════════════════

def cmd_exportar_json(input_path: str, output_path: str, solo_validos: bool, nicho: str | None, expo_id: str | None):
    log.info(f"📤  Exportando leads desde JSON a CSV: {input_path} -> {output_path}")

    # Cargar JSON
    path = Path(input_path)
    if not path.exists():
        log.error(f"❌  Archivo no encontrado: {input_path}")
        sys.exit(1)

    raw = json.loads(path.read_text(encoding="utf-8"))
    leads = raw["leads"] if isinstance(raw, dict) and "leads" in raw else raw

    if not leads:
        log.warning("⚠️  El archivo no contiene leads.")
        return

    log.info(f"   {len(leads)} leads en el archivo.")

    # Aplicar filtros
    filtrados = []
    for lead in leads:
        # Filtro por WhatsApp válido (a menos que se pida --todos)
        if solo_validos and not lead.get("whatsapp_valido"):
            continue
        # Filtro por nicho
        if nicho and lead.get("nicho") != nicho:
            continue
        # Filtro por expo_id (si existe en el lead, aunque normalmente no está en JSON)
        if expo_id and lead.get("expo_id") != expo_id:
            continue
        filtrados.append(lead)

    if not filtrados:
        log.warning("⚠️  No hay leads después de aplicar los filtros.")
        return

    # Preparar filas para CSV (en el mismo orden que COLUMNAS_CSV)
    filas = []
    for lead in filtrados:
        fila = []
        for col in COLUMNAS_CSV:
            valor = lead.get(col)
            # Convertir booleanos a string para que CSV los muestre como true/false
            if isinstance(valor, bool):
                valor = str(valor)
            elif valor is None:
                valor = ""
            fila.append(valor)
        filas.append(fila)

    # Escribir CSV
    out_path = Path(output_path)
    with out_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(COLUMNAS_CSV)
        writer.writerows(filas)

    con_wa = sum(1 for l in filtrados if l.get("whatsapp_valido") is True)

    print("\n" + "═" * 52)
    print("  📤  RESUMEN DE EXPORTACIÓN (desde JSON)")
    print("═" * 52)
    print(f"  Leads en archivo JSON  : {len(leads)}")
    print(f"  Leads después de filtros: {len(filtrados)}")
    print(f"  Con WhatsApp válido    : {con_wa}")
    print(f"  Archivo CSV            : {output_path}")
    print(f"  Encoding               : UTF-8 BOM (compatible Excel)")
    print("═" * 52)
    print("  ✅  Listo para importar a CRM")
    print("═" * 52 + "\n")


# ════════════════════════════════════════════════════════════════════════════
# 5. STATS — Resumen de la base de datos
# ════════════════════════════════════════════════════════════════════════════

def cmd_stats():
    log.info("📊  Consultando estadísticas...")
    conn = conectar()
    cur  = conn.cursor()

    try:
        # Total global
        cur.execute("SELECT COUNT(*) FROM leads")
        total = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM leads WHERE whatsapp_valido = TRUE")
        wa_validos = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM leads WHERE whatsapp_valido IS NULL")
        sin_verificar = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM leads WHERE email IS NOT NULL")
        con_email = cur.fetchone()[0]

        # Stats por nicho y fuente (desde vista)
        cur.execute("SELECT * FROM v_stats")
        stats_rows = cur.fetchall()
        stats_cols = [d[0] for d in cur.description]

        # Distribución por delegación (top 10)
        cur.execute("""
            SELECT delegacion, COUNT(*) as total,
                   COUNT(*) FILTER (WHERE whatsapp_valido=TRUE) as wa_validos
            FROM leads
            WHERE delegacion IS NOT NULL
            GROUP BY delegacion
            ORDER BY total DESC
            LIMIT 10
        """)
        por_deleg = cur.fetchall()

        # Últimas ejecuciones
        cur.execute("""
            SELECT tipo, nicho, total_procesados, total_insertados, duracion_seg, fecha
            FROM ejecuciones
            ORDER BY fecha DESC
            LIMIT 5
        """)
        ultimas = cur.fetchall()

    finally:
        cur.close()
        conn.close()

    tasa = wa_validos * 100 // max(total - sin_verificar, 1) if total > 0 else 0

    print("\n" + "═" * 62)
    print("  📊  ESTADÍSTICAS DE BASE DE DATOS — leads_b2b")
    print("═" * 62)
    print(f"  Total leads             : {total:>6}")
    print(f"  WhatsApp válidos        : {wa_validos:>6}  ({tasa}%)")
    print(f"  Sin verificar           : {sin_verificar:>6}")
    print(f"  Con email               : {con_email:>6}")
    print()

    if stats_rows:
        print(f"  {'NICHO':<20} {'FUENTE':<22} {'TOTAL':>6} {'✅WA':>5} {'%':>4} {'EMAIL':>6}")
        print("  " + "─" * 60)
        for row in stats_rows:
            d = dict(zip(stats_cols, row))
            pct = d.get("tasa_whatsapp_pct") or 0
            print(
                f"  {str(d['nicho']):<20} {str(d['fuente']):<22} "
                f"{d['total']:>6} {d['whatsapp_validos']:>5} "
                f"{pct:>3.0f}% {d['con_email']:>6}"
            )
        print()

    if por_deleg:
        print(f"  TOP DELEGACIONES/ALCALDÍAS")
        print("  " + "─" * 40)
        for deleg, tot, wa in por_deleg:
            bar = "█" * min(int(tot / max(total, 1) * 40), 20)
            print(f"  {str(deleg):<28} {tot:>4}  WA:{wa:>3}  {bar}")
        print()

    if ultimas:
        print(f"  ÚLTIMAS EJECUCIONES")
        print("  " + "─" * 55)
        for tipo, nicho, procesados, insertados, dur, fecha in ultimas:
            fecha_str = fecha.strftime("%d/%m %H:%M") if fecha else "—"
            print(f"  {fecha_str}  {str(tipo):<15} {str(nicho or ''):<18} +{insertados or 0}/{procesados or 0}")

    print("═" * 62 + "\n")


# ════════════════════════════════════════════════════════════════════════════
# 6. LIMPIAR — Deduplicar y sanear datos
# ════════════════════════════════════════════════════════════════════════════

def cmd_limpiar():
    log.info("🧹  Limpiando base de datos...")
    conn = conectar()
    cur  = conn.cursor()

    try:
        # 1. Eliminar leads sin empresa Y sin teléfono Y sin email
        cur.execute("""
            DELETE FROM leads
            WHERE empresa IS NULL
              AND telefono IS NULL
              AND email IS NULL
        """)
        vacios = cur.rowcount
        log.info(f"   Eliminados vacíos totales   : {vacios}")

        # 2. Normalizar empresa: trim y capitalizar
        cur.execute("""
            UPDATE leads
            SET empresa = INITCAP(TRIM(empresa))
            WHERE empresa IS NOT NULL
              AND empresa != INITCAP(TRIM(empresa))
        """)
        normalizados = cur.rowcount
        log.info(f"   Empresas normalizadas        : {normalizados}")

        # 3. Normalizar email a minúsculas
        cur.execute("""
            UPDATE leads
            SET email = LOWER(TRIM(email))
            WHERE email IS NOT NULL
              AND email != LOWER(TRIM(email))
        """)
        emails_norm = cur.rowcount
        log.info(f"   Emails normalizados          : {emails_norm}")

        # 4. Marcar como 'sin_telefono' los que whatsapp_estado es NULL y no tienen tel
        cur.execute("""
            UPDATE leads
            SET whatsapp_estado = 'sin_telefono',
                whatsapp_valido = FALSE
            WHERE telefono IS NULL
              AND (whatsapp_estado IS NULL OR whatsapp_estado != 'sin_telefono')
        """)
        sin_tel = cur.rowcount
        log.info(f"   Marcados sin_telefono        : {sin_tel}")

        conn.commit()
        log.info("   ✅  Limpieza completada.")

    except Exception as exc:
        conn.rollback()
        log.error(f"❌  Error durante limpieza: {exc}")
        sys.exit(1)
    finally:
        cur.close()
        conn.close()


# ════════════════════════════════════════════════════════════════════════════
# 7. UTILIDADES INTERNAS
# ════════════════════════════════════════════════════════════════════════════

def _registrar_ejecucion(conn, datos: dict):
    """Registra una ejecución en la tabla de log."""
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO ejecuciones
                (tipo, nicho, archivo, total_procesados, total_insertados, total_errores, duracion_seg, notas)
            VALUES
                (%(tipo)s, %(nicho)s, %(archivo)s, %(total_procesados)s,
                 %(total_insertados)s, %(total_errores)s, %(duracion_seg)s, %(notas)s)
        """, datos)
        conn.commit()
        cur.close()
    except Exception as exc:
        log.debug(f"   Error registrando ejecución: {exc}")


# ════════════════════════════════════════════════════════════════════════════
# PUNTO DE ENTRADA
# ════════════════════════════════════════════════════════════════════════════

def main():
    args = parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    if args.comando == "setup":
        cmd_setup()

    elif args.comando == "importar":
        cmd_importar(
            input_path=args.input,
            nicho=args.nicho,
            expo_id=args.expo,
        )

    elif args.comando == "exportar":
        cmd_exportar(
            output_path=args.output,
            solo_validos=not args.todos,
            nicho=args.nicho,
            expo_id=args.expo,
        )

    elif args.comando == "exportar-json":
        cmd_exportar_json(
            input_path=args.input,
            output_path=args.output,
            solo_validos=not args.todos,
            nicho=args.nicho,
            expo_id=args.expo,
        )

    elif args.comando == "stats":
        cmd_stats()

    elif args.comando == "limpiar":
        cmd_limpiar()


if __name__ == "__main__":
    main()