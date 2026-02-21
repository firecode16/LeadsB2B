#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════╗
║        Script 2: verificar_whatsapp.py — Verificador de WhatsApp B2B    ║
║        Nicho: Salud Mental / Psicólogos / Clínicas — CDMX               ║
╠══════════════════════════════════════════════════════════════════════════╣
║  Entrada  : leads_raw.json  (salida de extractor_hibrido.py)            ║
║  Salida   : leads_verificados.json  →  alimenta n8n / PostgreSQL        ║
╠══════════════════════════════════════════════════════════════════════════╣
║  Método   : WhatsApp Web automatizado con Playwright                    ║
║             → Verifica si un número tiene cuenta activa en WhatsApp     ║
╠══════════════════════════════════════════════════════════════════════════╣
║  Protecciones anti-baneo:                                               ║
║    • Máx 40 verificaciones / hora (configurable)                        ║
║    • Delays aleatorios entre verificaciones (5-10 seg default)          ║
║    • Perfil persistente en disco → QR solo una vez                      ║
║    • Checkpoint automático cada 10 leads                                ║
║    • Modo --mock para pruebas sin abrir WhatsApp                        ║
╠══════════════════════════════════════════════════════════════════════════╣
║  Uso:                                                                    ║
║    py -3.11 verificar_whatsapp.py --debug     ← primera vez (QR)       ║
║    py -3.11 verificar_whatsapp.py             ← producción (headless)  ║
║    py -3.11 verificar_whatsapp.py --mock      ← prueba sin WhatsApp    ║
║    py -3.11 verificar_whatsapp.py --reanudar  ← continúa checkpoint    ║
╚══════════════════════════════════════════════════════════════════════════╝

Dependencias:
    pip install playwright python-dotenv
    playwright install chromium

⚠️  IMPORTANTE: Usar cuenta WhatsApp desechable, no la personal.
"""

import argparse
import json
import logging
import os
import random
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

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
log = logging.getLogger("verificador")


# ════════════════════════════════════════════════════════════════════════════
# 0. CONFIGURACIÓN Y CLI
# ════════════════════════════════════════════════════════════════════════════

DEFAULT_INPUT      = "leads_raw.json"
DEFAULT_OUTPUT     = "leads_verificados.json"
DEFAULT_CHECKPOINT = "checkpoint_verificacion.json"
DEFAULT_MAX_HORA   = 40
DEFAULT_PAUSA_SEG  = 5
DEFAULT_PAUSA_MAX  = 10
DEFAULT_PROFILE    = "whatsapp_profile"  # carpeta para el perfil persistente


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Verificador de WhatsApp para leads B2B",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--input",     default=DEFAULT_INPUT,
                   help=f"Archivo JSON de entrada (default: {DEFAULT_INPUT})")
    p.add_argument("--output",    default=DEFAULT_OUTPUT,
                   help=f"Archivo JSON de salida (default: {DEFAULT_OUTPUT})")
    p.add_argument("--max-hora",  type=int, default=DEFAULT_MAX_HORA,
                   help=f"Máx verificaciones/hora anti-baneo (default: {DEFAULT_MAX_HORA})")
    p.add_argument("--pausa",     type=int, default=DEFAULT_PAUSA_SEG,
                   help=f"Pausa mínima entre verificaciones en seg (default: {DEFAULT_PAUSA_SEG})")
    p.add_argument("--pausa-max", type=int, default=DEFAULT_PAUSA_MAX,
                   help=f"Pausa máxima en seg (default: {DEFAULT_PAUSA_MAX})")
    p.add_argument("--profile",   default=DEFAULT_PROFILE,
                   help=f"Carpeta para el perfil persistente de Chrome (default: {DEFAULT_PROFILE})")
    p.add_argument("--mock",      action="store_true",
                   help="Modo prueba: sin abrir WhatsApp real")
    p.add_argument("--reanudar",  action="store_true",
                   help="Reanudar desde checkpoint anterior")
    p.add_argument("--debug",     action="store_true",
                   help="Navegador visible + logging detallado (necesario para escanear QR)")
    return p.parse_args()


# ════════════════════════════════════════════════════════════════════════════
# 1. CARGA Y GUARDADO DE DATOS
# ════════════════════════════════════════════════════════════════════════════

def cargar_leads(input_path: str) -> list:
    path = Path(input_path)
    if not path.exists():
        log.error(f"❌  Archivo no encontrado: {input_path}")
        log.error("   Asegúrate de correr primero: extractor_hibrido.py")
        sys.exit(1)
    raw = json.loads(path.read_text(encoding="utf-8"))
    return raw["leads"] if isinstance(raw, dict) and "leads" in raw else raw


def cargar_checkpoint(path: str) -> set:
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        tels = set(data.get("procesados", []))
        log.info(f"   📂  Checkpoint cargado: {len(tels)} teléfonos ya procesados.")
        return tels
    except Exception:
        return set()


def guardar_checkpoint(path: str, procesados: set):
    Path(path).write_text(
        json.dumps({"procesados": list(procesados)}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def guardar_resultados(leads: list, output_path: str):
    verificados = sum(1 for l in leads if l.get("whatsapp_valido") is not None)
    con_wa      = sum(1 for l in leads if l.get("whatsapp_valido") is True)
    output = {
        "metadata": {
            "total_leads":        len(leads),
            "total_verificados":  verificados,
            "whatsapp_validos":   con_wa,
            "whatsapp_invalidos": verificados - con_wa,
            "pendientes":         len(leads) - verificados,
            "tasa_validacion":    f"{con_wa * 100 // max(verificados, 1)}%",
            "fecha_verificacion": datetime.now().isoformat(),
            "version_script":     "2.1.0",
        },
        "leads": leads,
    }
    Path(output_path).write_text(
        json.dumps(output, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


# ════════════════════════════════════════════════════════════════════════════
# 2. VERIFICADOR PRINCIPAL — WhatsApp Web vía Playwright con perfil persistente
# ════════════════════════════════════════════════════════════════════════════

class VerificadorWhatsApp:
    """
    Verifica si un número tiene cuenta activa en WhatsApp usando
    WhatsApp Web automatizado con Playwright y un perfil persistente.
    """

    def __init__(self, profile_dir: str, headless: bool = True,
                 pausa_min: int = 5, pausa_max: int = 10):
        self.profile_dir      = Path(profile_dir).absolute()
        self.headless         = headless
        self.pausa_min        = pausa_min
        self.pausa_max        = pausa_max
        self._browser         = None
        self._context         = None
        self._page            = None
        self._sesion_iniciada = False
        self._pw              = None

    def iniciar(self):
        """Lanza Playwright con perfil persistente y abre WhatsApp Web."""
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            log.error("❌  playwright no instalado:")
            log.error("   pip install playwright && playwright install chromium")
            sys.exit(1)

        # ── Fix Windows Python 3.11+ ─────────────────────────────────────────
        if sys.platform == "win32":
            import asyncio
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

        log.info(f"   📁  Perfil persistente: {self.profile_dir}")

        self._pw = sync_playwright().__enter__()

        # Usamos Chromium de Playwright para consistencia entre plataformas
        self._browser = self._pw.chromium.launch_persistent_context(
            user_data_dir=str(self.profile_dir),
            headless=self.headless,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            locale="es-MX",
            timezone_id="America/Mexico_City",
            viewport={"width": 1280, "height": 800},
        )

        # `launch_persistent_context` devuelve directamente el contexto
        self._context = self._browser
        self._page = self._context.new_page()
        self._abrir_whatsapp_web()

    def _abrir_whatsapp_web(self):
        """Navega a WhatsApp Web y espera sesión activa o QR."""
        log.info("   📱  Abriendo WhatsApp Web...")
        self._page.goto("https://web.whatsapp.com", wait_until="domcontentloaded", timeout=30_000)

        # Verificar si ya hay sesión activa (usando múltiples selectores)
        if self._detectar_sesion_activa():
            log.info("   ✅  Sesión activa encontrada — no se requiere QR.")
            self._sesion_iniciada = True
            return

        log.info("   ⏳  No se encontró sesión activa. Se requiere escanear QR.")

        # Si no hay sesión, verificar modo headless
        if self.headless:
            log.error("❌  No hay sesión guardada y el navegador está en headless.")
            log.error("   Solución: ejecuta UNA VEZ con --debug para escanear el QR:")
            log.error("   py -3.11 verificar_whatsapp.py --debug")
            self.cerrar()
            sys.exit(1)

        # Modo visible — esperar a que aparezca el QR
        log.info("   ⏳  Esperando que cargue el QR...")
        time.sleep(5)  # tiempo para que se pinte el canvas

        # Traer ventana al frente
        try:
            self._page.bring_to_front()
        except Exception:
            pass

        log.warning("   📷  ════════════════════════════════════════")
        log.warning("   📷  ESCANEA EL QR CON TU TELÉFONO AHORA")
        log.warning("   📷  Tienes 180 segundos...")
        log.warning("   📷  ════════════════════════════════════════")

        # Esperar hasta 180 segundos a que aparezca algún indicador de sesión
        inicio_qr = time.time()
        while time.time() - inicio_qr < 180:
            if self._detectar_sesion_activa():
                log.info("   ✅  QR escaneado correctamente.")
                self._sesion_iniciada = True
                return
            time.sleep(2)

        # Si llegamos aquí, no se detectó sesión
        log.error("❌  No se completó el escaneo del QR en 180 segundos.")
        log.error(f"   URL actual: {self._page.url}")
        log.error(f"   Título: {self._page.title()}")
        self.cerrar()
        sys.exit(1)

    def _detectar_sesion_activa(self) -> bool:
        """
        Intenta detectar si ya hay sesión iniciada en WhatsApp Web.
        Retorna True si encuentra algún elemento característico de la interfaz principal.
        """
        selectores = [
            "div[data-testid='chat-list']",                # Lista de chats
            "div[data-testid='conversation-panel-messages']",  # Panel de mensajes
            "div[aria-label*='chat' i]",                   # ARIA label con 'chat'
            "button[aria-label='Nuevo chat']",             # Botón nuevo chat
            "div[data-testid='chat-list-search']",         # Buscador de chats
        ]
        for selector in selectores:
            try:
                elemento = self._page.wait_for_selector(selector, timeout=3000, state="visible")
                if elemento:
                    log.debug(f"   ✅  Sesión detectada con selector: {selector}")
                    return True
            except Exception:
                continue
        return False

    def cerrar(self):
        """Cierra el navegador (el perfil se guarda automáticamente)."""
        try:
            if self._context:
                self._context.close()
        except Exception:
            pass
        try:
            if self._pw:
                self._pw.stop()
        except Exception:
            pass
        log.info("   🔒  Navegador cerrado (perfil guardado).")

    def verificar(self, telefono: str) -> bool:
        """
        Verifica si {telefono} tiene WhatsApp activo.
        Devuelve True = tiene WhatsApp / False = no tiene o error.
        """
        if not self._sesion_iniciada:
            return False

        numero_limpio = re.sub(r"[^\d]", "", telefono)

        try:
            url = f"https://web.whatsapp.com/send?phone={numero_limpio}"
            self._page.goto(url, wait_until="domcontentloaded", timeout=30_000)

            # Esperar hasta 30 segundos a que aparezca el input o el error
            selector_esperado = (
                "footer[data-testid='conversation-compose-box-input'], "
                "div[data-testid='chat-input'], "
                "div[role='textbox'], "  # selector genérico del input
                "div._2pf_-, "
                "div[data-testid='intro-title']"
            )
            self._page.wait_for_selector(selector_esperado, timeout=30_000)

            # Si aparece el input del chat → número tiene WhatsApp
            input_chat = self._page.query_selector(
                "footer[data-testid='conversation-compose-box-input'], "
                "div[data-testid='chat-input'], "
                "div[role='textbox']"
            )
            if input_chat:
                return True

            # Si aparece error → número no tiene WhatsApp
            error_el = self._page.query_selector("div._2pf_-")
            if error_el:
                return False

            return False

        except Exception as exc:
            log.debug(f"   Error verificando {telefono}: {exc}")
            # Capturar pantalla y mostrar URL para diagnóstico
            try:
                screenshot_path = self.profile_dir / f"error_{numero_limpio}.png"
                self._page.screenshot(path=str(screenshot_path))
                log.debug(f"   📸  Captura guardada: {screenshot_path}")
            except Exception:
                pass
            log.debug(f"   URL actual: {self._page.url}")
            log.debug(f"   Título: {self._page.title()}")
            return False

        finally:
            time.sleep(random.uniform(self.pausa_min, self.pausa_max))


# ════════════════════════════════════════════════════════════════════════════
# 3. MODO MOCK
# ════════════════════════════════════════════════════════════════════════════

def verificar_mock(telefono: str) -> bool:
    """Simulación ~35% tasa de éxito sin abrir WhatsApp."""
    time.sleep(random.uniform(0.1, 0.3))
    ultimo = re.sub(r"\D", "", telefono)[-1:]
    return ultimo in "02468"


# ════════════════════════════════════════════════════════════════════════════
# 4. RATE LIMITER
# ════════════════════════════════════════════════════════════════════════════

class RateLimiter:
    def __init__(self, max_por_hora: int):
        self.max_por_hora   = max_por_hora
        self.ventana_inicio = datetime.now()
        self.count_ventana  = 0

    def esperar_si_necesario(self):
        ahora   = datetime.now()
        elapsed = (ahora - self.ventana_inicio).total_seconds()
        if elapsed >= 3600:
            self.ventana_inicio = ahora
            self.count_ventana  = 0
            return
        if self.count_ventana >= self.max_por_hora:
            restante = 3600 - elapsed
            log.warning(f"   ⏸️   Límite {self.max_por_hora}/hora alcanzado.")
            log.warning(f"   ⏰  Esperando {restante/60:.1f} minutos...")
            time.sleep(restante + 5)
            self.ventana_inicio = datetime.now()
            self.count_ventana  = 0

    def registrar(self):
        self.count_ventana += 1

    def progreso(self) -> str:
        return f"{self.count_ventana}/{self.max_por_hora}"


# ════════════════════════════════════════════════════════════════════════════
# 5. ORQUESTADOR PRINCIPAL
# ════════════════════════════════════════════════════════════════════════════

def main():
    args = parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    log.info("🚀  verificar_whatsapp.py — Iniciando...")
    log.info(f"   Entrada    : {args.input}")
    log.info(f"   Salida     : {args.output}")
    log.info(f"   Máx/hora   : {args.max_hora}")
    log.info(f"   Pausa      : {args.pausa}–{args.pausa_max} seg")
    log.info(f"   Perfil     : {args.profile}")
    log.info(f"   Mock       : {'SÍ' if args.mock else 'NO'}")
    log.info(f"   Reanudar   : {'SÍ' if args.reanudar else 'NO'}")

    # ── Cargar leads ──────────────────────────────────────────────────────────
    leads = cargar_leads(args.input)
    log.info(f"   📥  {len(leads)} leads cargados.")

    leads_con_tel = [l for l in leads if l.get("telefono")]
    leads_sin_tel = [l for l in leads if not l.get("telefono")]
    log.info(f"   Con teléfono : {len(leads_con_tel)}")
    log.info(f"   Sin teléfono : {len(leads_sin_tel)} → marcados inválidos")

    for lead in leads_sin_tel:
        lead["whatsapp_valido"]    = False
        lead["whatsapp_estado"]    = "sin_telefono"
        lead["fecha_verificacion"] = datetime.now().strftime("%Y-%m-%d")

    # ── Checkpoint ────────────────────────────────────────────────────────────
    procesados = set()
    if args.reanudar:
        procesados = cargar_checkpoint(DEFAULT_CHECKPOINT)

    pendientes = [l for l in leads_con_tel if l.get("telefono") not in procesados]
    ya_listos  = [l for l in leads_con_tel if l.get("telefono") in procesados]
    log.info(f"   Pendientes   : {len(pendientes)}")
    log.info(f"   Ya listos    : {len(ya_listos)} (checkpoint)")

    if not pendientes:
        log.info("   ✅  Todos verificados. Guardando output final...")
        guardar_resultados(leads_sin_tel + leads_con_tel, args.output)
        imprimir_resumen(leads_sin_tel + leads_con_tel, args.output)
        return

    # ── Inicializar verificador ───────────────────────────────────────────────
    rate  = RateLimiter(max_por_hora=args.max_hora)
    verif = None

    if not args.mock:
        verif = VerificadorWhatsApp(
            profile_dir=args.profile,
            headless  = not args.debug,
            pausa_min = args.pausa,
            pausa_max = args.pausa_max,
        )
        verif.iniciar()
    else:
        log.info("   🎭  Modo MOCK — no se abre WhatsApp.")

    # ── Bucle de verificación ─────────────────────────────────────────────────
    total   = len(pendientes)
    validos = 0
    errores = 0
    inicio  = datetime.now()

    try:
        for i, lead in enumerate(pendientes, 1):
            telefono = lead["telefono"]
            rate.esperar_si_necesario()

            try:
                resultado = verificar_mock(telefono) if args.mock else verif.verificar(telefono)
            except KeyboardInterrupt:
                log.warning("\n   ⚠️  Interrupción manual. Guardando progreso...")
                break
            except Exception as exc:
                log.warning(f"   ⚠️  Error en {telefono}: {exc}")
                resultado = False
                errores  += 1

            lead["whatsapp_valido"]    = resultado
            lead["whatsapp_estado"]    = "valido" if resultado else "invalido"
            lead["fecha_verificacion"] = datetime.now().strftime("%Y-%m-%d")

            if resultado:
                validos += 1

            rate.registrar()
            procesados.add(telefono)

            pct    = i * 100 // total
            eta    = _calcular_eta(inicio, i, total)
            estado = "✅" if resultado else "❌"
            log.info(
                f"   [{i:>3}/{total}] {pct:>3}%  {estado}  "
                f"{telefono:<18}  válidos={validos}  "
                f"rate={rate.progreso()}  ETA={eta}"
            )

            # Checkpoint cada 10 leads
            if i % 10 == 0:
                guardar_checkpoint(DEFAULT_CHECKPOINT, procesados)
                guardar_resultados(leads_sin_tel + leads_con_tel, args.output)
                log.debug("   💾  Checkpoint guardado.")

    finally:
        if verif:
            verif.cerrar()
        # Guardar checkpoint final y resultados
        guardar_checkpoint(DEFAULT_CHECKPOINT, procesados)
        guardar_resultados(leads_sin_tel + leads_con_tel, args.output)

    # ── Resultado final ───────────────────────────────────────────────────────
    todos = leads_sin_tel + leads_con_tel
    guardar_resultados(todos, args.output)

    if len(procesados) >= len(leads_con_tel):
        Path(DEFAULT_CHECKPOINT).unlink(missing_ok=True)
        log.info("   🧹  Checkpoint eliminado (verificación completa).")
    else:
        guardar_checkpoint(DEFAULT_CHECKPOINT, procesados)

    imprimir_resumen(todos, args.output)


# ════════════════════════════════════════════════════════════════════════════
# 6. UTILIDADES
# ════════════════════════════════════════════════════════════════════════════

def _calcular_eta(inicio: datetime, completados: int, total: int) -> str:
    if completados == 0:
        return "--:--"
    elapsed   = (datetime.now() - inicio).total_seconds()
    por_lead  = elapsed / completados
    segundos  = int(por_lead * (total - completados))
    if segundos < 60:
        return f"{segundos}s"
    elif segundos < 3600:
        return f"{segundos // 60}m {segundos % 60}s"
    else:
        return f"{segundos // 3600}h {(segundos % 3600) // 60}m"


def imprimir_resumen(leads: list, output_path: str):
    total       = len(leads)
    verificados = sum(1 for l in leads if l.get("whatsapp_valido") is not None)
    validos     = sum(1 for l in leads if l.get("whatsapp_valido") is True)
    invalidos   = sum(1 for l in leads if l.get("whatsapp_valido") is False)
    sin_tel     = sum(1 for l in leads if l.get("whatsapp_estado") == "sin_telefono")
    tasa        = validos * 100 // max(verificados - sin_tel, 1)

    print("\n" + "═" * 60)
    print("  📊  RESUMEN — verificar_whatsapp.py")
    print("═" * 60)
    print(f"  Total leads             : {total}")
    print(f"  Verificados             : {verificados - sin_tel}")
    print(f"  Sin teléfono (omitidos) : {sin_tel}")
    print(f"  ✅  WhatsApp VÁLIDO      : {validos}  ({tasa}%)")
    print(f"  ❌  WhatsApp inválido    : {invalidos - sin_tel}")
    print(f"  Archivo de salida       : {output_path}")
    print("═" * 60)
    print("  ➡️   Siguiente: setup_postgresql.py importar")
    print("═" * 60 + "\n")

    if tasa < 15:
        log.warning("⚠️  Tasa baja (<15%) — revisa conexión o números.")
    elif tasa > 70:
        log.info("🎉  Tasa excelente (>70%).")


if __name__ == "__main__":
    main()