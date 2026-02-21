#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════╗
║     Script 1b: extractor_hibrido.py — Doctoralia + Phantombuster        ║
║     Nichos: Psicólogos · Psiquiatras · Terapeutas · Clínicas — CDMX    ║
╠══════════════════════════════════════════════════════════════════════════╣
║  Fuente 1 (principal): Doctoralia.com.mx vía Playwright                 ║
║    → Psicólogos, Psiquiatras, Terapeutas, Clínicas en CDMX             ║
║    → Extrae: nombre, especialidad, teléfono, consultorio, colonia       ║
║                                                                          ║
║  Fuente 2 (enriquecimiento): Phantombuster API                          ║
║    → LinkedIn Profile Scraper (cuenta gratuita = 2h/mes)               ║
║    → Enriquece con: cargo real, email, LinkedIn URL                     ║
║                                                                          ║
║  Salida: leads_raw.json → compatible con verificar_whatsapp.py          ║
╠══════════════════════════════════════════════════════════════════════════╣
║  Uso:                                                                    ║
║    python extractor_hibrido.py                                          ║
║    python extractor_hibrido.py --max 300                                ║
║    python extractor_hibrido.py --solo-doctoralia                        ║
║    python extractor_hibrido.py --mock                                   ║
╠══════════════════════════════════════════════════════════════════════════╣
║  Variables de entorno (.env):                                           ║
║    PHANTOMBUSTER_API_KEY  → Settings → API en app.phantombuster.com     ║
║    PHANTOMBUSTER_AGENT_ID → ID del agente LinkedIn Profile Scraper      ║
╚══════════════════════════════════════════════════════════════════════════╝

Dependencias:
    pip install playwright requests python-dotenv
    playwright install chromium
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
log = logging.getLogger("extractor_hibrido")


# ════════════════════════════════════════════════════════════════════════════
# 0. CONFIGURACIÓN
# ════════════════════════════════════════════════════════════════════════════

DEFAULT_OUTPUT = "leads_raw.json"
DEFAULT_MAX    = 200

# ── Nichos y sus URLs en Doctoralia CDMX ─────────────────────────────────────
NICHOS_DOCTORALIA = [
    {
        "nicho":        "psicologo",
        "label":        "Psicólogos",
        # ID 78 = Psicología (confirmado desde Doctoralia.com.mx Feb 2026)
        "url":          "https://www.doctoralia.com.mx/buscar?q=Psic%C3%B3logo&loc=Ciudad%20de%20M%C3%A9xico&filters%5Bspecializations%5D%5B%5D=78",
        "especialidad": "Psicología",
    },
    {
        "nicho":        "psiquiatra",
        "label":        "Psiquiatras",
        # ID 77 = Psiquiatría
        "url":          "https://www.doctoralia.com.mx/buscar?q=Psiquiatra&loc=Ciudad%20de%20M%C3%A9xico&filters%5Bspecializations%5D%5B%5D=77",
        "especialidad": "Psiquiatría",
    },
    {
        "nicho":        "terapeuta",
        "label":        "Terapeutas",
        # Búsqueda por texto libre — psicoterapeuta
        "url":          "https://www.doctoralia.com.mx/buscar?q=Psicoterapeuta&loc=Ciudad%20de%20M%C3%A9xico",
        "especialidad": "Psicoterapia",
    },
    {
        "nicho":        "clinica_salud_mental",
        "label":        "Clínicas de Salud Mental",
        # Búsqueda de clínicas de salud mental
        "url":          "https://www.doctoralia.com.mx/buscar?q=Cl%C3%ADnica+salud+mental&loc=Ciudad%20de%20M%C3%A9xico",
        "especialidad": "Clínica Salud Mental",
    },
]

# Selectores CSS de Doctoralia (validados Feb 2026)
# Si Doctoralia cambia su estructura, actualizar aquí
SEL = {
    # Listado principal
    "tarjeta":          "li[data-id], div.search-item, article.search-doctor-card",
    "nombre":           "a.doctor-name, h3.doctor-name, span[itemprop='name']",
    "especialidad_tag": "span.specialization, p.specialization, li.specialization",
    "link_perfil":      "a.doctor-name, a[href*='/medicos/'], a[href*='/doctores/']",

    # Página de perfil individual
    "telefono":         "a[href^='tel:'], span[itemprop='telephone'], div.phone-number",
    "direccion":        "span[itemprop='streetAddress'], div.address-line, p.address",
    "consultorio":      "span[itemprop='name'][itemtype*='MedicalClinic'], div.office-name",
    "colonia":          "span[itemprop='addressLocality'], span.locality",

    # Paginación
    "next_page":        "a[rel='next'], a.next, li.next a",
}


# ════════════════════════════════════════════════════════════════════════════
# 1. FUENTE PRINCIPAL — Doctoralia.com.mx vía Playwright
# ════════════════════════════════════════════════════════════════════════════

class ScraperDoctoralia:
    """
    Extrae perfiles de profesionales de salud mental desde Doctoralia.com.mx.

    Estrategia:
        1. Recorre las páginas de listado de cada nicho
        2. Extrae el link al perfil individual de cada profesional
        3. Visita cada perfil para obtener teléfono, dirección y consultorio
        4. Respeta delays para no sobrecargar el servidor
    """

    BASE_DELAY_MIN = 2.0   # segundos entre requests (cortesía)
    BASE_DELAY_MAX = 4.5
    MAX_PAGINAS    = 10    # máximo de páginas por nicho (20 resultados/página = 200 max)

    def __init__(self, headless: bool = True):
        self.headless = headless
        self._browser = None
        self._context = None
        self._page    = None

    def iniciar(self):
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            log.error("❌  playwright no instalado: pip install playwright && playwright install chromium")
            sys.exit(1)

        # ── Fix para Windows (Python 3.11+) ───────────────────────────────────
        # Playwright necesita ProactorEventLoop en Windows (es el default).
        # NO usar WindowsSelectorEventLoopPolicy — eso rompe subprocess en Playwright.
        if sys.platform == "win32":
            import asyncio
            # Forzar ProactorEventLoop explícitamente (el correcto para Windows)
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
            loop = asyncio.ProactorEventLoop()
            asyncio.set_event_loop(loop)

        self._pw      = sync_playwright().__enter__()
        self._browser = self._pw.chromium.launch(
            headless=self.headless,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
        )
        self._context = self._browser.new_context(
            locale="es-MX",
            timezone_id="America/Mexico_City",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1366, "height": 768},
        )
        # Bloquear recursos pesados para mayor velocidad
        self._context.route(
            "**/*.{png,jpg,jpeg,gif,webp,svg,woff,woff2,ttf,eot}",
            lambda r: r.abort()
        )
        self._page = self._context.new_page()
        log.info("   🎭  Playwright iniciado (Chromium headless).")

    def cerrar(self):
        try:
            if self._browser:
                self._browser.close()
        except Exception:
            pass
        try:
            if hasattr(self, "_pw") and self._pw:
                self._pw.stop()
        except Exception:
            pass

    # ── Extracción por nicho ─────────────────────────────────────────────────

    def extraer_nicho(self, nicho: dict, max_leads: int = 50) -> list[dict]:
        """Extrae todos los leads de un nicho de Doctoralia."""
        log.info(f"\n   🏥  Extrayendo nicho: {nicho['label']}")
        log.info(f"   URL base: {nicho['url']}")

        links_perfiles = self._recolectar_links(nicho["url"], max_leads)
        log.info(f"   Perfiles encontrados: {len(links_perfiles)}")

        leads = []
        for i, link in enumerate(links_perfiles, 1):
            try:
                lead = self._extraer_perfil(link, nicho)
                if lead:
                    leads.append(lead)
                    tel = lead.get("telefono") or "sin tel"
                    log.info(f"   [{i:>3}/{len(links_perfiles)}] ✅  {lead.get('empresa', '?')[:40]:<40} {tel}")
                else:
                    log.debug(f"   [{i:>3}] ⚠️  Perfil vacío: {link}")
            except Exception as exc:
                log.warning(f"   [{i:>3}] ❌  Error en {link}: {exc}")
            finally:
                time.sleep(random.uniform(self.BASE_DELAY_MIN, self.BASE_DELAY_MAX))

        log.info(f"   ✅  {nicho['label']}: {len(leads)} leads extraídos.")
        return leads

    def _recolectar_links(self, url_base: str, max_links: int) -> list:
        """
        Recorre las páginas de listado y recolecta los links a perfiles individuales.
        """
        links  = []
        url    = url_base
        pagina = 1

        while url and pagina <= self.MAX_PAGINAS and len(links) < max_links:
            try:
                log.debug(f"   Página {pagina}: {url}")
                self._page.goto(url, wait_until="domcontentloaded", timeout=20_000)
                time.sleep(random.uniform(1.5, 3.0))

                # Buscar links a perfiles — múltiples selectores por si cambia el HTML
                nuevos = self._extraer_links_pagina()
                if not nuevos:
                    log.debug(f"   Sin resultados en página {pagina} — terminando paginación.")
                    break

                links.extend(l for l in nuevos if l not in links)
                log.debug(f"   Página {pagina}: {len(nuevos)} perfiles nuevos (total: {len(links)})")

                # Siguiente página
                url    = self._get_siguiente_pagina()
                pagina += 1

            except Exception as exc:
                log.warning(f"   Error en página {pagina}: {exc}")
                break

        return links[:max_links]

    def _extraer_links_pagina(self) -> list:
        """Extrae todos los links a perfiles en la página actual."""
        links  = []
        vistos = set()

        # Esperar carga dinámica (Doctoralia usa React/JS)
        try:
            self._page.wait_for_load_state("networkidle", timeout=10_000)
        except Exception:
            pass
        time.sleep(1.5)  # pausa extra para JS dinámico

        try:
            # Selectores reales de Doctoralia /buscar (estructura Feb 2026)
            selectores = [
                # Cards de resultados — link principal del doctor
                "a[href*='/medico/']",
                "a[href*='/doctor/']",
                # Links con slug de doctor (formato: /nombre-apellido,ciudad.html)
                "a[href$='.html']",
                # Clases de la lista de resultados
                "ul.search-list a",
                "div.search-content a",
                "div[class*='doctor'] a",
                "div[class*='search'] a[href*='.html']",
                # Selector genérico de tarjetas
                "article a",
                "li a[href*='doctoralia']",
            ]

            for sel in selectores:
                try:
                    elementos = self._page.query_selector_all(sel)
                    for el in elementos:
                        href = el.get_attribute("href") or ""
                        # Filtrar solo links de perfil individual
                        # Formato Doctoralia: /dr-nombre-apellido,ciudad.html
                        if (href and
                            len(href) > 30 and
                            "buscar" not in href and
                            "doctoralia.com.mx" in href or href.startswith("/dr-") or
                            (href.endswith(".html") and "/dr-" in href)):
                            url_completa = (href if href.startswith("http")
                                          else f"https://www.doctoralia.com.mx{href}")
                            if url_completa not in vistos and "doctoralia.com.mx" in url_completa:
                                vistos.add(url_completa)
                                links.append(url_completa)
                except Exception:
                    continue

            if not links:
                log.debug("   Sin links con selectores específicos.")
                log.debug(f"   URL actual: {self._page.url}")
                log.debug(f"   Título: {self._page.title()}")
                # Mostrar todos los hrefs encontrados para diagnóstico
                todos_hrefs = self._page.eval_on_selector_all(
                    "a[href]",
                    "els => els.map(e => e.href).filter(h => h.includes('doctoralia')).slice(0, 20)"
                )
                log.debug(f"   Hrefs Doctoralia encontrados: {todos_hrefs}")

        except Exception as exc:
            log.debug(f"   Error extrayendo links: {exc}")
        return links

    def _get_siguiente_pagina(self) -> Optional[str]:
        """Busca el link a la siguiente página."""
        try:
            selectores = [
                "a[rel='next']",
                "a[aria-label='Next']",
                "li.next a",
                "a.next-page",
                "a[data-testid='pagination-next']",
            ]
            for sel in selectores:
                el = self._page.query_selector(sel)
                if el:
                    href = el.get_attribute("href")
                    if href:
                        return href if href.startswith("http") else f"https://www.doctoralia.com.mx{href}"
        except Exception:
            pass
        return None

    def _extraer_perfil(self, url: str, nicho: dict) -> Optional[dict]:
        """
        Visita la página de perfil individual y extrae todos los datos disponibles.
        """
        try:
            self._page.goto(url, wait_until="domcontentloaded", timeout=20_000)
            time.sleep(random.uniform(1.0, 2.0))

            # ── Nombre ────────────────────────────────────────────────────────
            nombre = self._get_text_multi([
                "h1[itemprop='name']",
                "h1.doctor-name",
                "h1",
                "span[itemprop='name']",
            ])

            # ── Teléfono ──────────────────────────────────────────────────────
            telefono = self._extraer_telefono()

            # ── Consultorio / Clínica ─────────────────────────────────────────
            consultorio = self._get_text_multi([
                "span[itemprop='name'][itemtype*='MedicalClinic']",
                "div.office-name",
                "h2.office-name",
                "p.clinic-name",
                "div[data-testid='office-name']",
            ])

            # ── Dirección ─────────────────────────────────────────────────────
            direccion = self._get_text_multi([
                "span[itemprop='streetAddress']",
                "div.address",
                "p.address",
                "address",
            ])

            # ── Colonia / Delegación ──────────────────────────────────────────
            colonia = self._get_text_multi([
                "span[itemprop='addressLocality']",
                "span.locality",
                "div.neighborhood",
            ])
            delegacion = self._inferir_delegacion(direccion or colonia or "")

            # ── Especialidades ────────────────────────────────────────────────
            especialidades = self._extraer_especialidades()

            # ── Sitio web ─────────────────────────────────────────────────────
            sitio_web = self._get_attr_multi([
                "a[itemprop='url']",
                "a[data-testid='website-link']",
                "a.website-link",
            ], "href")

            # ── Validar: al menos nombre o teléfono ───────────────────────────
            if not nombre and not telefono:
                return None

            # ── Filtrar por CDMX ──────────────────────────────────────────────
            KEYWORDS_CDMX = [
                "ciudad de méxico", "cdmx", "df", "distrito federal",
                "iztapalapa", "coyoacán", "benito juárez", "cuauhtémoc",
                "miguel hidalgo", "tlalpan", "xochimilco", "azcapotzalco",
                "iztacalco", "gustavo a. madero", "venustiano carranza",
                "tláhuac", "milpa alta", "álvaro obregón", "cuajimalpa",
                "magdalena contreras", "narvarte", "condesa", "roma norte",
                "roma sur", "polanco", "del valle", "pedregal", "coyoacan",
            ]
            texto_dir = (direccion or "").lower() + " " + (colonia or "").lower()

            # Verificar keywords de CDMX
            es_cdmx = any(k in texto_dir for k in KEYWORDS_CDMX)

            # Verificar código postal CDMX: 01000–16999
            if not es_cdmx:
                cp_match = re.search(r'\b(0[1-9]\d{3}|1[0-6]\d{3})\b', direccion or "")
                es_cdmx = bool(cp_match)

            if not es_cdmx:
                log.debug(f"   ⚠️  Filtrado (no CDMX): {nombre} — {direccion}")
                return None

            return {
                "empresa":          consultorio or nombre,
                "nombre_contacto":  nombre,
                "cargo":            f"{nicho['especialidad']} — {', '.join(especialidades[:2]) if especialidades else ''}".rstrip(" — "),
                "telefono":         normalizar_telefono(telefono),
                "email":            None,   # Doctoralia no expone emails públicamente
                "linkedin":         None,
                "sitio_web":        sitio_web,
                "colonia":          colonia,
                "delegacion":       delegacion,
                "direccion_raw":    direccion,
                "especialidades":   especialidades,
                "nicho":            nicho["nicho"],
                "url_perfil":       url,
                "fuente":           "Doctoralia",
                "fecha_extraccion": datetime.now().strftime("%Y-%m-%d"),
            }

        except Exception as exc:
            log.debug(f"   Error extrayendo perfil {url}: {exc}")
            return None

    def _extraer_telefono(self) -> Optional[str]:
        """
        Intenta extraer teléfono por múltiples métodos:
        1. Link tel:
        2. Atributo itemprop
        3. Regex sobre el texto visible
        """
        # Método 1: link tel:
        try:
            el = self._page.query_selector("a[href^='tel:']")
            if el:
                href = el.get_attribute("href") or ""
                return href.replace("tel:", "").strip()
        except Exception:
            pass

        # Método 2: itemprop telephone
        try:
            el = self._page.query_selector("span[itemprop='telephone'], meta[itemprop='telephone']")
            if el:
                return (el.get_attribute("content") or el.inner_text() or "").strip()
        except Exception:
            pass

        # Método 3: regex sobre texto visible de la página
        try:
            texto = self._page.inner_text("body")
            # Patrones de teléfonos mexicanos
            patrones = [
                r"\+52\s?[\d\s\-]{10,14}",
                r"55\s?\d{4}\s?\d{4}",     # CDMX con lada 55
                r"\(\s?55\s?\)\s?\d{4}[\s\-]\d{4}",
                r"\d{2,3}[\s\-]\d{4}[\s\-]\d{4}",
            ]
            for patron in patrones:
                match = re.search(patron, texto)
                if match:
                    return match.group().strip()
        except Exception:
            pass

        return None

    def _extraer_especialidades(self) -> list:
        """Extrae la lista de especialidades del perfil."""
        try:
            selectores = [
                "span.specialization",
                "li.specialization",
                "div[data-testid='specialization']",
                "ul.specializations li",
            ]
            for sel in selectores:
                elementos = self._page.query_selector_all(sel)
                if elementos:
                    return [el.inner_text().strip() for el in elementos if el.inner_text().strip()]
        except Exception:
            pass
        return []

    def _get_text_multi(self, selectores: list) -> Optional[str]:
        """Intenta múltiples selectores y devuelve el primero que encuentre texto."""
        for sel in selectores:
            try:
                el = self._page.query_selector(sel)
                if el:
                    texto = el.inner_text().strip()
                    if texto:
                        return texto
            except Exception:
                continue
        return None

    def _get_attr_multi(self, selectores: list, attr: str) -> Optional[str]:
        """Intenta múltiples selectores para obtener un atributo."""
        for sel in selectores:
            try:
                el = self._page.query_selector(sel)
                if el:
                    val = el.get_attribute(attr)
                    if val:
                        return val
            except Exception:
                continue
        return None

    def _inferir_delegacion(self, texto: str) -> Optional[str]:
        """Infiere la alcaldía/delegación de CDMX desde un texto de dirección."""
        ALCALDIAS = [
            "Álvaro Obregón", "Azcapotzalco", "Benito Juárez", "Coyoacán",
            "Cuajimalpa", "Cuauhtémoc", "Gustavo A. Madero", "Iztacalco",
            "Iztapalapa", "Magdalena Contreras", "Miguel Hidalgo", "Milpa Alta",
            "Tláhuac", "Tlalpan", "Venustiano Carranza", "Xochimilco",
            # Nombres cortos/populares
            "Álvaro Obregón", "GAM", "Benito Juárez", "Del Valle",
            "Polanco", "Condesa", "Roma", "Coyoacán", "Tlalpan",
        ]
        texto_lower = texto.lower()
        for alc in ALCALDIAS:
            if alc.lower() in texto_lower:
                return alc
        return None


# ════════════════════════════════════════════════════════════════════════════
# 2. ENRIQUECIMIENTO — Phantombuster API
# ════════════════════════════════════════════════════════════════════════════

class EnriquecedorPhantombuster:
    """
    Usa la API de Phantombuster para enriquecer leads con datos de LinkedIn.

    Agente recomendado: "LinkedIn Profile Scraper"
    Plan gratuito: 2 horas de ejecución/mes (~200-400 perfiles).

    Cómo obtener las credenciales:
        1. Crea cuenta en phantombuster.com
        2. Ve a Settings → API → copia tu API Key
        3. Crea el agente "LinkedIn Profile Scraper"
        4. Copia el Agent ID de la URL: /agents/{AGENT_ID}
        5. El agente necesita también tus cookies de LinkedIn (sessioncookie)

    Variables de entorno:
        PHANTOMBUSTER_API_KEY   → tu API key
        PHANTOMBUSTER_AGENT_ID  → ID del agente LinkedIn Profile Scraper
        LINKEDIN_SESSION_COOKIE → cookie "li_at" de LinkedIn
    """

    API_BASE = "https://api.phantombuster.com/api/v2"

    def __init__(self):
        self.api_key   = os.getenv("PHANTOMBUSTER_API_KEY", "").strip()
        self.agent_id  = os.getenv("PHANTOMBUSTER_AGENT_ID", "").strip()
        self.li_cookie = os.getenv("LINKEDIN_SESSION_COOKIE", "").strip()
        self.disponible = bool(self.api_key and self.agent_id)

        if not self.disponible:
            log.warning("⚠️  Phantombuster no configurado — se omitirá enriquecimiento.")
            log.warning("   Para activar:")
            log.warning("   export PHANTOMBUSTER_API_KEY='tu_api_key'")
            log.warning("   export PHANTOMBUSTER_AGENT_ID='tu_agent_id'")

    def enriquecer_lote(self, leads: list[dict], max_enriquecer: int = 50) -> list[dict]:
        """
        Busca en LinkedIn los leads que tienen nombre pero no email ni cargo detallado.
        Respeta el límite gratuito de Phantombuster (2h/mes ≈ 50-100 perfiles).

        Devuelve los leads con campos adicionales donde fue posible enriquecer.
        """
        if not self.disponible:
            return leads

        # Solo enriquecer los que tienen nombre pero no email
        candidatos = [
            l for l in leads
            if l.get("nombre_contacto") and not l.get("email")
        ][:max_enriquecer]

        if not candidatos:
            log.info("   ℹ️  Sin candidatos para enriquecer con Phantombuster.")
            return leads

        log.info(f"🔍  [Phantombuster] Enriqueciendo {len(candidatos)} perfiles vía LinkedIn...")

        try:
            import requests
        except ImportError:
            log.error("❌  requests no instalado: pip install requests")
            return leads

        headers = {
            "X-Phantombuster-Key": self.api_key,
            "Content-Type": "application/json",
        }

        # Construir lista de búsquedas para el agente
        # El agente LinkedIn Profile Scraper acepta nombres o URLs de LinkedIn
        busquedas = []
        for lead in candidatos:
            nombre = lead.get("nombre_contacto", "")
            empresa = lead.get("empresa", "")
            # Búsqueda por nombre + empresa para mayor precisión
            busquedas.append(f"{nombre} {empresa} CDMX psicología".strip())

        # Lanzar el agente de Phantombuster
        try:
            payload = {
                "id": self.agent_id,
                "argument": {
                    "sessionCookie":      self.li_cookie,
                    "spreadsheetUrl":     None,
                    "profileUrls":        busquedas[:50],  # límite por lanzamiento
                    "numberOfProfiles":   len(busquedas[:50]),
                    "extractDefaultUrl":  True,
                },
            }
            resp = requests.post(
                f"{self.API_BASE}/agents/launch",
                headers=headers,
                json=payload,
                timeout=30,
            )
            resp.raise_for_status()
            container_id = resp.json().get("containerId")
            log.info(f"   🚀  Agente lanzado. Container ID: {container_id}")
        except Exception as exc:
            log.warning(f"   ⚠️  Error lanzando agente Phantombuster: {exc}")
            return leads

        # Esperar a que el agente termine (polling)
        resultados_pb = self._esperar_resultado(container_id, headers)

        if not resultados_pb:
            log.warning("   ⚠️  Phantombuster no devolvió resultados.")
            return leads

        # Mapear resultados de vuelta a los leads por nombre
        leads_enriquecidos = self._aplicar_enriquecimiento(leads, candidatos, resultados_pb)
        log.info(f"   ✅  Phantombuster: {len(resultados_pb)} perfiles procesados.")
        return leads_enriquecidos

    def _esperar_resultado(self, container_id: str, headers: dict,
                           max_espera: int = 300, intervalo: int = 15) -> list[dict]:
        """
        Hace polling a la API de Phantombuster hasta que el agente termina.
        Timeout: 5 minutos (suficiente para plan gratuito).
        """
        import requests

        log.info(f"   ⏳  Esperando resultado del agente (máx {max_espera}s)...")
        tiempo_inicio = time.time()

        while time.time() - tiempo_inicio < max_espera:
            try:
                resp = requests.get(
                    f"{self.API_BASE}/containers/fetch-output",
                    headers=headers,
                    params={"id": container_id},
                    timeout=20,
                )
                resp.raise_for_status()
                data = resp.json()

                status = data.get("status", "")
                log.debug(f"   Estado agente: {status}")

                if status in ("finished", "error", "stopped"):
                    if status == "finished":
                        # El output viene como JSON lines en "output"
                        output_raw = data.get("output", "[]")
                        try:
                            if isinstance(output_raw, str):
                                # JSON Lines → parsear línea por línea
                                lineas = [l for l in output_raw.strip().split("\n") if l.strip()]
                                resultados = []
                                for linea in lineas:
                                    try:
                                        resultados.append(json.loads(linea))
                                    except Exception:
                                        pass
                                return resultados
                            elif isinstance(output_raw, list):
                                return output_raw
                        except Exception as exc:
                            log.warning(f"   Error parseando output PB: {exc}")
                    else:
                        log.warning(f"   Agente terminó con estado: {status}")
                    return []

            except Exception as exc:
                log.warning(f"   Error consultando estado: {exc}")

            time.sleep(intervalo)

        log.warning("   ⏰  Timeout esperando Phantombuster.")
        return []

    def _aplicar_enriquecimiento(self, leads: list[dict],
                                  candidatos: list[dict],
                                  resultados_pb: list[dict]) -> list[dict]:
        """
        Aplica los datos de LinkedIn a los leads correspondientes.
        Matching por nombre (fuzzy simple).
        """
        # Crear índice de resultados por nombre normalizado
        idx_pb = {}
        for r in resultados_pb:
            nombre_pb = (r.get("fullName") or r.get("name") or "").lower().strip()
            if nombre_pb:
                idx_pb[nombre_pb] = r

        enriquecidos = 0
        for lead in leads:
            nombre_lead = (lead.get("nombre_contacto") or "").lower().strip()
            if not nombre_lead:
                continue

            # Buscar match exacto primero, luego parcial
            datos_li = idx_pb.get(nombre_lead)
            if not datos_li:
                # Match parcial: buscar si el nombre del lead está contenido en algún resultado
                for nombre_pb, datos in idx_pb.items():
                    if nombre_lead in nombre_pb or nombre_pb in nombre_lead:
                        datos_li = datos
                        break

            if datos_li:
                # Enriquecer solo campos vacíos (no sobreescribir datos buenos)
                if not lead.get("email"):
                    lead["email"] = datos_li.get("email") or datos_li.get("mailFromLinkedin")
                if not lead.get("cargo") or lead["cargo"] == "—":
                    lead["cargo"] = datos_li.get("currentJobTitle") or datos_li.get("title")
                if not lead.get("linkedin"):
                    lead["linkedin"] = datos_li.get("linkedinProfileUrl") or datos_li.get("profileUrl")
                if not lead.get("empresa") and datos_li.get("currentCompanyName"):
                    lead["empresa"] = datos_li.get("currentCompanyName")

                lead["enriquecido_linkedin"] = True
                enriquecidos += 1

        log.info(f"   ✅  Leads enriquecidos con LinkedIn: {enriquecidos}")
        return leads


# ════════════════════════════════════════════════════════════════════════════
# 3. MODO MOCK — Para pruebas sin abrir navegadores
# ════════════════════════════════════════════════════════════════════════════

def generar_leads_mock(max_leads: int = 20) -> list[dict]:
    """Genera leads ficticios para probar el pipeline sin scraping real."""
    log.info("   🎭  Modo MOCK: generando leads de prueba...")

    nombres = [
        "Dra. Ana García López", "Psic. Carlos Mendoza Ruiz", "Dr. Roberto Silva Torres",
        "Dra. María Fernández Cruz", "Psic. Laura Jiménez Mora", "Dr. Eduardo Vega Soto",
        "Dra. Patricia Herrera Díaz", "Psic. Alejandro Ramos Núñez", "Dra. Sofía Castro Reyes",
        "Dr. Miguel Ángel Flores Gutiérrez",
    ]
    consultorios = [
        "Centro de Psicología Integral", "Clínica Mente Sana", "Consultorio Psicológico del Sur",
        "Instituto de Salud Mental CDMX", "Centro Terapéutico Humanista", "Psicoterapia Avanzada",
    ]
    colonias = [
        ("Del Valle", "Benito Juárez"), ("Condesa", "Cuauhtémoc"), ("Coyoacán", "Coyoacán"),
        ("Polanco", "Miguel Hidalgo"), ("Tlalpan Centro", "Tlalpan"), ("Roma Norte", "Cuauhtémoc"),
        ("Narvarte", "Benito Juárez"), ("Pedregal", "Álvaro Obregón"),
    ]
    nichos = ["psicologo", "psiquiatra", "terapeuta", "clinica_salud_mental"]

    leads = []
    for i in range(min(max_leads, len(nombres) * 2)):
        nombre   = nombres[i % len(nombres)]
        consult  = consultorios[i % len(consultorios)]
        colonia, deleg = colonias[i % len(colonias)]
        nicho    = nichos[i % len(nichos)]
        telefono = f"+5255{random.randint(10000000, 99999999)}"

        leads.append({
            "empresa":          consult,
            "nombre_contacto":  nombre,
            "cargo":            "Psicólogo / Director",
            "telefono":         telefono,
            "email":            None,
            "linkedin":         None,
            "sitio_web":        None,
            "colonia":          colonia,
            "delegacion":       deleg,
            "nicho":            nicho,
            "fuente":           "Mock",
            "fecha_extraccion": datetime.now().strftime("%Y-%m-%d"),
        })

    log.info(f"   ✅  {len(leads)} leads mock generados.")
    return leads


# ════════════════════════════════════════════════════════════════════════════
# 4. UTILIDADES
# ════════════════════════════════════════════════════════════════════════════

def normalizar_telefono(tel: Optional[str]) -> Optional[str]:
    if not tel:
        return None
    limpio = re.sub(r"[^\d+]", "", str(tel))
    if not limpio or len(limpio) < 7:
        return None
    if limpio.startswith("+52"):
        numero = limpio[3:]
    elif limpio.startswith("52") and len(limpio) >= 12:
        numero = limpio[2:]
    else:
        numero = limpio.lstrip("0")
    if len(numero) == 10:
        return f"+52{numero}"
    elif len(numero) == 8:
        return f"+5255{numero}"
    elif len(numero) > 10:
        return f"+52{numero[-10:]}"
    return f"+52{numero}" if len(numero) >= 7 else None


def deduplicar(leads: list[dict]) -> list[dict]:
    por_tel   = {}
    sin_tel   = []
    for lead in leads:
        tel = lead.get("telefono")
        if tel:
            existente = por_tel.get(tel)
            if not existente or _completitud(lead) > _completitud(existente):
                por_tel[tel] = lead
        else:
            sin_tel.append(lead)
    nombres_vistos = set()
    sin_tel_unicos = []
    for lead in sin_tel:
        clave = (lead.get("nombre_contacto") or lead.get("empresa") or "").lower().strip()
        if clave and clave not in nombres_vistos:
            nombres_vistos.add(clave)
            sin_tel_unicos.append(lead)
    return list(por_tel.values()) + sin_tel_unicos


def _completitud(lead: dict) -> int:
    campos = ["empresa", "telefono", "email", "nombre_contacto", "cargo",
              "sitio_web", "colonia", "linkedin"]
    return sum(1 for c in campos if lead.get(c))


def guardar_resultados(leads: list[dict], output_path: str):
    por_fuente = {}
    for l in leads:
        f = l.get("fuente", "?")
        por_fuente[f] = por_fuente.get(f, 0) + 1

    output = {
        "metadata": {
            "total_leads":      len(leads),
            "fecha_ejecucion":  datetime.now().isoformat(),
            "version_script":   "1b.0.0",
            "con_telefono":     sum(1 for l in leads if l.get("telefono")),
            "con_email":        sum(1 for l in leads if l.get("email")),
            "con_contacto":     sum(1 for l in leads if l.get("nombre_contacto")),
            "por_fuente":       por_fuente,
            "por_nicho": {
                n: sum(1 for l in leads if l.get("nicho") == n)
                for n in set(l.get("nicho", "?") for l in leads)
            },
        },
        "leads": leads,
    }
    Path(output_path).write_text(
        json.dumps(output, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def imprimir_resumen(leads: list[dict], output_path: str):
    con_tel    = sum(1 for l in leads if l.get("telefono"))
    con_email  = sum(1 for l in leads if l.get("email"))
    con_nombre = sum(1 for l in leads if l.get("nombre_contacto"))
    enriquec   = sum(1 for l in leads if l.get("enriquecido_linkedin"))

    por_nicho  = {}
    por_fuente = {}
    for l in leads:
        n = l.get("nicho", "?");   por_nicho[n]  = por_nicho.get(n, 0) + 1
        f = l.get("fuente", "?");  por_fuente[f] = por_fuente.get(f, 0) + 1

    print("\n" + "═" * 60)
    print("  📋  RESUMEN — extractor_hibrido.py")
    print("═" * 60)
    print(f"  Total leads únicos          : {len(leads)}")
    print(f"  ✅  Con teléfono             : {con_tel}  ({con_tel*100//max(len(leads),1)}%)")
    print(f"  📧  Con email                : {con_email}  ({con_email*100//max(len(leads),1)}%)")
    print(f"  👤  Con nombre de contacto   : {con_nombre}  ({con_nombre*100//max(len(leads),1)}%)")
    print(f"  🔗  Enriquecidos LinkedIn    : {enriquec}")
    print()
    print("  Por nicho:")
    for nicho, cant in sorted(por_nicho.items(), key=lambda x: -x[1]):
        print(f"    • {nicho:<30} {cant}")
    print()
    print("  Por fuente:")
    for fuente, cant in sorted(por_fuente.items(), key=lambda x: -x[1]):
        print(f"    • {fuente:<30} {cant}")
    print(f"\n  Archivo de salida           : {output_path}")
    print("═" * 60)
    print("  ➡️   Siguiente: python verificar_whatsapp.py")
    print("═" * 60 + "\n")


# ════════════════════════════════════════════════════════════════════════════
# 5. CLI Y ORQUESTADOR PRINCIPAL
# ════════════════════════════════════════════════════════════════════════════

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Extractor híbrido Doctoralia + Phantombuster — Salud Mental CDMX",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--output",          default=DEFAULT_OUTPUT)
    p.add_argument("--max",             type=int, default=DEFAULT_MAX,
                   help=f"Máx leads totales (default: {DEFAULT_MAX})")
    p.add_argument("--max-por-nicho",   type=int, default=60,
                   help="Máx leads por nicho en Doctoralia (default: 60)")
    p.add_argument("--solo-doctoralia", action="store_true",
                   help="Omitir Phantombuster (solo Doctoralia)")
    p.add_argument("--mock",            action="store_true",
                   help="Modo prueba: sin scraping real")
    p.add_argument("--debug",           action="store_true",
                   help="Logging detallado + navegador visible")
    return p.parse_args()


def main():
    args = parse_args()
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    log.info("🚀  extractor_hibrido.py — Doctoralia + Phantombuster")
    log.info(f"   Nichos    : {', '.join(n['label'] for n in NICHOS_DOCTORALIA)}")
    log.info(f"   Máx leads : {args.max}")
    log.info(f"   Salida    : {args.output}")

    todos_los_leads: list[dict] = []

    # ── Modo Mock ────────────────────────────────────────────────────────────
    if args.mock:
        todos_los_leads = generar_leads_mock(args.max)

    else:
        # ── Fuente 1: Doctoralia ─────────────────────────────────────────────
        scraper = ScraperDoctoralia(headless=not args.debug)
        scraper.iniciar()

        try:
            max_por_nicho = min(args.max_por_nicho, args.max // len(NICHOS_DOCTORALIA))
            for nicho in NICHOS_DOCTORALIA:
                if len(todos_los_leads) >= args.max:
                    break
                leads_nicho = scraper.extraer_nicho(nicho, max_leads=max_por_nicho)
                todos_los_leads.extend(leads_nicho)
                log.info(f"   Acumulado: {len(todos_los_leads)} leads.")
        finally:
            scraper.cerrar()

        # ── Fuente 2: Phantombuster (enriquecimiento) ────────────────────────
        if not args.solo_doctoralia:
            pb = EnriquecedorPhantombuster()
            if pb.disponible:
                # Limitar a 50 por el plan gratuito
                todos_los_leads = pb.enriquecer_lote(todos_los_leads, max_enriquecer=50)

    # ── Deduplicar ────────────────────────────────────────────────────────────
    log.info(f"   Deduplicando {len(todos_los_leads)} leads...")
    leads_unicos = deduplicar(todos_los_leads)
    log.info(f"   ✅  {len(leads_unicos)} leads únicos.")

    if not leads_unicos:
        log.error("❌  No se extrajeron leads. Prueba con --mock para verificar el pipeline.")
        sys.exit(1)

    # ── Guardar ───────────────────────────────────────────────────────────────
    guardar_resultados(leads_unicos, args.output)
    imprimir_resumen(leads_unicos, args.output)


if __name__ == "__main__":
    main()
