```markdown
# LeadsB2B — Pipeline de Generación y Verificación de Leads para Salud Mental en CDMX

[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/release/python-311/)
[![Playwright](https://img.shields.io/badge/playwright-1.40-green.svg)](https://playwright.dev/)
[![n8n](https://img.shields.io/badge/n8n-workflow-orange.svg)](https://n8n.io/)

Pipeline automatizado para extraer leads de psicólogos, psiquiatras y clínicas de salud mental en la CDMX desde **Doctoralia**, verificar su disponibilidad en **WhatsApp** y almacenarlos en **PostgreSQL** para su exportación a CRM.

## 🚀 Características

- **Extracción automatizada** desde Doctoralia.com.mx (vía Playwright).
- **Verificación de WhatsApp** con persistencia de sesión (perfil Chrome).
- **Persistencia de datos** en PostgreSQL con upsert por teléfono.
- **Exportación a CSV** listo para CRM (HubSpot, Pipedrive, etc.).
- **Flujo orquestado con n8n** (compatible Windows/Linux).
- **Notificaciones** por Telegram/Email de resultados y errores.
- **Checkpoint y reanudación** en caso de interrupción.
- **Protecciones anti-baneo** (rate limiting, delays aleatorios).

## 🧰 Tecnologías

- Python 3.11 + Playwright (automatización)
- PostgreSQL 15+ (almacenamiento)
- n8n (orquestación)
- Telegram Bot API / SMTP (notificaciones)

## 📦 Instalación

### 1. Clonar el repositorio

```bash
git clone https://github.com/firecode16/LeadsB2B.git
cd LeadsB2B
```

### 2. Crear y activar entorno virtual

**Windows:**
```bash
python -m venv venv
venv\Scripts\activate
```

**Linux/macOS:**
```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Instalar dependencias

```bash
pip install playwright python-dotenv psycopg2-binary
playwright install chromium
```

### 4. Configurar variables de entorno

Crea un archivo `.env` en la raíz del proyecto:

```env
# PostgreSQL
DB_HOST=localhost
DB_PORT=5432
DB_NAME=leads_b2b
DB_USER=postgres
DB_PASSWORD=tu_contraseña

# n8n (rutas absolutas)
SCRIPTS_DIR=C:\LeadsB2B           # Windows
# SCRIPTS_DIR=/home/user/LeadsB2B # Linux
EXPORTS_DIR=C:\exports             # Windows
# EXPORTS_DIR=/home/user/exports   # Linux

# Notificaciones (opcional)
TELEGRAM_BOT_TOKEN=tu_token
TELEGRAM_CHAT_ID=tu_chat_id
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=tu_correo@gmail.com
SMTP_PASS=tu_contraseña
SMTP_FROM=tu_correo@gmail.com
NOTIFY_EMAIL=destinatario@example.com
```

> **Importante:** En producción, usa rutas absolutas en `SCRIPTS_DIR` y `EXPORTS_DIR`.

## 📁 Estructura de Archivos

| Archivo | Descripción |
|--------|-------------|
| `extractor_hibrido.py` | Extrae leads de Doctoralia (genera `leads_raw.json`) |
| `verificar_whatsapp.py` | Verifica números en WhatsApp (genera `leads_verificados.json`) |
| `setup_postgresql.py` | Gestiona base de datos (importar, exportar, stats, limpiar) |
| `guardar_sesion_wa.py` | (Legacy) Guarda cookies de WhatsApp manualmente |
| `workflow_leads_b2b.json` | Workflow listo para importar en n8n |
| `whatsapp_profile/` | Perfil persistente de Chrome (NO SUBIR A GIT) |
| `leads_*.json` | Archivos de datos generados |
| `leads_crm.csv` | Exportación final para CRM |

## 🚦 Secuencia de Ejecución

### 1️⃣ Extraer leads de Doctoralia

```bash
# Primera ejecución (modo visible para depurar)
python extractor_hibrido.py --debug

# Ejecución normal (modo headless)
python extractor_hibrido.py --max 200
```

Genera: `leads_raw.json`

### 2️⃣ Verificar números en WhatsApp

**Primera vez (escanear QR):**
```bash
python verificar_whatsapp.py --debug
```

**Ejecuciones posteriores:**
```bash
python verificar_whatsapp.py
```

Genera: `leads_verificados.json` y carpeta `whatsapp_profile/`

> ⚠️ **Usa una cuenta de WhatsApp desechable**, no tu número personal.

### 3️⃣ Gestionar base de datos y exportar

#### Opción A: Con PostgreSQL

```bash
# Crear tablas (solo una vez)
python setup_postgresql.py setup

# Importar leads verificados
python setup_postgresql.py importar

# Ver estadísticas
python setup_postgresql.py stats

# Exportar a CSV (solo WhatsApp válidos)
python setup_postgresql.py exportar
```

#### Opción B: Exportar CSV directamente desde JSON

```bash
python setup_postgresql.py exportar-json
```

Genera: `leads_crm_AAAAMMDD_HHMM.csv` (o `leads_crm.csv` según configuración)

## 🤖 Integración con n8n

El archivo `workflow_leads_b2b.json` contiene un flujo completo que:

- Se activa manualmente, por schedule (lunes 6am) o vía webhook.
- Ejecuta los tres scripts secuencialmente.
- Verifica códigos de salida y maneja errores.
- Envía notificaciones por Telegram/Email con resumen.
- Es **multiplataforma** (usa variables `SCRIPTS_DIR` y `EXPORTS_DIR`).

### 📥 Importar el workflow en n8n

1. Abre n8n (local o cloud).
2. Ve a **Workflows** → **Import from File**.
3. Selecciona `workflow_leads_b2b.json`.
4. Configura las credenciales (Telegram, SMTP) si las usas.
5. Define las variables de entorno en n8n (Settings → Environment Variables):
   - `SCRIPTS_DIR`
   - `EXPORTS_DIR`
   - Opcionales: `TELEGRAM_CHAT_ID`, `SMTP_FROM`, etc.

### 🔗 Activar por webhook

El workflow expone un webhook en:
```
https://tu-n8n.com/webhook/leads-b2b-trigger
```
Puedes llamarlo desde Postman, un cron externo o tu propia app.

## ⚙️ Configuración Avanzada

### Rate Limiting (anti-baneo)

En `verificar_whatsapp.py` puedes ajustar:
```python
DEFAULT_MAX_HORA = 40   # máx verificaciones por hora
DEFAULT_PAUSA_SEG = 5   # pausa mínima entre verificaciones
DEFAULT_PAUSA_MAX = 10  # pausa máxima
```

### Filtros de exportación

```bash
# Exportar todos los leads (incluyendo inválidos)
python setup_postgresql.py exportar --todos

# Filtrar por nicho específico
python setup_postgresql.py exportar --nicho psicologo

# Filtrar por campaña (expo_id)
python setup_postgresql.py exportar --expo 2026-02
```

## 🧹 Mantenimiento

### Limpiar base de datos
```bash
python setup_postgresql.py limpiar
```
Elimina registros vacíos y normaliza datos.

### Reanudar verificación interrumpida
```bash
python verificar_whatsapp.py --reanudar
```

### Forzar nuevo escaneo de QR
Borra la carpeta `whatsapp_profile/` y ejecuta con `--debug`.

## 🛡️ Buenas Prácticas y Seguridad

- **Nunca uses tu WhatsApp personal** para la verificación. Crea una cuenta desechable.
- La carpeta `whatsapp_profile/` contiene datos de sesión sensibles. **Está en `.gitignore`** para evitar subirla al repositorio.
- Los archivos `leads_*.json` y `*.csv` también están ignorados por defecto.
- Usa variables de entorno para credenciales (nunca las hardcodees).
- En producción, ejecuta los scripts con un usuario con permisos restringidos.

## 📄 Licencia

MIT License — uso libre, bajo tu propia responsabilidad.

---

¿Preguntas o mejoras? Abre un issue o contacta al mantenedor.
```

### ✅ Cambios principales respecto a versiones anteriores

1. **Sección de instalación detallada** con comandos para Windows y Linux.
2. **Variables de entorno** claramente documentadas (incluyendo las nuevas `SCRIPTS_DIR` y `EXPORTS_DIR` para n8n).
3. **Integración con n8n** explicada paso a paso (importación, configuración de credenciales, webhook).
4. **Notas de seguridad** reforzadas sobre la cuenta de WhatsApp y el perfil persistente.
5. **Comandos de exportación** actualizados con las opciones `--todos`, `--nicho`, `--expo`.
6. **Estructura de archivos** actualizada reflejando los scripts actuales.

Puedes copiar este contenido directamente y pegarlo en el editor de GitHub (el enlace que compartiste). Asegúrate de que el formato Markdown se vea bien (puedes previsualizarlo antes de guardar).
