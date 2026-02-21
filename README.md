---

## 🚀 Secuencia de Ejecución del Pipeline (Guía Paso a Paso)

Sigue estos pasos en orden para ejecutar correctamente el pipeline completo de LeadsB2B.

### 📌 Requisitos Previos
1.  **Python 3.11** instalado.
2.  Instalar dependencias globales:
    ```bash
    pip install playwright python-dotenv psycopg2-binary
    playwright install chromium
    ```
3.  Configurar archivo `.env` con tus credenciales de base de datos (si usarás PostgreSQL):
    ```env
    DB_HOST=localhost
    DB_PORT=5432
    DB_NAME=leads_b2b
    DB_USER=postgres
    DB_PASSWORD=tu_contraseña
    ```
4.  Tener una cuenta de WhatsApp **desechable** (no personal) para la verificación.

---

### 1️⃣ Extraer Leads desde Doctoralia
Este script obtiene los datos de psicólogos, psiquiatras, etc., en CDMX y los guarda en `leads_raw.json`.
```bash
# Primera ejecución (puede tardar, modo visible para depurar)
python extractor_hibrido.py --debug

# Ejecución normal (modo headless)
python extractor_hibrido.py
```
*Archivo generado:* `leads_raw.json`

---

### 2️⃣ (Opcional) Guardar Sesión de WhatsApp Manualmente
Si tu sesión de WhatsApp Web no persiste, puedes guardarla con este script auxiliar. **Ya no es necesario** con la versión actual que usa perfil persistente, pero se deja como referencia.
```bash
python guardar_sesion_wa.py
```

---

### 3️⃣ Verificar Números de WhatsApp
Este script es el corazón del proceso. Usa un perfil persistente de Chrome para mantener la sesión.

**Primera ejecución (escanear QR una sola vez):**
```bash
python verificar_whatsapp.py --debug
```
- Se abrirá una ventana de Chrome.
- Escanea el código QR con tu teléfono (cuenta secundaria).
- Espera a que la sesión se guarde automáticamente en la carpeta `whatsapp_profile/`.

**Ejecuciones subsecuentes (modo automático):**
```bash
python verificar_whatsapp.py
```
- Cargará la sesión guardada y verificará los números sin intervención.
- Los resultados se añaden al archivo `leads_verificados.json`.
*Archivo generado:* `leads_verificados.json`

---

### 4️⃣ Gestionar Base de Datos y Exportar a CRM
Este script unificado maneja la base de datos PostgreSQL y también puede generar CSVs directamente desde el JSON.

#### Opción A: Flujo Completo con Base de Datos
```bash
# 4a. Crear tablas en PostgreSQL (solo la primera vez)
python setup_postgresql.py setup

# 4b. Importar leads verificados a la base de datos
python setup_postgresql.py importar

# 4c. Ver estadísticas de la base de datos
python setup_postgresql.py stats

# 4d. Exportar a CSV (solo leads con WhatsApp válido)
python setup_postgresql.py exportar

# 4e. (Opcional) Exportar TODOS los leads a un archivo específico
python setup_postgresql.py exportar --todos --output mis_leads_completos.csv
```

#### Opción B: Exportar CSV Directamente desde JSON (sin PostgreSQL)
```bash
# Exportar solo leads válidos (por defecto)
python setup_postgresql.py exportar-json

# Exportar todos los leads
python setup_postgresql.py exportar-json --todos

# Exportar a un archivo con nombre personalizado
python setup_postgresql.py exportar-json --output leads_para_crm.csv
```
*Archivo generado:* `leads_crm_AAAAMMDD_HHMM.csv` (o el nombre que elijas).

---

### 📁 Estructura de Archivos Generados
| Archivo | Descripción |
| :--- | :--- |
| `leads_raw.json` | Datos crudos extraídos de Doctoralia. |
| `leads_verificados.json` | Datos con el resultado de la verificación de WhatsApp. |
| `checkpoint_verificacion.json` | Checkpoint para reanudar verificaciones interrumpidas. |
| `leads_crm_*.csv` | Archivo final listo para importar en HubSpot, Pipedrive, etc. |
| `whatsapp_profile/` | Perfil persistente de Chrome con la sesión de WhatsApp. **(NO SUBIR A GIT)** |

---

### ⚠️ Notas Importantes
- **Sesión de WhatsApp**: La carpeta `whatsapp_profile/` contiene datos de sesión sensibles. Está incluida en el `.gitignore` para que no se suba al repositorio.
- **Nunca uses tu WhatsApp personal** para la verificación. Crea una cuenta desechable.
- Si la verificación falla o se queda colgada, puedes ejecutar con `--debug` para ver el navegador en acción y diagnosticar el problema.
- Los tiempos de espera (`timeout`) están configurados para conexiones lentas, pero puedes ajustarlos en los scripts si es necesario.
