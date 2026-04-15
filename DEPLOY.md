# Despliegue en Railway — Guía paso a paso

## Archivos listos en tu carpeta wdsf_app
- `servidor.py` — app Flask adaptada para nube
- `index.html` — frontend (URL relativa /api)
- `wdsf_panel.db` — base de datos inicial con los 478 jueces
- `requirements.txt` — dependencias Python
- `Procfile` — comando de arranque para Railway
- `.gitignore` — qué ignorar en Git

---

## PASO 1 — Crear repositorio en GitHub

1. Abre https://github.com/new
2. Nombre del repositorio: `wdsf-panel-system`
3. Márcalo como **Private** (datos de jueces)
4. Clica **Create repository**
5. GitHub te mostrará instrucciones. Abre tu Terminal y ejecuta:

```bash
cd ~/wdsf_app
git init
git add .
git commit -m "WDSF Panel System inicial"
git branch -M main
git remote add origin https://github.com/TU_USUARIO/wdsf-panel-system.git
git push -u origin main
```
*(Cambia TU_USUARIO por tu nombre de usuario de GitHub)*

---

## PASO 2 — Crear cuenta en Railway

1. Ve a https://railway.app
2. Clica **Login with GitHub** → autoriza Railway
3. Ya estás dentro

---

## PASO 3 — Crear el proyecto en Railway

1. Clica **New Project**
2. Elige **Deploy from GitHub repo**
3. Selecciona `wdsf-panel-system`
4. Railway detectará automáticamente que es Python y usará el `Procfile`
5. Espera ~2 minutos a que termine el despliegue

---

## PASO 4 — Añadir volumen persistente (para la base de datos)

Sin esto, la base de datos se borraría al reiniciar el servidor.

1. En tu proyecto Railway, clica en el servicio (el cuadro azul)
2. Ve a la pestaña **Volumes**
3. Clica **Add Volume**
4. Mount path: `/data`
5. Clica **Add** → Railway reiniciará el servicio
6. La primera vez copiará automáticamente `wdsf_panel.db` al volumen

---

## PASO 5 — Añadir contraseña de acceso

1. En el servicio, ve a la pestaña **Variables**
2. Clica **New Variable** y añade:
   - `APP_PASSWORD` = la contraseña que quieras (ej: `Wdsf2024!`)
   - `APP_USER` = `wdsf` (o el nombre de usuario que prefieras)
   - `SECRET_KEY` = una cadena aleatoria larga (ej: `wdsf-panel-key-xyz-2024`)
3. Railway reiniciará con las nuevas variables

---

## PASO 6 — Obtener la URL pública

1. En el servicio, ve a la pestaña **Settings**
2. Busca la sección **Networking → Public Networking**
3. Clica **Generate Domain**
4. Te dará una URL tipo: `wdsf-panel-system-production.up.railway.app`

¡Ya puedes compartir esa URL con los compañeros de la WDSF!

---

## Acceso para compañeros

Envíales la URL y la contraseña. Al abrir la URL, el navegador pedirá usuario y contraseña:
- **Usuario**: `wdsf` (o el que pusiste en APP_USER)
- **Contraseña**: la que pusiste en APP_PASSWORD

---

## Actualizar la app (cuando hagas cambios)

Cada vez que modifiques archivos localmente:

```bash
cd ~/wdsf_app
git add .
git commit -m "descripción del cambio"
git push
```

Railway detectará el push automáticamente y redespliegará en ~1 minuto.

---

## Coste estimado

- Railway cobra por uso real de CPU/RAM
- Una app Flask ligera cuesta aprox. **$1-3/mes**
- El volumen de 1GB cuesta **$0.25/mes**
- Total estimado: **menos de $5/mes**
