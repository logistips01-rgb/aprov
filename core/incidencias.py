"""Módulo de Incidencias de Transporte y Devoluciones.

Completamente aislado del resto de la app de aprovisionamiento: no importa
nada de app48.py. Si algo falla aquí dentro, mostrar_incidencias() lo
captura y muestra un error en pantalla sin propagar la excepción.

Persistencia: Firestore, colección `incidencias_transporte`, UN DOCUMENTO
POR INCIDENCIA (a diferencia del patrón de "todo el DataFrame en chunks"
que usa el resto de la app). Así, dos usuarios dando de alta a la vez no
se pisan: cada alta es un `.add()` independiente, y el catálogo de
transportistas/clientes crece de forma atómica con ArrayUnion.
"""
import os
from datetime import datetime, date

import pandas as pd
import streamlit as st

from core.incidencias_config import (
    MOTIVOS, RESPONSABILIDADES, ESTADOS, COLECCION, DOC_CATALOGO,
    COLOR_ESTADO, COLOR_CORPORATIVO, COLOR_OSCURO,
)

_FIREBASE_KEY = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    'aprov-c526a-firebase-adminsdk-fbsvc-c7b86e52ba.json'
)


def _get_db():
    """Devuelve (cliente_firestore, error). Reutiliza la app de Firebase ya
    inicializada por app48.py si existe; si no, la inicializa ella misma
    (para que este módulo funcione de forma independiente)."""
    try:
        import firebase_admin
        from firebase_admin import credentials, firestore
        try:
            firebase_admin.get_app()
        except ValueError:
            cert = None
            try:
                if 'firebase' in st.secrets:
                    cert = dict(st.secrets['firebase'])
                    if 'private_key' in cert:
                        cert['private_key'] = cert['private_key'].replace('\\n', '\n')
            except Exception:
                cert = None
            try:
                if cert:
                    cred = credentials.Certificate(cert)
                elif os.path.exists(_FIREBASE_KEY):
                    cred = credentials.Certificate(_FIREBASE_KEY)
                else:
                    return None, "No se encontró configuración de Firebase"
                firebase_admin.initialize_app(cred)
            except Exception as e:
                return None, str(e)
        return firestore.client(), None
    except Exception as e:
        return None, str(e)


# ─────────────────────────────────────────────
# CAPA DE ACCESO A DATOS
# ─────────────────────────────────────────────

@st.cache_data(ttl=60, show_spinner=False)
def listar_incidencias(incluir_inactivas=False):
    """Lee todas las incidencias desde Firestore. Cacheado 60s; se invalida
    explícitamente (listar_incidencias.clear()) tras cada escritura."""
    db, err = _get_db()
    if err:
        return pd.DataFrame(), err
    try:
        docs = db.collection(COLECCION).stream()
        filas = []
        for d in docs:
            if d.id == DOC_CATALOGO:
                continue
            row = d.to_dict()
            row['_doc_id'] = d.id
            filas.append(row)
        if not filas:
            return pd.DataFrame(), None
        df = pd.DataFrame(filas)
        if not incluir_inactivas and 'activo' in df.columns:
            df = df[df['activo'] != False]  # noqa: E712 (NaN debe tratarse como activo)
        for col_fecha in ['fecha_incidencia', 'fecha_expedicion']:
            if col_fecha in df.columns:
                df[col_fecha] = pd.to_datetime(df[col_fecha], errors='coerce')
        return df.reset_index(drop=True), None
    except Exception as e:
        return pd.DataFrame(), str(e)


@st.cache_data(ttl=60, show_spinner=False)
def obtener_catalogo():
    """Catálogo abierto de transportistas y clientes/destinos, crece solo."""
    db, err = _get_db()
    if err:
        return {"transportistas": [], "clientes": []}
    try:
        doc = db.collection(COLECCION).document(DOC_CATALOGO).get()
        if doc.exists:
            data = doc.to_dict()
            return {
                "transportistas": sorted(data.get("transportistas", [])),
                "clientes": sorted(data.get("clientes", [])),
            }
    except Exception:
        pass
    return {"transportistas": [], "clientes": []}


def _anadir_al_catalogo(transportista=None, cliente=None):
    """Añade valores nuevos al catálogo de forma atómica (ArrayUnion),
    segura ante altas concurrentes de distintos usuarios."""
    db, err = _get_db()
    if err:
        return
    from firebase_admin import firestore
    updates = {}
    if transportista:
        updates["transportistas"] = firestore.ArrayUnion([transportista])
    if cliente:
        updates["clientes"] = firestore.ArrayUnion([cliente])
    if updates:
        try:
            db.collection(COLECCION).document(DOC_CATALOGO).set(updates, merge=True)
        except Exception:
            pass
    obtener_catalogo.clear()


def crear_incidencia(datos: dict):
    """Alta de una incidencia nueva. Un documento independiente por alta:
    no hay lectura-modificación-escritura de una lista compartida, así que
    dos altas simultáneas de usuarios distintos no pueden pisarse."""
    db, err = _get_db()
    if err:
        return False, err
    try:
        datos = dict(datos)
        datos['activo'] = True
        datos['timestamp_alta'] = datetime.now().isoformat()
        for k in ('fecha_incidencia', 'fecha_expedicion'):
            if isinstance(datos.get(k), date):
                datos[k] = datos[k].isoformat()
        db.collection(COLECCION).document().set(datos)
        _anadir_al_catalogo(datos.get('transportista'), datos.get('cliente'))
        listar_incidencias.clear()
        return True, None
    except Exception as e:
        return False, str(e)


def actualizar_incidencia(doc_id: str, datos: dict):
    db, err = _get_db()
    if err:
        return False, err
    try:
        datos = dict(datos)
        for k in ('fecha_incidencia', 'fecha_expedicion'):
            if isinstance(datos.get(k), (date, pd.Timestamp)):
                datos[k] = pd.Timestamp(datos[k]).isoformat()
        db.collection(COLECCION).document(doc_id).update(datos)
        listar_incidencias.clear()
        return True, None
    except Exception as e:
        return False, str(e)


def borrar_incidencia(doc_id: str):
    """Borrado lógico: nunca se elimina el documento físicamente."""
    return actualizar_incidencia(doc_id, {"activo": False})


# ─────────────────────────────────────────────
# UI — PÁGINA DE REGISTRO (Fase 1)
# ─────────────────────────────────────────────

_SENTINEL_NUEVO = "➕ Añadir nuevo..."


def _selector_con_alta(label, opciones, key):
    """Selectbox que permite escribir un valor nuevo si no está en la lista."""
    opts = list(opciones) + [_SENTINEL_NUEVO]
    sel = st.selectbox(label, opts, key=f"{key}_sel")
    if sel == _SENTINEL_NUEVO:
        return st.text_input(f"Nuevo/a {label.lower()}", key=f"{key}_nuevo").strip()
    return sel


def _pagina_registro():
    catalogo = obtener_catalogo()

    st.markdown("#### 📝 Nueva incidencia")
    with st.form("form_incidencia", clear_on_submit=True):
        c1, c2, c3 = st.columns(3)
        with c1:
            fecha_incidencia = st.date_input("Fecha incidencia *", value=date.today())
            albaran = st.text_input("Nº albarán / pedido")
            referencia = st.text_input("Referencia producto")
        with c2:
            fecha_expedicion = st.date_input("Fecha expedición", value=None)
            destino = st.text_input("Destino (plataforma/almacén)")
            bandejas = st.number_input("Bandejas afectadas *", min_value=0, step=1)
        with c3:
            matricula_ruta = st.text_input("Matrícula / ruta")
            cajas = st.number_input("Cajas (manual)", min_value=0.0, step=1.0,
                                     help="Cálculo automático desde bandejas pendiente de datos de bandejas/caja por referencia. Por ahora, introduce el valor a mano.")

        cliente = _selector_con_alta("Cliente", catalogo["clientes"], "cliente")
        transportista = _selector_con_alta("Transportista", catalogo["transportistas"], "transportista")

        c4, c5 = st.columns(2)
        with c4:
            motivo = st.selectbox("Motivo *", MOTIVOS)
            responsabilidad = st.selectbox("Responsabilidad", RESPONSABILIDADES)
        with c5:
            estado = st.selectbox("Estado", ESTADOS, index=0)
            coste_producto = st.number_input("Coste producto (€)", min_value=0.0, step=1.0)
            coste_porte = st.number_input("Coste porte (€)", min_value=0.0, step=1.0)

        accion_correctiva = st.text_area("Acción correctiva")
        observaciones = st.text_area("Observaciones")

        submitted = st.form_submit_button("💾 Guardar incidencia", use_container_width=True)

        if submitted:
            errores = []
            if not fecha_incidencia:
                errores.append("La fecha de incidencia es obligatoria.")
            if not motivo:
                errores.append("El motivo es obligatorio.")
            if bandejas <= 0:
                errores.append("Las bandejas afectadas deben ser mayores que 0.")

            coste_total = round((coste_producto or 0) + (coste_porte or 0), 2)
            if errores:
                for e in errores:
                    st.error(e)
            else:
                if coste_total == 0:
                    st.warning("⚠️ El coste total es 0. Se ha guardado igualmente.")
                datos = {
                    "fecha_incidencia": fecha_incidencia,
                    "fecha_expedicion": fecha_expedicion,
                    "albaran": albaran.strip(),
                    "cliente": cliente,
                    "destino": destino.strip(),
                    "transportista": transportista,
                    "matricula_ruta": matricula_ruta.strip(),
                    "referencia": referencia.strip().upper(),
                    "bandejas": int(bandejas),
                    "cajas": float(cajas),
                    "motivo": motivo,
                    "responsabilidad": responsabilidad,
                    "coste_producto": float(coste_producto or 0),
                    "coste_porte": float(coste_porte or 0),
                    "coste_total": coste_total,
                    "estado": estado,
                    "accion_correctiva": accion_correctiva.strip(),
                    "observaciones": observaciones.strip(),
                    "usuario_alta": st.session_state.get("rol_usuario", "desconocido"),
                }
                ok, err = crear_incidencia(datos)
                if ok:
                    st.success("✅ Incidencia guardada.")
                    st.rerun()
                else:
                    st.error(f"❌ Error al guardar: {err}")

    st.divider()
    st.markdown("#### 📋 Incidencias registradas")

    df, err = listar_incidencias()
    if err:
        st.error(f"❌ Error al leer incidencias: {err}")
        return
    if df.empty:
        st.info("Todavía no hay incidencias registradas.")
        return

    # ── Filtros ──
    fc1, fc2, fc3, fc4, fc5 = st.columns(5)
    with fc1:
        f_desde = st.date_input("Desde", value=None, key="f_desde")
    with fc2:
        f_hasta = st.date_input("Hasta", value=None, key="f_hasta")
    with fc3:
        f_cliente = st.selectbox("Cliente", ["Todos"] + sorted(df['cliente'].dropna().unique().tolist()), key="f_cliente")
    with fc4:
        f_transportista = st.selectbox("Transportista", ["Todos"] + sorted(df['transportista'].dropna().unique().tolist()), key="f_transportista")
    with fc5:
        f_estado = st.selectbox("Estado", ["Todos"] + ESTADOS, key="f_estado")

    vista = df.copy()
    if f_desde:
        vista = vista[vista['fecha_incidencia'] >= pd.Timestamp(f_desde)]
    if f_hasta:
        vista = vista[vista['fecha_incidencia'] <= pd.Timestamp(f_hasta)]
    if f_cliente != "Todos":
        vista = vista[vista['cliente'] == f_cliente]
    if f_transportista != "Todos":
        vista = vista[vista['transportista'] == f_transportista]
    if f_estado != "Todos":
        vista = vista[vista['estado'] == f_estado]

    st.caption(f"{len(vista)} de {len(df)} incidencias")

    cols_editor = [
        '_doc_id', 'fecha_incidencia', 'cliente', 'transportista', 'referencia',
        'bandejas', 'cajas', 'motivo', 'responsabilidad', 'coste_total', 'estado', 'activo',
    ]
    cols_editor = [c for c in cols_editor if c in vista.columns]
    vista_editor = vista[cols_editor].copy()
    if 'activo' not in vista_editor.columns:
        vista_editor['activo'] = True
    vista_editor['activo'] = vista_editor['activo'].fillna(True)

    edited = st.data_editor(
        vista_editor,
        column_config={
            "_doc_id": st.column_config.TextColumn("ID", disabled=True),
            "fecha_incidencia": st.column_config.DateColumn("Fecha", disabled=True),
            "coste_total": st.column_config.NumberColumn("Coste total (€)", format="%.2f €", disabled=True),
            "estado": st.column_config.SelectboxColumn("Estado", options=ESTADOS),
            "activo": st.column_config.CheckboxColumn("Activo"),
        },
        disabled=['_doc_id', 'fecha_incidencia', 'coste_total'],
        hide_index=True,
        use_container_width=True,
        key="editor_incidencias",
    )

    if st.button("💾 Guardar cambios en la tabla"):
        cambios = 0
        errores_guardado = []
        original = vista_editor.set_index('_doc_id')
        nuevo = edited.set_index('_doc_id')
        for doc_id in nuevo.index:
            fila_orig = original.loc[doc_id]
            fila_nueva = nuevo.loc[doc_id]
            diffs = {}
            for col in ['estado', 'activo']:
                if col in fila_nueva.index and fila_orig[col] != fila_nueva[col]:
                    diffs[col] = fila_nueva[col]
            if diffs:
                ok, err = actualizar_incidencia(doc_id, diffs)
                if ok:
                    cambios += 1
                else:
                    errores_guardado.append(f"{doc_id}: {err}")
        if errores_guardado:
            for e in errores_guardado:
                st.error(e)
        if cambios:
            st.success(f"✅ {cambios} incidencia(s) actualizadas.")
            st.rerun()
        elif not errores_guardado:
            st.info("No había cambios que guardar.")


def mostrar_incidencias():
    """Punto de entrada único llamado desde app48.py. Cualquier excepción
    queda contenida aquí; nunca debe tumbar el resto de la aplicación."""
    st.header("🚚 Incidencias de Transporte y Devoluciones")
    try:
        _pagina_registro()
    except Exception as e:
        st.error(f"❌ Error en el módulo de Incidencias de Transporte: {e}")
        st.caption("El resto de la aplicación no se ve afectada por este error.")
