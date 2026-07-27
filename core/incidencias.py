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
from io import BytesIO

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

def _inject_css():
    """CSS propia del módulo, autocontenida (no depende de app48.py). Sólo
    se inyecta mientras esta página está visible, así que no hay riesgo de
    que afecte al resto de la aplicación."""
    st.markdown(f"""
    <style>
    .inc-seccion {{
      font-size: 12px !important;
      font-weight: 700 !important;
      color: {COLOR_CORPORATIVO} !important;
      text-transform: uppercase !important;
      letter-spacing: 0.06em !important;
      border-left: 3px solid {COLOR_CORPORATIVO};
      padding-left: 8px;
      margin-bottom: 10px;
    }}
    /* Los number_input no estaban cubiertos por el CSS global de app48.py:
       sus labels quedaban en negro/tamaño normal, distintos del resto. */
    .stNumberInput label {{
      font-size: 10px !important;
      font-weight: 600 !important;
      color: #7F8C8D !important;
      text-transform: uppercase !important;
      letter-spacing: 0.05em !important;
    }}
    /* Refuerzo de contraste en selects y su desplegable: evita texto poco
       visible si el navegador fuerza modo oscuro. */
    div[data-baseweb="select"] div, div[data-baseweb="select"] span {{
      color: {COLOR_OSCURO} !important;
    }}
    ul[role="listbox"] li {{
      color: {COLOR_OSCURO} !important;
      background: #FFFFFF !important;
    }}
    ul[role="listbox"] li:hover {{
      background: #F4E4E7 !important;
    }}
    </style>
    """, unsafe_allow_html=True)


_SENTINEL_NUEVO = "➕ Añadir nuevo..."


def _selector_con_alta(label, opciones, key):
    """Selectbox que permite escribir un valor nuevo si no está en la lista."""
    opts = list(opciones) + [_SENTINEL_NUEVO]
    sel = st.selectbox(label, opts, key=f"{key}_sel")
    if sel == _SENTINEL_NUEVO:
        return st.text_input(f"Nuevo/a {label.lower()}", key=f"{key}_nuevo").strip()
    return sel


def _filtros(df, key_prefix):
    """Bloque de filtros reutilizado entre Registro e Informe. Devuelve el
    DataFrame filtrado."""
    fc1, fc2, fc3, fc4, fc5 = st.columns(5)
    with fc1:
        f_desde = st.date_input("Desde", value=None, key=f"{key_prefix}_desde")
    with fc2:
        f_hasta = st.date_input("Hasta", value=None, key=f"{key_prefix}_hasta")
    with fc3:
        f_cliente = st.selectbox("Cliente", ["Todos"] + sorted(df['cliente'].dropna().unique().tolist()), key=f"{key_prefix}_cliente")
    with fc4:
        f_transportista = st.selectbox("Transportista", ["Todos"] + sorted(df['transportista'].dropna().unique().tolist()), key=f"{key_prefix}_transportista")
    with fc5:
        f_estado = st.selectbox("Estado", ["Todos"] + ESTADOS, key=f"{key_prefix}_estado")

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
    return vista


def _seccion(titulo):
    st.markdown(
        f'<div class="inc-seccion">{titulo}</div>',
        unsafe_allow_html=True,
    )


def _pagina_registro():
    catalogo = obtener_catalogo()

    st.markdown("#### 📝 Nueva incidencia")
    with st.form("form_incidencia", clear_on_submit=True):
        with st.container(border=True):
            _seccion("📦 Expedición")
            c1, c2, c3 = st.columns(3)
            with c1:
                fecha_incidencia = st.date_input("Fecha incidencia *", value=date.today())
                albaran = st.text_input("Nº albarán / pedido")
            with c2:
                fecha_expedicion = st.date_input("Fecha expedición", value=None)
                destino = st.text_input("Destino (plataforma/almacén)")
            with c3:
                matricula_ruta = st.text_input("Matrícula / ruta")
                referencia = st.text_input("Referencia producto")

        with st.container(border=True):
            _seccion("🚚 Cliente y transportista")
            cc1, cc2 = st.columns(2)
            with cc1:
                cliente = _selector_con_alta("Cliente", catalogo["clientes"], "cliente")
            with cc2:
                transportista = _selector_con_alta("Transportista", catalogo["transportistas"], "transportista")

        with st.container(border=True):
            _seccion("⚠️ Detalle de la incidencia")
            d1, d2, d3, d4 = st.columns(4)
            with d1:
                bandejas = st.number_input("Bandejas afectadas *", min_value=0, step=1)
            with d2:
                cajas = st.number_input("Cajas (manual)", min_value=0.0, step=1.0,
                                         help="Cálculo automático desde bandejas pendiente de datos de bandejas/caja por referencia. Por ahora, introduce el valor a mano.")
            with d3:
                motivo = st.selectbox("Motivo *", MOTIVOS)
            with d4:
                responsabilidad = st.selectbox("Responsabilidad", RESPONSABILIDADES)

        with st.container(border=True):
            _seccion("💶 Costes y seguimiento")
            e1, e2, e3 = st.columns(3)
            with e1:
                coste_producto = st.number_input("Coste producto (€)", min_value=0.0, step=1.0)
            with e2:
                coste_porte = st.number_input("Coste porte (€)", min_value=0.0, step=1.0)
            with e3:
                estado = st.selectbox("Estado", ESTADOS, index=0)

        with st.container(border=True):
            _seccion("📝 Notas")
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

    vista = _filtros(df, "reg")
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


# ─────────────────────────────────────────────
# UI — PÁGINA DE INFORME (Fase 2)
# ─────────────────────────────────────────────

def _pagina_informe():
    import plotly.express as px

    df, err = listar_incidencias()
    if err:
        st.error(f"❌ Error al leer incidencias: {err}")
        return
    if df.empty:
        st.info("Todavía no hay incidencias registradas.")
        return

    st.markdown("#### 🔎 Filtros")
    vista = _filtros(df, "inf")
    st.caption(f"{len(vista)} de {len(df)} incidencias")

    if vista.empty:
        st.warning("No hay incidencias que cumplan estos filtros.")
        return

    # ── KPIs ──
    total_inc = len(vista)
    coste_total = vista['coste_total'].sum() if 'coste_total' in vista.columns else 0
    coste_medio = coste_total / total_inc if total_inc else 0
    bandejas_total = int(vista['bandejas'].sum()) if 'bandejas' in vista.columns else 0
    pct_abiertas = (vista['estado'].isin(["Abierto", "En gestión"]).mean() * 100) if 'estado' in vista.columns else 0

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("📄 Incidencias", total_inc)
    k2.metric("💶 Coste total", f"{coste_total:,.2f} €".replace(",", "."))
    k3.metric("📊 Coste medio", f"{coste_medio:,.2f} €".replace(",", "."))
    k4.metric("📦 Bandejas afectadas", bandejas_total)
    k5.metric("🔴 % Abiertas/En gestión", f"{pct_abiertas:.1f} %")

    st.divider()

    # ── Evolución mensual ──
    st.subheader("📅 Evolución mensual")
    evol = vista.dropna(subset=['fecha_incidencia']).copy()
    if not evol.empty:
        evol['Mes'] = evol['fecha_incidencia'].dt.to_period('M').dt.to_timestamp()
        evol_mes = evol.groupby('Mes').agg(
            Incidencias=('fecha_incidencia', 'count'),
            Coste_total=('coste_total', 'sum'),
        ).reset_index()
        evol_mes['Mes_str'] = evol_mes['Mes'].dt.strftime('%b %Y')

        ce1, ce2 = st.columns(2)
        with ce1:
            fig_n = px.bar(
                evol_mes, x='Mes_str', y='Incidencias',
                text='Incidencias', color='Incidencias',
                color_continuous_scale='Reds',
                title='Nº de incidencias por mes',
                labels={'Mes_str': 'Mes'},
            )
            fig_n.update_traces(textposition='outside')
            fig_n.update_layout(showlegend=False, coloraxis_showscale=False)
            st.plotly_chart(fig_n, use_container_width=True)
        with ce2:
            fig_c = px.bar(
                evol_mes, x='Mes_str', y='Coste_total',
                text='Coste_total', color='Coste_total',
                color_continuous_scale='Reds',
                title='Coste total por mes (€)',
                labels={'Mes_str': 'Mes', 'Coste_total': 'Coste (€)'},
            )
            fig_c.update_traces(texttemplate='%{text:.0f} €', textposition='outside')
            fig_c.update_layout(showlegend=False, coloraxis_showscale=False)
            st.plotly_chart(fig_c, use_container_width=True)
    else:
        st.info("No hay fechas válidas para mostrar la evolución mensual.")

    st.divider()

    # ── Motivo y responsabilidad ──
    st.subheader("🧭 Motivo y responsabilidad")
    cm1, cm2 = st.columns(2)
    with cm1:
        por_motivo = vista.groupby('motivo').agg(
            Incidencias=('motivo', 'count'), Coste_total=('coste_total', 'sum')
        ).reset_index().sort_values('Incidencias', ascending=True)
        fig_m = px.bar(
            por_motivo, x='Incidencias', y='motivo', orientation='h',
            text='Incidencias', color='Incidencias',
            color_continuous_scale='Blues',
            title='Incidencias por motivo',
            labels={'motivo': 'Motivo'},
        )
        fig_m.update_traces(textposition='outside')
        fig_m.update_layout(showlegend=False, coloraxis_showscale=False)
        st.plotly_chart(fig_m, use_container_width=True)
    with cm2:
        por_resp = vista.groupby('responsabilidad').agg(
            Incidencias=('responsabilidad', 'count'), Coste_total=('coste_total', 'sum')
        ).reset_index().sort_values('Incidencias', ascending=True)
        fig_r = px.bar(
            por_resp, x='Incidencias', y='responsabilidad', orientation='h',
            text='Incidencias', color='Incidencias',
            color_continuous_scale='Oranges',
            title='Incidencias por responsabilidad',
            labels={'responsabilidad': 'Responsabilidad'},
        )
        fig_r.update_traces(textposition='outside')
        fig_r.update_layout(showlegend=False, coloraxis_showscale=False)
        st.plotly_chart(fig_r, use_container_width=True)

    st.divider()

    # ── Estado ──
    st.subheader("📌 Distribución por estado")
    por_estado = vista.groupby('estado').size().reset_index(name='Incidencias')
    fig_e = px.bar(
        por_estado, x='estado', y='Incidencias', text='Incidencias',
        color='estado', color_discrete_map=COLOR_ESTADO,
        title='Incidencias por estado',
        labels={'estado': 'Estado'},
    )
    fig_e.update_traces(textposition='outside')
    fig_e.update_layout(showlegend=False)
    st.plotly_chart(fig_e, use_container_width=True)

    st.divider()

    # ── Top transportistas y clientes ──
    st.subheader("🏆 Top transportistas y clientes")
    ct1, ct2 = st.columns(2)
    with ct1:
        top_transp = vista.groupby('transportista').agg(
            Incidencias=('transportista', 'count'), Coste_total=('coste_total', 'sum')
        ).reset_index().nlargest(10, 'Coste_total').sort_values('Coste_total', ascending=True)
        fig_t = px.bar(
            top_transp, x='Coste_total', y='transportista', orientation='h',
            text='Coste_total', color='Coste_total',
            color_continuous_scale='Reds',
            title='Top 10 transportistas por coste (€)',
            labels={'transportista': 'Transportista', 'Coste_total': 'Coste (€)'},
        )
        fig_t.update_traces(texttemplate='%{text:.0f} €', textposition='outside')
        fig_t.update_layout(showlegend=False, coloraxis_showscale=False)
        st.plotly_chart(fig_t, use_container_width=True)
    with ct2:
        top_cli = vista.groupby('cliente').agg(
            Incidencias=('cliente', 'count'), Coste_total=('coste_total', 'sum')
        ).reset_index().nlargest(10, 'Incidencias').sort_values('Incidencias', ascending=True)
        fig_cl = px.bar(
            top_cli, x='Incidencias', y='cliente', orientation='h',
            text='Incidencias', color='Incidencias',
            color_continuous_scale='Purples',
            title='Top 10 clientes por nº de incidencias',
            labels={'cliente': 'Cliente'},
        )
        fig_cl.update_traces(textposition='outside')
        fig_cl.update_layout(showlegend=False, coloraxis_showscale=False)
        st.plotly_chart(fig_cl, use_container_width=True)


_COLUMNAS_EXPORT = {
    "fecha_incidencia": "Fecha incidencia",
    "fecha_expedicion": "Fecha expedición",
    "albaran": "Albarán",
    "cliente": "Cliente",
    "destino": "Destino",
    "transportista": "Transportista",
    "matricula_ruta": "Matrícula/ruta",
    "referencia": "Referencia",
    "bandejas": "Bandejas",
    "cajas": "Cajas",
    "motivo": "Motivo",
    "responsabilidad": "Responsabilidad",
    "coste_producto": "Coste producto (€)",
    "coste_porte": "Coste porte (€)",
    "coste_total": "Coste total (€)",
    "estado": "Estado",
    "accion_correctiva": "Acción correctiva",
    "observaciones": "Observaciones",
    "usuario_alta": "Dado de alta por",
}


def _pagina_exportar():
    df, err = listar_incidencias()
    if err:
        st.error(f"❌ Error al leer incidencias: {err}")
        return
    if df.empty:
        st.info("Todavía no hay incidencias registradas.")
        return

    st.markdown("#### 🔎 Filtros a exportar")
    vista = _filtros(df, "exp")
    st.caption(f"{len(vista)} de {len(df)} incidencias seleccionadas para exportar")

    if vista.empty:
        st.warning("No hay incidencias que cumplan estos filtros.")
        return

    cols = [c for c in _COLUMNAS_EXPORT if c in vista.columns]
    export_df = vista[cols].rename(columns=_COLUMNAS_EXPORT).sort_values(
        _COLUMNAS_EXPORT["fecha_incidencia"]
    )
    for col_fecha in [_COLUMNAS_EXPORT["fecha_incidencia"], _COLUMNAS_EXPORT["fecha_expedicion"]]:
        if col_fecha in export_df.columns:
            export_df[col_fecha] = pd.to_datetime(export_df[col_fecha]).dt.strftime('%d/%m/%Y')

    st.dataframe(export_df, use_container_width=True, height=300)

    buf = BytesIO()
    export_df.to_excel(buf, index=False, sheet_name="Incidencias")
    buf.seek(0)
    nombre = f"incidencias_transporte_{date.today().strftime('%Y%m%d')}.xlsx"
    st.download_button(
        "📥 Exportar a Excel",
        buf,
        nombre,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )


def mostrar_incidencias():
    """Punto de entrada único llamado desde app48.py. Cualquier excepción
    queda contenida aquí; nunca debe tumbar el resto de la aplicación."""
    st.header("🚚 Incidencias de Transporte y Devoluciones")
    try:
        _inject_css()
        tab_registro, tab_informe, tab_exportar = st.tabs(
            ["📝 Registro", "📊 Informe", "📤 Exportar"]
        )
        with tab_registro:
            _pagina_registro()
        with tab_informe:
            _pagina_informe()
        with tab_exportar:
            _pagina_exportar()
    except Exception as e:
        st.error(f"❌ Error en el módulo de Incidencias de Transporte: {e}")
        st.caption("El resto de la aplicación no se ve afectada por este error.")
