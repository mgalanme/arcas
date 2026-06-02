"""
ARCAS - Sistema de Vigilancia Anticorrupción
Versión 4 — interfaz en lenguaje natural, acciones múltiples HITL,
recarga manual, generador de posts en Markdown con prompt maestro
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import requests, re, smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

# ── Configuración de página ───────────────────────────────────────────────────
st.set_page_config(
    page_title="ARCAS · Vigilancia Anticorrupción",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Estilos ───────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap');

html, body, [class*="css"] { font-family: 'Syne', sans-serif; }
.stApp { background: #080c10; color: #ddd8ce; }
h1,h2,h3 { font-family:'Syne',sans-serif; font-weight:800; letter-spacing:-0.03em; }

.arcas-header {
    background: linear-gradient(135deg,#0d1825 0%,#162030 100%);
    border:1px solid #1e3048; border-radius:14px;
    padding:2rem 2.5rem; margin-bottom:2rem; position:relative; overflow:hidden;
}
.arcas-header::before {
    content:''; position:absolute; top:0;left:0;right:0; height:3px;
    background:linear-gradient(90deg,#e63946,#f4a261,#2a9d8f,#457b9d);
}
.arcas-title { font-size:2rem; font-weight:800; color:#f0ebe0;
    margin:0; letter-spacing:-0.04em; }
.arcas-subtitle { color:#6a8099; font-family:'JetBrains Mono',monospace;
    font-size:0.72rem; margin-top:0.3rem; letter-spacing:0.05em;
    text-transform:uppercase; }

.kpi-card {
    background:#0d1520; border:1px solid #1a2d40; border-radius:12px;
    padding:1.4rem 1.6rem; transition:border-color 0.2s;
    position:relative; overflow:hidden;
}
.kpi-card::after {
    content:''; position:absolute; bottom:0;left:0;right:0; height:2px;
    background:var(--kpi-color,#2a9d8f);
}
.kpi-value { font-size:2.6rem; font-weight:800; color:#f0ebe0; line-height:1; }
.kpi-label { font-family:'JetBrains Mono',monospace; font-size:0.68rem;
    color:#445566; text-transform:uppercase; letter-spacing:0.08em;
    margin-top:0.4rem; }
.kpi-sublabel { font-size:0.75rem; color:#6a8099; margin-top:0.2rem; }

.alert-row {
    background:#0d1520; border:1px solid #1a2d40;
    border-left:4px solid var(--cat-color,#2a9d8f);
    border-radius:8px; padding:1rem 1.2rem; margin-bottom:0.7rem;
    transition:all 0.18s;
}
.alert-row:hover { border-color:#2a4a6a; }
.alert-title { font-weight:700; font-size:0.92rem; color:#e8e4dc; margin-bottom:0.35rem; }
.alert-meta { font-family:'JetBrains Mono',monospace; font-size:0.67rem;
    color:#445566; letter-spacing:0.03em; }

.badge {
    display:inline-block; padding:0.12rem 0.45rem; border-radius:4px;
    font-family:'JetBrains Mono',monospace; font-size:0.62rem;
    font-weight:600; letter-spacing:0.06em; text-transform:uppercase;
}
.b-A{background:#3d1a1f;color:#e63946;} .b-B{background:#3d2a1a;color:#f4a261;}
.b-C{background:#3d3020;color:#e9c46a;} .b-D{background:#1a3d3a;color:#2a9d8f;}
.b-E{background:#1a303d;color:#a8dadc;} .b-F{background:#2d1a3d;color:#9b72cf;}
.b-pending{background:#1e2d3d;color:#7a99bb;}
.b-approved{background:#1a3d2a;color:#2a9d8f;}
.b-rejected{background:#3d1a1f;color:#e63946;}
.b-escalated{background:#3d2a1a;color:#f4a261;}

.section-label {
    font-size:0.65rem; font-family:'JetBrains Mono',monospace;
    color:#3a5068; text-transform:uppercase; letter-spacing:0.12em;
    border-bottom:1px solid #141f2d; padding-bottom:0.4rem;
    margin-bottom:1.2rem; margin-top:1.5rem;
}
.deep-analysis {
    background:#0a1420; border:1px solid #1e3048; border-radius:8px;
    padding:1.2rem 1.5rem; line-height:1.75; color:#b8b0a4;
    font-size:0.88rem;
}
.post-raw {
    background:#0a1420; border:1px solid #1e3048; border-radius:8px;
    padding:1.2rem 1.5rem; font-size:0.88rem; line-height:1.7;
    color:#b8b0a4; white-space:pre-wrap; font-family:'JetBrains Mono',monospace;
}
.reload-box {
    background:linear-gradient(135deg,#0d1825,#162030);
    border:1px solid #1e3048; border-radius:12px;
    padding:1.5rem 2rem; margin-bottom:1.5rem;
}
.stButton>button {
    background:#111e2d!important; color:#c8d8e8!important;
    border:1px solid #1e3048!important; border-radius:6px!important;
    font-family:'JetBrains Mono',monospace!important; font-size:0.72rem!important;
    letter-spacing:0.05em!important; text-transform:uppercase!important;
    transition:all 0.18s!important; padding:0.45rem 1rem!important;
}
.stButton>button:hover {
    border-color:#3a6a9a!important; background:#162030!important;
    color:#e0eef8!important;
}
.stButton>button[kind="primary"] {
    background:linear-gradient(135deg,#1a4060,#1a5070)!important;
    border-color:#2a6080!important; color:#c8eeff!important;
}
.stCheckbox>label { font-size:0.82rem!important; color:#8899aa!important; }
.stTextArea textarea {
    background:#0d1520!important; border-color:#1a2d40!important;
    color:#c8d8e8!important; font-family:'JetBrains Mono',monospace!important;
    font-size:0.82rem!important;
}
.stSelectbox>div>div { background:#0d1520!important; border-color:#1a2d40!important; }
</style>
""", unsafe_allow_html=True)

# ── Constantes ────────────────────────────────────────────────────────────────
DATABRICKS_HOST  = st.secrets.get("DATABRICKS_HOST", "").rstrip("/")
DATABRICKS_TOKEN = st.secrets.get("DATABRICKS_TOKEN", "")
DATABRICKS_HTTP  = st.secrets.get("DATABRICKS_HTTP_PATH", "")
GROQ_API_KEY     = st.secrets.get("GROQ_API_KEY", "")
DATABRICKS_JOB_ID = "998370935632321"

CAT_INFO = {
    "A": ("Contratación Pública",  "#e63946"),
    "B": ("Enriquecimiento",       "#f4a261"),
    "C": ("Sesgo Judicial",        "#e9c46a"),
    "D": ("Desinformación",        "#2a9d8f"),
    "E": ("Redes de Influencia",   "#a8dadc"),
    "F": ("Nepotismo",             "#9b72cf"),
}

CAT_EXPLAIN = {
    "A": "Irregularidades en contratos o gasto público",
    "B": "Enriquecimiento ilícito o puertas giratorias",
    "C": "Trato judicial diferente según partido político",
    "D": "Noticias falsas o manipuladas",
    "E": "Redes de influencia o financiación ilegal",
    "F": "Enchufismo o nepotismo en cargos públicos",
}

LANGUAGES = {
    "🇪🇸 Español":  "español",
    "🇩🇪 Alemán":   "alemán",
    "🇫🇷 Francés":  "francés",
    "🇮🇹 Italiano": "italiano",
    "🇵🇱 Polaco":   "polaco",
    "🇷🇴 Rumano":   "rumano",
}

# ── Conexión Databricks ───────────────────────────────────────────────────────
@st.cache_resource
def get_db_conn():
    try:
        from databricks import sql
        return sql.connect(
            server_hostname=DATABRICKS_HOST.replace("https://", ""),
            http_path=DATABRICKS_HTTP,
            access_token=DATABRICKS_TOKEN,
        )
    except Exception as e:
        st.error(f"Error de conexión: {e}")
        return None

def run_query(q: str) -> pd.DataFrame:
    conn = get_db_conn()
    if not conn:
        return pd.DataFrame()
    try:
        with conn.cursor() as cur:
            cur.execute(q)
            cols = [d[0] for d in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=cols)
    except Exception as e:
        st.error(f"Error en consulta: {e}")
        return pd.DataFrame()

def update_alerts(alert_ids: list[str], status: str) -> bool:
    conn = get_db_conn()
    if not conn or not alert_ids:
        return False
    try:
        ids_str = ", ".join(f"'{aid}'" for aid in alert_ids)
        with conn.cursor() as cur:
            cur.execute(f"""
                UPDATE arcas_processed.alerts
                SET status = '{status}'
                WHERE alert_id IN ({ids_str})
            """)
        return True
    except Exception as e:
        st.error(f"Error al actualizar: {e}")
        return False

# ── Databricks Job trigger ────────────────────────────────────────────────────
def trigger_databricks_job() -> tuple[bool, str]:
    try:
        r = requests.post(
            f"{DATABRICKS_HOST}/api/2.1/jobs/run-now",
            headers={"Authorization": f"Bearer {DATABRICKS_TOKEN}",
                     "Content-Type": "application/json"},
            json={"job_id": int(DATABRICKS_JOB_ID)},
            timeout=15,
        )
        r.raise_for_status()
        run_id = r.json().get("run_id", "?")
        return True, str(run_id)
    except Exception as e:
        return False, str(e)

# ── Email helper ──────────────────────────────────────────────────────────────
def validate_email(email: str) -> bool:
    return bool(re.match(r"^[\w\.\+\-]+@[\w\-]+\.[a-z]{2,}$", email.strip(), re.IGNORECASE))

def send_email(to_addr: str, subject: str, body: str) -> tuple[bool, str]:
    sender   = st.secrets.get("EMAIL_SENDER", "")
    password = st.secrets.get("EMAIL_PASSWORD", "")
    if not sender or not password:
        return False, "Credenciales de email no configuradas en secrets."
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = sender
        msg["To"]      = to_addr
        msg.attach(MIMEText(body, "plain", "utf-8"))
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as srv:
            srv.login(sender, password)
            srv.sendmail(sender, to_addr, msg.as_string())
        return True, ""
    except Exception as e:
        return False, str(e)

# ── Groq helper ───────────────────────────────────────────────────────────────
def groq_generate(prompt: str) -> str:
    try:
        import groq as groq_sdk
        client = groq_sdk.Groq(api_key=GROQ_API_KEY)
        res = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=700,
        )
        return res.choices[0].message.content.strip()
    except Exception as e:
        return f"[Error al generar: {e}]"

# ── Navegación ────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style="padding:1rem 0 1.5rem;">
        <div style="font-family:'Syne',sans-serif;font-weight:800;font-size:1.5rem;
                    color:#f0ebe0;letter-spacing:-0.03em;">ARCAS</div>
        <div style="font-family:'JetBrains Mono',monospace;font-size:0.58rem;
                    color:#3a5068;text-transform:uppercase;letter-spacing:0.1em;
                    margin-top:0.15rem;">Vigilancia Anticorrupción</div>
    </div>
    """, unsafe_allow_html=True)

    page = st.radio(
        "nav",
        ["📊 Resumen general",
         "✅ Decisiones humanas",
         "✍️ Generar publicaciones",
         "🔄 Recargar información"],
        label_visibility="collapsed",
    )

    st.markdown("---")
    st.markdown("""
    <div style="font-family:'JetBrains Mono',monospace;font-size:0.58rem;
                color:#2a3d4d;text-transform:uppercase;letter-spacing:0.07em;
                line-height:1.8;">
        Fuentes activas<br>
        <span style="color:#3a5568;">
        BOE · El País · El Mundo · ABC<br>
        Público · elDiario · El Confidencial<br>
        OK Diario · La Razón · El Español<br>
        RTVE · La Sexta · Expansión<br>
        Poder Judicial · Transparencia<br>
        Civio · El Salto · AP News<br>
        Maldita · Newtral · EFE Verifica<br>
        RTVE Verifica · Snopes · PolitiFact
        </span>
    </div>
    """, unsafe_allow_html=True)

# ── Cabecera ──────────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="arcas-header">
    <div class="arcas-title">Sistema ARCAS</div>
    <div class="arcas-subtitle">
        Vigilancia Anticorrupción · {datetime.now().strftime('%d %b %Y %H:%M')}
    </div>
</div>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# PÁGINA 1 — RESUMEN GENERAL
# ══════════════════════════════════════════════════════════════════════════════
if page == "📊 Resumen general":

    df_alerts = run_query("""
        SELECT alert_id, category, status, confidence_score,
               source_name, title, created_at
        FROM arcas_processed.alerts
        ORDER BY created_at DESC
    """)
    df_art = run_query("SELECT count(*) AS n FROM arcas_raw.articles")
    total_art  = int(df_art["n"].iloc[0]) if not df_art.empty else 0
    total_al   = len(df_alerts)
    pending    = len(df_alerts[df_alerts["status"] == "pending"])  if not df_alerts.empty else 0
    approved   = len(df_alerts[df_alerts["status"] == "approved"]) if not df_alerts.empty else 0

    c1, c2, c3, c4 = st.columns(4)
    for col, val, label, sub, color in [
        (c1, total_art,  "Noticias analizadas",     "desde todas las fuentes",  "#2a9d8f"),
        (c2, total_al,   "Alertas detectadas",      "patrones identificados",   "#f4a261"),
        (c3, pending,    "Pendientes de decisión",  "esperan tu revisión",      "#e63946"),
        (c4, approved,   "Decisiones tomadas",      "aprobadas por operadores", "#9b72cf"),
    ]:
        col.markdown(f"""
        <div class="kpi-card" style="--kpi-color:{color};">
            <div class="kpi-value">{val}</div>
            <div class="kpi-label">{label}</div>
            <div class="kpi-sublabel">{sub}</div>
        </div>
        """, unsafe_allow_html=True)

    if not df_alerts.empty:
        st.markdown("<br>", unsafe_allow_html=True)
        col_l, col_r = st.columns([1, 1])

        with col_l:
            st.markdown('<div class="section-label">Tipos de irregularidad detectada</div>', unsafe_allow_html=True)
            cc = df_alerts["category"].value_counts().reset_index()
            cc.columns = ["cat", "n"]
            cc["label"] = cc["cat"].map(lambda c: CAT_EXPLAIN.get(c, c))
            cc["color"] = cc["cat"].map(lambda c: CAT_INFO.get(c, ("?", "#556677"))[1])
            fig = go.Figure(go.Bar(
                x=cc["n"], y=cc["label"], orientation="h",
                marker_color=cc["color"],
                text=cc["n"], textposition="outside",
            ))
            fig.update_layout(
                paper_bgcolor="#080c10", plot_bgcolor="#080c10",
                font=dict(color="#6a8099", family="JetBrains Mono", size=11),
                xaxis=dict(showgrid=False, zeroline=False, visible=False),
                yaxis=dict(showgrid=False, tickfont=dict(size=11)),
                margin=dict(l=0, r=30, t=5, b=5), height=300,
            )
            st.plotly_chart(fig, use_container_width=True)

        with col_r:
            st.markdown('<div class="section-label">Últimas noticias marcadas</div>', unsafe_allow_html=True)
            for _, row in df_alerts.head(6).iterrows():
                cat    = str(row.get("category", "?"))
                status = str(row.get("status", "pending"))
                title  = str(row.get("title", ""))[:75]
                source = str(row.get("source_name", ""))
                conf   = float(row.get("confidence_score", 0))
                color  = CAT_INFO.get(cat, ("?", "#556677"))[1]
                st.markdown(f"""
                <div class="alert-row" style="--cat-color:{color};">
                    <div class="alert-title">{title}…</div>
                    <div class="alert-meta">
                        <span class="badge b-{cat}">{CAT_INFO.get(cat,("?",""))[0]}</span>
                        <span class="badge b-{status}">{status}</span>
                        · {source} · {conf:.0%} de certeza
                    </div>
                </div>
                """, unsafe_allow_html=True)
    else:
        st.info("No hay datos todavía. Usa 'Recargar información' para obtener las últimas noticias.")

# ══════════════════════════════════════════════════════════════════════════════
# PÁGINA 2 — DECISIONES HUMANAS (HITL)
# ══════════════════════════════════════════════════════════════════════════════
elif page == "✅ Decisiones humanas":
    st.markdown('<div class="section-label">Noticias que necesitan tu revisión</div>', unsafe_allow_html=True)
    st.markdown("Revisa cada alerta y decide si publicarla, descartarla o escalarla. También puedes seleccionar varias y actuar sobre todas a la vez.")

    df = run_query("""
        SELECT alert_id, category, status, confidence_score,
               nl_justification, source_name, title, content_url, created_at
        FROM arcas_processed.alerts
        WHERE status = 'pending'
        ORDER BY confidence_score DESC
    """)

    if df.empty:
        st.success("✅ No hay noticias pendientes de revisión en este momento.")
    else:
        # ── Acciones en bloque ────────────────────────────────────────────
        st.markdown(f"**{len(df)} alerta(s) esperan tu decisión**")

        with st.expander("⚡ Actuar sobre varias alertas a la vez"):
            st.markdown("Selecciona las alertas que quieres gestionar de forma conjunta:")
            selected_ids = []
            for _, row in df.iterrows():
                aid   = str(row["alert_id"])
                cat   = str(row["category"])
                title = str(row["title"])[:70]
                conf  = float(row["confidence_score"])
                checked = st.checkbox(
                    f"[{CAT_INFO.get(cat,('?',''))[0]}] {title}… ({conf:.0%})",
                    key=f"chk_{aid}"
                )
                if checked:
                    selected_ids.append(aid)

            if selected_ids:
                st.markdown(f"**{len(selected_ids)} seleccionadas**")
                col_ba, col_br, col_be = st.columns(3)
                with col_ba:
                    if st.button("✅ Aprobar seleccionadas", key="bulk_approve"):
                        if update_alerts(selected_ids, "approved"):
                            st.success(f"{len(selected_ids)} alertas aprobadas.")
                            st.rerun()
                with col_br:
                    if st.button("❌ Rechazar seleccionadas", key="bulk_reject"):
                        if update_alerts(selected_ids, "rejected"):
                            st.warning(f"{len(selected_ids)} alertas rechazadas.")
                            st.rerun()
                with col_be:
                    if st.button("⬆️ Escalar seleccionadas", key="bulk_escalate"):
                        if update_alerts(selected_ids, "escalated"):
                            st.info(f"{len(selected_ids)} alertas escaladas.")
                            st.rerun()

        st.markdown("---")

        # ── Revisión individual ───────────────────────────────────────────
        for _, row in df.iterrows():
            cat      = str(row["category"])
            conf     = float(row["confidence_score"])
            title    = str(row["title"])
            source   = str(row["source_name"])
            url      = str(row["content_url"])
            analysis = str(row["nl_justification"])
            alert_id = str(row["alert_id"])
            cat_name = CAT_INFO.get(cat, ("?", "#556677"))[0]
            cat_expl = CAT_EXPLAIN.get(cat, "")
            color    = CAT_INFO.get(cat, ("?", "#556677"))[1]

            with st.expander(f"[{cat_name}] {title[:75]}… · {conf:.0%} certeza"):
                st.markdown(f"""
                <div class="alert-meta" style="margin-bottom:0.8rem;">
                    <span class="badge b-{cat}">{cat_name}</span>
                    <span style="color:#6a8099;font-family:'JetBrains Mono',monospace;font-size:0.68rem;">
                    · {cat_expl} · Fuente: {source} · Certeza: {conf:.0%}
                    </span>
                </div>
                """, unsafe_allow_html=True)

                if url:
                    st.markdown(f"🔗 [Ver noticia original]({url})")

                st.markdown("**Análisis del sistema:**")
                st.markdown(f'<div class="deep-analysis">{analysis}</div>', unsafe_allow_html=True)

                st.markdown("<br>", unsafe_allow_html=True)
                notes = st.text_area(
                    "Notas (opcional)",
                    key=f"notes_{alert_id}",
                    placeholder="Añade contexto, dudas o modificaciones...",
                    height=70,
                    label_visibility="collapsed",
                )

                # ── Botones de decisión ───────────────────────────────────
                col_a, col_r, col_e, col_email = st.columns(4)
                with col_a:
                    if st.button("✅ Publicar", key=f"ap_{alert_id}"):
                        if update_alerts([alert_id], "approved"):
                            st.success("Aprobada y publicada.")
                            st.rerun()
                with col_r:
                    if st.button("❌ Descartar", key=f"re_{alert_id}"):
                        if update_alerts([alert_id], "rejected"):
                            st.warning("Descartada.")
                            st.rerun()
                with col_e:
                    if st.button("⬆️ Escalar", key=f"es_{alert_id}"):
                        if update_alerts([alert_id], "escalated"):
                            st.info("Escalada a nivel superior.")
                            st.rerun()
                with col_email:
                    if st.button("📧 Enviar por email", key=f"em_{alert_id}"):
                        st.session_state[f"show_email_{alert_id}"] = True

                if st.session_state.get(f"show_email_{alert_id}"):
                    email_addr = st.text_input(
                        "Dirección de email",
                        key=f"email_input_{alert_id}",
                        placeholder="nombre@dominio.com",
                    )
                    if st.button("Enviar ahora", key=f"send_{alert_id}"):
                        if not validate_email(email_addr):
                            st.error("La dirección de email no es válida.")
                        else:
                            body = (
                                f"ALERTA ARCAS — {cat_name}\n"
                                f"{'='*50}\n\n"
                                f"Titular: {title}\n"
                                f"Fuente: {source}\n"
                                f"Certeza: {conf:.0%}\n"
                                f"URL: {url}\n\n"
                                f"Análisis:\n{analysis}\n\n"
                                f"Notas del operador: {notes or '(ninguna)'}\n"
                            )
                            ok, err = send_email(
                                email_addr,
                                f"[ARCAS] Alerta {cat_name}: {title[:50]}",
                                body,
                            )
                            if ok:
                                st.success(f"Email enviado a {email_addr}")
                                st.session_state[f"show_email_{alert_id}"] = False
                            else:
                                st.error(f"Error al enviar: {err}")

# ══════════════════════════════════════════════════════════════════════════════
# PÁGINA 3 — GENERAR PUBLICACIONES
# ══════════════════════════════════════════════════════════════════════════════
elif page == "✍️ Generar publicaciones":
    st.markdown('<div class="section-label">Convierte una alerta en una publicación para redes sociales</div>', unsafe_allow_html=True)

    df = run_query("""
        SELECT alert_id, category, confidence_score,
               nl_justification, source_name, title, content_url
        FROM arcas_processed.alerts
        WHERE status IN ('approved','pending')
        ORDER BY confidence_score DESC
        LIMIT 50
    """)

    if df.empty:
        st.info("No hay alertas disponibles.")
    else:
        col_sel, col_lang = st.columns([2, 1])
        with col_sel:
            options = {
                f"[{row['category']}] {str(row['title'])[:65]}…": row
                for _, row in df.iterrows()
            }
            sel_label = st.selectbox("Selecciona la noticia a publicar", list(options.keys()))
            sel = options[sel_label]
        with col_lang:
            lang_label = st.selectbox("Idioma de la publicación", list(LANGUAGES.keys()))
            lang_name  = LANGUAGES[lang_label]

        col_plat, col_tone = st.columns(2)
        with col_plat:
            platform = st.radio(
                "Red social",
                ["Facebook / LinkedIn", "X (Twitter)", "Instagram"],
                horizontal=True,
            )
        with col_tone:
            tone = st.radio(
                "Tono de la publicación",
                ["Informativo", "Analítico", "Urgente", "Pedagógico"],
                horizontal=True,
            )

        char_limits = {
            "Facebook / LinkedIn": 3000,
            "X (Twitter)": 280,
            "Instagram": 2200,
        }
        char_limit = char_limits[platform]

        st.markdown('<div class="section-label" style="margin-top:1.5rem;">Instrucciones al redactor automático</div>', unsafe_allow_html=True)
        st.caption("Personaliza cómo quieres que se redacte la publicación. Puedes cambiar el estilo, añadir restricciones o pedir un enfoque concreto.")

        DEFAULT_PROMPT = """Eres un periodista de investigación riguroso y políticamente neutral.
Tu misión es informar al público sobre patrones que merecen atención cívica.
Nunca acuses directamente a personas ni partidos.
Usa un lenguaje accesible, sin jerga técnica ni legal.
Céntrate en los hechos específicos de la noticia analizada, no en generalidades.
Incluye al final una reflexión o pregunta que invite a la ciudadanía a investigar más."""

        master_prompt = st.text_area(
            "instrucciones",
            value=DEFAULT_PROMPT,
            height=150,
            label_visibility="collapsed",
        )

        if st.button("✍️ Redactar publicación", use_container_width=True, type="primary"):
            cat      = sel["category"]
            cat_name = CAT_INFO.get(cat, (cat, "#556677"))[0]
            justif   = str(sel["nl_justification"])
            title    = str(sel["title"])
            source   = str(sel["source_name"])
            conf     = float(sel["confidence_score"])

            full_prompt = f"""INSTRUCCIONES DEL OPERADOR:
{master_prompt}

---
NOTICIA ANALIZADA:
- Tipo de irregularidad: {cat_name}
- Fuente: {source}
- Titular: {title}
- Análisis del sistema: {justif[:600]}
- Nivel de certeza: {conf:.0%}

---
REQUISITOS TÉCNICOS:
- Idioma: {lang_name}
- Red social: {platform} (máximo {char_limit} caracteres)
- Tono: {tone}
- Formato: Markdown (usa **negrita**, *cursiva*, emojis y #hashtags donde sean naturales)
- No menciones sistemas de inteligencia artificial ni automatización
- Céntrate en los hechos específicos de ESTA noticia concreta

Escribe únicamente el texto de la publicación en Markdown."""

            with st.spinner("Redactando..."):
                post_md = groq_generate(full_prompt)

            st.markdown('<div class="section-label">Texto generado (código Markdown)</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="post-raw">{post_md}</div>', unsafe_allow_html=True)

            st.markdown('<div class="section-label">Vista previa renderizada</div>', unsafe_allow_html=True)
            st.markdown(post_md)

            char_count = len(post_md)
            color = "#2a9d8f" if char_count <= char_limit else "#e63946"
            st.markdown(
                f'<span style="font-family:JetBrains Mono,monospace;font-size:0.68rem;color:{color};">'
                f'{char_count} / {char_limit} caracteres</span>',
                unsafe_allow_html=True,
            )

            c1, c2 = st.columns(2)
            with c1:
                st.download_button("⬇️ Descargar .md", post_md,
                    file_name=f"arcas_{lang_name}_{cat}.md", mime="text/markdown")
            with c2:
                st.download_button("⬇️ Descargar .txt", post_md,
                    file_name=f"arcas_{lang_name}_{cat}.txt", mime="text/plain")

# ══════════════════════════════════════════════════════════════════════════════
# PÁGINA 4 — RECARGAR INFORMACIÓN
# ══════════════════════════════════════════════════════════════════════════════
elif page == "🔄 Recargar información":
    st.markdown('<div class="section-label">Actualización manual de fuentes</div>', unsafe_allow_html=True)

    st.markdown("""
    <div class="reload-box">
        <div style="font-size:1.1rem;font-weight:700;color:#e8e4dc;margin-bottom:0.5rem;">
            ¿Quieres analizar las noticias de ahora mismo?
        </div>
        <div style="font-size:0.85rem;color:#6a8099;line-height:1.6;">
            El sistema recoge información automáticamente cada día a las 09:30.
            Si necesitas analizar noticias que acaban de publicarse, pulsa el botón
            para lanzar el proceso ahora. Tardará entre 3 y 8 minutos.
            Las nuevas alertas aparecerán en <strong>Decisiones humanas</strong> al terminar.
        </div>
    </div>
    """, unsafe_allow_html=True)

    col_btn, col_info = st.columns([1, 2])
    with col_btn:
        if st.button("🔄 Analizar noticias ahora", use_container_width=True, type="primary"):
            with st.spinner("Lanzando actualización en Databricks..."):
                ok, result = trigger_databricks_job()
            if ok:
                st.success(f"✅ Proceso iniciado correctamente (ID de ejecución: {result}). En unos minutos verás nuevas alertas.")
            else:
                st.error(f"No se pudo iniciar el proceso: {result}")

    with col_info:
        st.markdown("""
        <div style="font-size:0.78rem;color:#445566;font-family:'JetBrains Mono',monospace;
                    line-height:1.9;padding-top:0.5rem;">
            FUENTES QUE SE ANALIZARÁN:<br>
            <span style="color:#3a5568;">
            · BOE (Boletín Oficial del Estado)<br>
            · El País, El Mundo, ABC, La Vanguardia<br>
            · Público, elDiario, El Confidencial<br>
            · Poder Judicial, Transparencia.gob.es<br>
            · Civio, El Salto, RTVE, La Sexta<br>
            · Maldita.es, Newtral, EFE Verifica<br>
            · RTVE Verifica, Snopes, PolitiFact
            </span>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("---")
    st.markdown('<div class="section-label">Última actualización automática</div>', unsafe_allow_html=True)

    df_last = run_query("""
        SELECT max(ingested_at) AS ultima, count(*) AS total
        FROM arcas_raw.articles
    """)
    if not df_last.empty and df_last["ultima"].iloc[0]:
        ultima = str(df_last["ultima"].iloc[0])[:19].replace("T", " ")
        total  = int(df_last["total"].iloc[0])
        st.markdown(f"""
        <div style="font-size:0.85rem;color:#6a8099;">
            Última recarga: <strong style="color:#c8d8e8;">{ultima}</strong>
            · Total de noticias almacenadas: <strong style="color:#c8d8e8;">{total:,}</strong>
        </div>
        """, unsafe_allow_html=True)
