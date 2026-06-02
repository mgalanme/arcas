"""
ARCAS - Anti-Corruption & Accountability System
Streamlit Community Cloud dashboard
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import json, os, re

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="ARCAS · Vigilancia Anticorrupción",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap');

html, body, [class*="css"] {
    font-family: 'Syne', sans-serif;
}
.stApp {
    background: #0a0d12;
    color: #e8e4dc;
}
h1, h2, h3 {
    font-family: 'Syne', sans-serif;
    font-weight: 800;
    letter-spacing: -0.03em;
}
.arcas-header {
    background: linear-gradient(135deg, #0f1923 0%, #1a2535 100%);
    border: 1px solid #2a3a4a;
    border-radius: 12px;
    padding: 2rem 2.5rem;
    margin-bottom: 2rem;
    position: relative;
    overflow: hidden;
}
.arcas-header::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 3px;
    background: linear-gradient(90deg, #e63946, #f4a261, #2a9d8f);
}
.arcas-title {
    font-size: 2.2rem;
    font-weight: 800;
    color: #f0ebe0;
    margin: 0;
    letter-spacing: -0.04em;
}
.arcas-subtitle {
    color: #8899aa;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.75rem;
    margin-top: 0.3rem;
    letter-spacing: 0.05em;
    text-transform: uppercase;
}
.metric-card {
    background: #111820;
    border: 1px solid #1e2d3d;
    border-radius: 10px;
    padding: 1.2rem 1.5rem;
    transition: border-color 0.2s;
}
.metric-card:hover { border-color: #2a4a6a; }
.metric-value {
    font-size: 2.4rem;
    font-weight: 800;
    color: #f0ebe0;
    line-height: 1;
}
.metric-label {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.7rem;
    color: #556677;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    margin-top: 0.4rem;
}
.alert-card {
    background: #111820;
    border: 1px solid #1e2d3d;
    border-left: 4px solid #e63946;
    border-radius: 8px;
    padding: 1.2rem 1.5rem;
    margin-bottom: 1rem;
    transition: all 0.2s;
}
.alert-card:hover { border-color: #2a4a6a; border-left-color: #f4a261; }
.alert-card.cat-a { border-left-color: #e63946; }
.alert-card.cat-b { border-left-color: #f4a261; }
.alert-card.cat-c { border-left-color: #e9c46a; }
.alert-card.cat-d { border-left-color: #2a9d8f; }
.alert-card.cat-e { border-left-color: #a8dadc; }
.alert-card.cat-f { border-left-color: #9b72cf; }
.alert-title {
    font-weight: 700;
    font-size: 0.95rem;
    color: #e8e4dc;
    margin-bottom: 0.4rem;
}
.alert-meta {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.68rem;
    color: #556677;
    letter-spacing: 0.03em;
}
.badge {
    display: inline-block;
    padding: 0.15rem 0.5rem;
    border-radius: 4px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.65rem;
    font-weight: 500;
    letter-spacing: 0.05em;
    text-transform: uppercase;
}
.badge-a { background: #3d1a1f; color: #e63946; }
.badge-b { background: #3d2a1a; color: #f4a261; }
.badge-c { background: #3d3020; color: #e9c46a; }
.badge-d { background: #1a3d3a; color: #2a9d8f; }
.badge-e { background: #1a303d; color: #a8dadc; }
.badge-f { background: #2d1a3d; color: #9b72cf; }
.badge-pending { background: #2a3a4a; color: #8899aa; }
.badge-approved { background: #1a3d2a; color: #2a9d8f; }
.badge-rejected { background: #3d1a1f; color: #e63946; }
.section-title {
    font-size: 0.7rem;
    font-family: 'JetBrains Mono', monospace;
    color: #556677;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    border-bottom: 1px solid #1e2d3d;
    padding-bottom: 0.5rem;
    margin-bottom: 1.5rem;
}
.post-output {
    background: #0d1520;
    border: 1px solid #2a3a4a;
    border-radius: 8px;
    padding: 1.5rem;
    font-size: 0.9rem;
    line-height: 1.7;
    color: #c8c0b4;
    font-family: 'Syne', sans-serif;
    white-space: pre-wrap;
}
.stButton > button {
    background: #1e2d3d !important;
    color: #e8e4dc !important;
    border: 1px solid #2a3a4a !important;
    border-radius: 6px !important;
    font-family: 'JetBrains Mono', monospace !important;
    font-size: 0.75rem !important;
    letter-spacing: 0.05em !important;
    text-transform: uppercase !important;
    transition: all 0.2s !important;
}
.stButton > button:hover {
    border-color: #4a6a8a !important;
    background: #263545 !important;
}
.stSelectbox > div > div {
    background: #111820 !important;
    border-color: #1e2d3d !important;
    color: #e8e4dc !important;
}
.stTextArea textarea {
    background: #111820 !important;
    border-color: #1e2d3d !important;
    color: #e8e4dc !important;
    font-family: 'JetBrains Mono', monospace !important;
}
.sidebar .sidebar-content { background: #0d1520 !important; }
</style>
""", unsafe_allow_html=True)

# ── Databricks connection ─────────────────────────────────────────────────────
@st.cache_resource
def get_db_connection():
    try:
        from databricks import sql
        conn = sql.connect(
            server_hostname = st.secrets["DATABRICKS_HOST"].replace("https://", ""),
            http_path       = st.secrets["DATABRICKS_HTTP_PATH"],
            access_token    = st.secrets["DATABRICKS_TOKEN"],
        )
        return conn
    except Exception as e:
        st.error(f"Databricks connection failed: {e}")
        return None

def run_query(query: str) -> pd.DataFrame:
    conn = get_db_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
            return pd.DataFrame(rows, columns=cols)
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()

def update_alert(alert_id: str, status: str, notes: str = "") -> bool:
    conn = get_db_connection()
    if conn is None:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE arcas_processed.alerts SET status = ? WHERE alert_id = ?",
                [status, alert_id]
            )
        return True
    except Exception as e:
        st.error(f"Update error: {e}")
        return False

# ── Groq helper ───────────────────────────────────────────────────────────────
def groq_generate(prompt: str) -> str:
    try:
        import groq as groq_sdk
        client = groq_sdk.Groq(api_key=st.secrets["GROQ_API_KEY"])
        response = client.chat.completions.create(
            model    = "llama-3.3-70b-versatile",
            messages = [{"role": "user", "content": prompt}],
            max_tokens = 600,
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        return f"[Groq error: {e}]"

# ── Category metadata ─────────────────────────────────────────────────────────
CAT_INFO = {
    "A": ("Contratación Pública",   "#e63946"),
    "B": ("Enriquecimiento",        "#f4a261"),
    "C": ("Sesgo Judicial",         "#e9c46a"),
    "D": ("Desinformación",         "#2a9d8f"),
    "E": ("Redes de Influencia",    "#a8dadc"),
    "F": ("Nepotismo",              "#9b72cf"),
}

LANGUAGES = {
    "🇪🇸 Español":   "Spanish",
    "🇩🇪 Alemán":    "German",
    "🇫🇷 Francés":   "French",
    "🇮🇹 Italiano":  "Italian",
    "🇵🇱 Polaco":    "Polish",
    "🇷🇴 Rumano":    "Romanian",
}

# ── Sidebar navigation ────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style="padding: 1rem 0 1.5rem 0;">
        <div style="font-family: 'Syne', sans-serif; font-weight: 800; font-size: 1.4rem;
                    color: #f0ebe0; letter-spacing: -0.03em;">ARCAS</div>
        <div style="font-family: 'JetBrains Mono', monospace; font-size: 0.6rem;
                    color: #556677; text-transform: uppercase; letter-spacing: 0.1em;
                    margin-top: 0.2rem;">Anti-Corruption System</div>
    </div>
    """, unsafe_allow_html=True)

    page = st.radio(
        "Navegación",
        ["📊 Dashboard", "⚖️ Cola HITL", "✍️ Generador de Posts"],
        label_visibility="collapsed",
    )
    st.markdown("---")
    st.markdown("""
    <div style="font-family: 'JetBrains Mono', monospace; font-size: 0.6rem;
                color: #334455; text-transform: uppercase; letter-spacing: 0.08em;">
        Fuentes monitorizadas<br>
        <span style="color: #556677;">BOE · El País · El Mundo · ABC<br>
        Público · elDiario · OKDiario<br>
        Maldita.es · Newtral · Snopes</span>
    </div>
    """, unsafe_allow_html=True)

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="arcas-header">
    <div class="arcas-title">Sistema ARCAS</div>
    <div class="arcas-subtitle">Anti-Corruption &amp; Accountability Research System
    · {datetime.now().strftime('%d %b %Y %H:%M')}</div>
</div>
""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 1 — DASHBOARD
# ═══════════════════════════════════════════════════════════════════════════════
if page == "📊 Dashboard":
    st.markdown('<div class="section-title">Resumen ejecutivo</div>', unsafe_allow_html=True)

    df_alerts = run_query("""
        SELECT alert_id, category, status, confidence_score,
               source_name, title, coalesce(title, title) AS title_es, created_at
        FROM arcas_processed.alerts
        ORDER BY created_at DESC
    """)

    df_articles = run_query("""
        SELECT count(*) AS total FROM arcas_raw.articles
    """)

    total_articles = int(df_articles["total"].iloc[0]) if not df_articles.empty else 0
    total_alerts   = len(df_alerts)
    pending        = len(df_alerts[df_alerts["status"] == "pending"]) if not df_alerts.empty else 0
    approved       = len(df_alerts[df_alerts["status"] == "approved"]) if not df_alerts.empty else 0

    c1, c2, c3, c4 = st.columns(4)
    for col, val, label in [
        (c1, total_articles, "Artículos en Delta"),
        (c2, total_alerts,   "Alertas generadas"),
        (c3, pending,        "Pendientes HITL"),
        (c4, approved,       "Aprobadas"),
    ]:
        col.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{val}</div>
            <div class="metric-label">{label}</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    if not df_alerts.empty:
        col_chart, col_list = st.columns([1, 1])

        with col_chart:
            st.markdown('<div class="section-title">Distribución por categoría</div>', unsafe_allow_html=True)
            cat_counts = df_alerts["category"].value_counts().reset_index()
            cat_counts.columns = ["category", "count"]
            cat_counts["label"] = cat_counts["category"].map(
                lambda c: CAT_INFO.get(c, (c, "#556677"))[0]
            )
            cat_counts["color"] = cat_counts["category"].map(
                lambda c: CAT_INFO.get(c, (c, "#556677"))[1]
            )
            fig = go.Figure(go.Bar(
                x=cat_counts["count"],
                y=cat_counts["label"],
                orientation="h",
                marker_color=cat_counts["color"],
                text=cat_counts["count"],
                textposition="outside",
            ))
            fig.update_layout(
                paper_bgcolor="#0a0d12", plot_bgcolor="#0a0d12",
                font=dict(color="#8899aa", family="JetBrains Mono, monospace", size=11),
                xaxis=dict(showgrid=False, zeroline=False, visible=False),
                yaxis=dict(showgrid=False),
                margin=dict(l=0, r=30, t=10, b=10),
                height=280,
            )
            st.plotly_chart(fig, use_container_width=True)

        with col_list:
            st.markdown('<div class="section-title">Últimas alertas</div>', unsafe_allow_html=True)
            for _, row in df_alerts.head(5).iterrows():
                cat   = str(row.get("category", "?"))
                badge = f'<span class="badge badge-{cat.lower()}">{cat}</span>'
                status_cls = str(row.get("status", "pending"))
                status_badge = f'<span class="badge badge-{status_cls}">{status_cls}</span>'
                title = str(row.get("title", ""))[:80]
                source = str(row.get("source_name", ""))
                conf   = float(row.get("confidence_score", 0))
                st.markdown(f"""
                <div class="alert-card cat-{cat.lower()}">
                    <div class="alert-title">{title}…</div>
                    <div class="alert-meta">{badge} {status_badge}
                    · {source} · conf {conf:.2f}</div>
                </div>
                """, unsafe_allow_html=True)
    else:
        st.info("No hay alertas en Databricks todavía. El Job diario se ejecuta a las 09:30 CET.")

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 2 — COLA HITL
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "⚖️ Cola HITL":
    st.markdown('<div class="section-title">Alertas pendientes de revisión humana</div>', unsafe_allow_html=True)

    df = run_query("""
        SELECT alert_id, category, status, confidence_score,
               nl_justification, source_name, title, content_url, created_at
        FROM arcas_processed.alerts
        WHERE status = 'pending'
        ORDER BY confidence_score DESC
    """)

    if df.empty:
        st.success("✅ No hay alertas pendientes. La cola está vacía.")
    else:
        st.markdown(f"**{len(df)} alerta(s) pendiente(s) de revisión**")

        for idx, row in df.iterrows():
            cat     = str(row.get("category", "?"))
            conf    = float(row.get("confidence_score", 0))
            title   = str(row.get("title", "Sin título"))
            source  = str(row.get("source_name", ""))
            url     = str(row.get("content_url", ""))
            justif  = str(row.get("nl_justification", ""))
            alert_id = str(row.get("alert_id", ""))
            cat_name = CAT_INFO.get(cat, (cat, "#556677"))[0]

            with st.expander(f"[{cat}] {title[:70]}… · conf {conf:.2f}"):
                st.markdown(f"""
                <div class="alert-meta">
                    <span class="badge badge-{cat.lower()}">{cat} · {cat_name}</span>
                    &nbsp;·&nbsp; {source}
                    &nbsp;·&nbsp; Confianza: <strong>{conf:.2%}</strong>
                </div>
                """, unsafe_allow_html=True)

                if url:
                    st.markdown(f"🔗 [Fuente original]({url})")

                st.markdown("**Justificación del agente:**")
                st.markdown(f'<div class="post-output">{justif}</div>', unsafe_allow_html=True)

                st.markdown("<br>", unsafe_allow_html=True)
                notes = st.text_area(
                    "Notas del operador (opcional)",
                    key=f"notes_{alert_id}",
                    placeholder="Añade contexto o modificaciones...",
                    height=80,
                )

                col_a, col_r, col_e = st.columns(3)
                with col_a:
                    if st.button("✅ Aprobar", key=f"approve_{alert_id}"):
                        if update_alert(alert_id, "approved", notes):
                            st.success("Alerta aprobada.")
                            st.rerun()
                with col_r:
                    if st.button("❌ Rechazar", key=f"reject_{alert_id}"):
                        if update_alert(alert_id, "rejected", notes):
                            st.warning("Alerta rechazada.")
                            st.rerun()
                with col_e:
                    if st.button("⬆️ Escalar", key=f"escalate_{alert_id}"):
                        if update_alert(alert_id, "escalated", notes):
                            st.info("Alerta escalada a nivel 2.")
                            st.rerun()

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 3 — GENERADOR DE POSTS
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "✍️ Generador de Posts":
    st.markdown('<div class="section-title">Generador de narrativa multiidioma</div>', unsafe_allow_html=True)

    df = run_query("""
        SELECT alert_id, category, confidence_score,
               nl_justification, source_name, title, content_url
        FROM arcas_processed.alerts
        WHERE status IN ('approved', 'pending')
        ORDER BY confidence_score DESC
        LIMIT 50
    """)

    if df.empty:
        st.info("No hay alertas disponibles para generar posts.")
    else:
        col_sel, col_lang = st.columns([2, 1])
        with col_sel:
            options = {
                f"[{row['category']}] {str(row['title'])[:65]}…": row
                for _, row in df.iterrows()
            }
            selected_label = st.selectbox("Seleccionar alerta", list(options.keys()))
            selected = options[selected_label]
        with col_lang:
            lang_label = st.selectbox("Idioma del post", list(LANGUAGES.keys()))
            lang_name  = LANGUAGES[lang_label]

        col_plat, col_tone = st.columns(2)
        with col_plat:
            platform = st.radio(
                "Red social",
                ["Facebook / LinkedIn", "X (Twitter)", "Instagram"],
                horizontal=True,
            )
        with col_tone:
            tone = st.select_slider(
                "Tono",
                options=["Informativo", "Analítico", "Urgente", "Pedagógico"],
                value="Informativo",
            )

        char_limits = {
            "Facebook / LinkedIn": 3000,
            "X (Twitter)":         280,
            "Instagram":           2200,
        }
        char_limit = char_limits[platform]

        st.markdown('<div class="section-title" style="margin-top:1.5rem;">Prompt maestro — instrucciones al modelo</div>', unsafe_allow_html=True)

        DEFAULT_MASTER_PROMPT = """Eres un periodista de investigación riguroso y políticamente neutral.
Tu misión es informar al público sobre patrones que merecen atención cívica.
Nunca acuses directamente a personas ni partidos.
Usa un lenguaje accesible, sin jerga técnica ni legal.
Incluye siempre una llamada a la reflexión ciudadana al final del post."""

        master_prompt = st.text_area(
            "prompt_maestro",
            value=DEFAULT_MASTER_PROMPT,
            height=140,
            label_visibility="collapsed",
            placeholder="Escribe aquí tus instrucciones maestras para la generación del post...",
        )

        if st.button("🖊️ Generar post en Markdown", use_container_width=True):
            cat      = selected["category"]
            cat_name = CAT_INFO.get(cat, (cat, "#556677"))[0]
            justif   = selected["nl_justification"]
            title    = selected["title"]
            source   = selected["source_name"]
            conf     = float(selected["confidence_score"])

            full_prompt = f"""INSTRUCCIONES DEL OPERADOR:
{master_prompt}

---
DATOS DE LA ALERTA:
- Patrón: {cat_name} (Categoría {cat})
- Fuente: {source}
- Titular: {title}
- Análisis: {justif[:500]}
- Confianza: {conf:.0%}

---
REQUISITOS:
- Idioma: {lang_name}
- Plataforma: {platform} (máximo {char_limit} caracteres)
- Tono: {tone}
- Formato de salida: Markdown (usa **negrita**, *cursiva*, emojis y #hashtags)
- NO menciones IA, algoritmos ni sistemas automatizados
- Factual y neutral — señala patrones, nunca acuses

Escribe únicamente el texto del post en Markdown."""

            with st.spinner("Generando post..."):
                post_md = groq_generate(full_prompt)

            st.markdown("**Texto Markdown (raw):**")
            st.markdown(f'<div class="post-output">{post_md}</div>', unsafe_allow_html=True)

            st.markdown("**Renderizado:**")
            st.markdown(post_md)

            char_count = len(post_md)
            color = "#2a9d8f" if char_count <= char_limit else "#e63946"
            st.markdown(
                f'<div class="alert-meta" style="margin-top:0.5rem; color:{color};">{char_count} / {char_limit} caracteres</div>',
                unsafe_allow_html=True,
            )

            col_dl1, col_dl2 = st.columns(2)
            with col_dl1:
                st.download_button(
                    "⬇️ Descargar .md",
                    post_md,
                    file_name=f"arcas_post_{lang_name.lower()}_{cat}.md",
                    mime="text/markdown",
                )
            with col_dl2:
                st.download_button(
                    "⬇️ Descargar .txt",
                    post_md,
                    file_name=f"arcas_post_{lang_name.lower()}_{cat}.txt",
                    mime="text/plain",
                )
