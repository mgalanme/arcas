"""
ARCAS v5 - Fondo papel, estilo periodístico, revisión de decisiones
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import requests, re, smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

st.set_page_config(
    page_title="ARCAS · Vigilancia Anticorrupción",
    page_icon="🔍", layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Playfair+Display:wght@400;600;700;900&family=Source+Serif+4:wght@300;400;600&family=JetBrains+Mono:wght@400;500&display=swap');

html, body, [class*="css"] { font-family: "Source Serif 4", Georgia, serif; }
.stApp { background: #f5f2ec; color: #1a1a1a; }

h1,h2,h3 { font-family:"Playfair Display",Georgia,serif; font-weight:700; color:#0d0d0d; }

/* Header */
.arcas-header {
    background: #1a1a1a; color: #f5f2ec;
    padding: 1.8rem 2.5rem; margin-bottom: 2rem;
    border-radius: 4px; position: relative;
}
.arcas-header::after {
    content:""; display:block; margin-top:0.8rem;
    border-bottom: 3px double #c8a96e;
}
.arcas-title {
    font-family:"Playfair Display",serif; font-size:2.2rem;
    font-weight:900; color:#f5f2ec; letter-spacing:-0.02em; margin:0;
}
.arcas-subtitle {
    font-family:"JetBrains Mono",monospace; font-size:0.68rem;
    color:#9a9080; text-transform:uppercase; letter-spacing:0.1em;
    margin-top:0.3rem;
}
.arcas-date {
    font-family:"JetBrains Mono",monospace; font-size:0.72rem;
    color:#c8a96e; margin-top:0.15rem;
}

/* KPIs */
.kpi-card {
    background:#fff; border:1px solid #ddd8ce;
    border-top: 3px solid var(--kc,#1a1a1a);
    padding:1.2rem 1.5rem; border-radius:2px;
}
.kpi-value { font-family:"Playfair Display",serif; font-size:2.8rem;
    font-weight:900; color:#0d0d0d; line-height:1; }
.kpi-label { font-family:"JetBrains Mono",monospace; font-size:0.62rem;
    color:#888; text-transform:uppercase; letter-spacing:0.1em; margin-top:0.3rem; }
.kpi-sub { font-size:0.75rem; color:#888; font-style:italic; margin-top:0.1rem; }

/* Alert cards */
.alert-card {
    background:#fff; border:1px solid #ddd8ce;
    border-left: 4px solid var(--ac,#888);
    padding:1rem 1.3rem; margin-bottom:0.8rem; border-radius:2px;
}
.alert-headline {
    font-family:"Playfair Display",serif; font-weight:700;
    font-size:0.95rem; color:#0d0d0d; margin-bottom:0.3rem; line-height:1.3;
}
.alert-byline {
    font-family:"JetBrains Mono",monospace; font-size:0.62rem;
    color:#888; letter-spacing:0.03em;
}

/* Badges */
.badge {
    display:inline-block; padding:0.1rem 0.4rem; border-radius:2px;
    font-family:"JetBrains Mono",monospace; font-size:0.6rem;
    font-weight:600; letter-spacing:0.06em; text-transform:uppercase;
    border:1px solid currentColor;
}
.b-A{color:#c0392b;background:#fdf2f0;} .b-B{color:#d35400;background:#fef5ec;}
.b-C{color:#8B6914;background:#fef9e7;} .b-D{color:#1a7a4a;background:#edfaf4;}
.b-E{color:#1a5276;background:#eaf4fb;} .b-F{color:#6c3483;background:#f5eef8;}
.b-pending{color:#666;background:#f5f5f5;border-color:#ccc;}
.b-approved{color:#1a7a4a;background:#edfaf4;}
.b-rejected{color:#c0392b;background:#fdf2f0;}
.b-escalated{color:#d35400;background:#fef5ec;}

/* Section labels */
.section-rule {
    font-family:"JetBrains Mono",monospace; font-size:0.62rem;
    color:#888; text-transform:uppercase; letter-spacing:0.12em;
    border-bottom:2px solid #1a1a1a; padding-bottom:0.3rem;
    margin:1.5rem 0 1rem 0;
}
.section-rule-light {
    font-family:"JetBrains Mono",monospace; font-size:0.62rem;
    color:#888; text-transform:uppercase; letter-spacing:0.12em;
    border-bottom:1px solid #ddd8ce; padding-bottom:0.3rem;
    margin:1.2rem 0 0.8rem 0;
}

/* Deep analysis box */
.analysis-box {
    background:#fffef9; border:1px solid #ddd8ce;
    border-left:3px solid #c8a96e;
    padding:1.2rem 1.5rem; border-radius:2px;
    font-size:0.87rem; line-height:1.75; color:#2a2a2a;
    font-family:"Source Serif 4",serif;
}

/* Sidebar */
section[data-testid="stSidebar"] { background:#1a1a1a !important; }
section[data-testid="stSidebar"] * { color:#c8c0b4 !important; }
section[data-testid="stSidebar"] hr { border-color:#333 !important; }

/* Buttons */
.stButton>button {
    background:#1a1a1a !important; color:#f5f2ec !important;
    border:1px solid #1a1a1a !important; border-radius:2px !important;
    font-family:"JetBrains Mono",monospace !important;
    font-size:0.7rem !important; letter-spacing:0.06em !important;
    text-transform:uppercase !important;
}
.stButton>button:hover { background:#333 !important; }

/* Inputs */
.stTextArea textarea, .stTextInput input {
    background:#fff !important; border:1px solid #ddd8ce !important;
    border-radius:2px !important; font-family:"Source Serif 4",serif !important;
    color:#1a1a1a !important;
}
.stSelectbox>div>div { background:#fff !important; border-color:#ddd8ce !important; }
.stCheckbox label { color:#1a1a1a !important; font-size:0.85rem !important; }
</style>
""", unsafe_allow_html=True)

# ── Constantes ────────────────────────────────────────────────────────────────
DATABRICKS_HOST   = st.secrets.get("DATABRICKS_HOST","").rstrip("/")
DATABRICKS_TOKEN  = st.secrets.get("DATABRICKS_TOKEN","")
DATABRICKS_HTTP   = st.secrets.get("DATABRICKS_HTTP_PATH","")
GROQ_API_KEY      = st.secrets.get("GROQ_API_KEY","")
DATABRICKS_JOB_ID = "998370935632321"

CAT_INFO = {
    "A":("Contratación Pública","#c0392b"),
    "B":("Enriquecimiento","#d35400"),
    "C":("Sesgo Judicial","#8B6914"),
    "D":("Desinformación","#1a7a4a"),
    "E":("Redes de Influencia","#1a5276"),
    "F":("Nepotismo","#6c3483"),
}
CAT_EXPLAIN = {
    "A":"Irregularidades en contratos o gasto público",
    "B":"Enriquecimiento ilícito o puertas giratorias",
    "C":"Trato judicial diferente según partido",
    "D":"Noticias falsas o manipuladas",
    "E":"Redes de influencia o financiación ilegal",
    "F":"Enchufismo o nepotismo en cargos",
}
LANGUAGES = {
    "🇪🇸 Español":"español","🇩🇪 Alemán":"alemán","🇫🇷 Francés":"francés",
    "🇮🇹 Italiano":"italiano","🇵🇱 Polaco":"polaco","🇷🇴 Rumano":"rumano",
}

# ── DB helpers ────────────────────────────────────────────────────────────────
@st.cache_resource
def get_conn():
    from databricks import sql
    return sql.connect(
        server_hostname=DATABRICKS_HOST.replace("https://",""),
        http_path=DATABRICKS_HTTP, access_token=DATABRICKS_TOKEN,
    )

def qry(q):
    try:
        conn = get_conn()
        with conn.cursor() as c:
            c.execute(q)
            cols = [d[0] for d in c.description]
            return pd.DataFrame(c.fetchall(), columns=cols)
    except Exception as e:
        st.error(f"Error: {e}"); return pd.DataFrame()

def update_alerts(ids, status):
    try:
        conn = get_conn()
        ids_str = ",".join(f"\'{i}\'" for i in ids)
        with conn.cursor() as c:
            c.execute(f"UPDATE arcas_processed.alerts SET status=\'{status}\' WHERE alert_id IN ({ids_str})")
        return True
    except Exception as e:
        st.error(str(e)); return False

def trigger_job():
    try:
        r = requests.post(f"{DATABRICKS_HOST}/api/2.1/jobs/run-now",
            headers={"Authorization":f"Bearer {DATABRICKS_TOKEN}","Content-Type":"application/json"},
            json={"job_id":int(DATABRICKS_JOB_ID)}, timeout=15)
        r.raise_for_status()
        return True, str(r.json().get("run_id","?"))
    except Exception as e:
        return False, str(e)

def validate_email(e):
    return bool(re.match(r"^[\w\.\+\-]+@[\w\-]+\.[a-z]{2,}$", e.strip(), re.IGNORECASE))

def send_email(to, subject, body):
    s = st.secrets.get("EMAIL_SENDER","")
    p = st.secrets.get("EMAIL_PASSWORD","")
    if not s or not p: return False,"Credenciales email no configuradas."
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"]=subject; msg["From"]=s; msg["To"]=to
        msg.attach(MIMEText(body,"plain","utf-8"))
        with smtplib.SMTP_SSL("smtp.gmail.com",465) as srv:
            srv.login(s,p); srv.sendmail(s,to,msg.as_string())
        return True,""
    except Exception as e: return False,str(e)

def groq_gen(prompt):
    try:
        import groq as g
        c = g.Groq(api_key=GROQ_API_KEY)
        r = c.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role":"user","content":prompt}], max_tokens=700)
        return r.choices[0].message.content.strip()
    except Exception as e: return f"[Error: {e}]"

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style="padding:1rem 0 1rem;">
        <div style="font-family:'Playfair Display',serif;font-weight:900;font-size:1.6rem;
                    color:#f5f2ec;">ARCAS</div>
        <div style="font-family:'JetBrains Mono',monospace;font-size:0.58rem;
                    color:#666;text-transform:uppercase;letter-spacing:0.1em;margin-top:0.1rem;">
            Vigilancia Anticorrupción</div>
        <div style="border-bottom:1px solid #333;margin:0.8rem 0;"></div>
    </div>
    """, unsafe_allow_html=True)
    page = st.radio("nav",[
        "🔄 Recargar información",
        "📊 Resumen general",
        "✅ Decisiones pendientes",
        "📋 Historial de decisiones",
        "✍️ Generar publicaciones",
    ], label_visibility="collapsed")
    st.markdown("---")
    st.markdown("""<div style="font-family:'JetBrains Mono',monospace;font-size:0.56rem;
        color:#444;text-transform:uppercase;letter-spacing:0.07em;line-height:2;">
        Fuentes<br><span style="color:#666;text-transform:none;letter-spacing:0;">
        El País · El Mundo · ABC<br>La Vanguardia · Público · elDiario<br>
        El Confidencial · OK Diario<br>La Razón · El Español · RTVE<br>
        Maldita · Newtral · EFE Verifica<br>
        Civio · Poder Judicial · AP News
        </span></div>""", unsafe_allow_html=True)

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="arcas-header">
    <div class="arcas-title">Sistema ARCAS</div>
    <div class="arcas-subtitle">Anti-Corruption &amp; Accountability Research System</div>
    <div class="arcas-date">{datetime.now().strftime("%A, %d de %B de %Y · %H:%M")}</div>
</div>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# PÁGINA 1 — RECARGAR INFORMACIÓN (primera en el menú)
# ══════════════════════════════════════════════════════════════════════════════
if page == "🔄 Recargar información":
    st.markdown('<div class="section-rule">Actualización de fuentes</div>', unsafe_allow_html=True)
    st.markdown("""
    El sistema recoge y analiza automáticamente las noticias cada día a las **09:30**.
    Si necesitas analizar noticias publicadas ahora mismo, usa el botón para lanzar
    el proceso en este momento. Tardará entre 3 y 8 minutos.
    """)

    col_btn, col_info = st.columns([1, 2])
    with col_btn:
        if st.button("🔄 Analizar noticias ahora", use_container_width=True):
            with st.spinner("Lanzando análisis..."):
                ok, result = trigger_job()
            if ok:
                st.success(f"Proceso iniciado. En unos minutos verás nuevas alertas. (ID: {result})")
            else:
                st.error(f"No se pudo iniciar: {result}")

    with col_info:
        df_last = qry("SELECT max(ingested_at) AS ultima, count(*) AS total FROM arcas_raw.articles WHERE source_type != 'gazette'")
        if not df_last.empty and df_last["ultima"].iloc[0]:
            ultima = str(df_last["ultima"].iloc[0])[:19].replace("T"," ")
            total  = int(df_last["total"].iloc[0])
            st.markdown(f"""
            <div style="background:#fff;border:1px solid #ddd8ce;padding:1rem 1.3rem;border-radius:2px;">
                <div style="font-family:'JetBrains Mono',monospace;font-size:0.65rem;color:#888;text-transform:uppercase;letter-spacing:0.08em;">
                    Última recarga</div>
                <div style="font-size:1.1rem;font-weight:600;color:#0d0d0d;margin-top:0.2rem;">{ultima}</div>
                <div style="font-size:0.8rem;color:#888;font-style:italic;">
                    {total:,} noticias de medios almacenadas</div>
            </div>
            """, unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# PÁGINA 2 — RESUMEN GENERAL
# ══════════════════════════════════════════════════════════════════════════════
elif page == "📊 Resumen general":
    df_al = qry("SELECT alert_id,category,status,confidence_score,source_name,title,created_at FROM arcas_processed.alerts ORDER BY created_at DESC")
    df_art = qry("SELECT count(*) AS n FROM arcas_raw.articles WHERE source_type != 'gazette'")
    total_art = int(df_art["n"].iloc[0]) if not df_art.empty else 0
    total_al  = len(df_al)
    pending   = len(df_al[df_al["status"]=="pending"])  if not df_al.empty else 0
    approved  = len(df_al[df_al["status"]=="approved"]) if not df_al.empty else 0

    c1,c2,c3,c4 = st.columns(4)
    for col,val,label,sub,color in [
        (c1,total_art,"Noticias analizadas","de medios de comunicación","#1a5276"),
        (c2,total_al,"Alertas detectadas","patrones identificados","#c0392b"),
        (c3,pending,"Pendientes de decisión","esperan tu revisión","#8B6914"),
        (c4,approved,"Decisiones tomadas","aprobadas","#1a7a4a"),
    ]:
        col.markdown(f"""
        <div class="kpi-card" style="--kc:{color};">
            <div class="kpi-value">{val}</div>
            <div class="kpi-label">{label}</div>
            <div class="kpi-sub">{sub}</div>
        </div>""", unsafe_allow_html=True)

    if not df_al.empty:
        st.markdown("<br>", unsafe_allow_html=True)
        cL, cR = st.columns([1,1])
        with cL:
            st.markdown('<div class="section-rule-light">Tipos de patrón detectado</div>', unsafe_allow_html=True)
            cc = df_al["category"].value_counts().reset_index()
            cc.columns=["cat","n"]
            cc["label"]=cc["cat"].map(lambda c:CAT_EXPLAIN.get(c,c))
            cc["color"]=cc["cat"].map(lambda c:CAT_INFO.get(c,("?","#888"))[1])
            fig=go.Figure(go.Bar(x=cc["n"],y=cc["label"],orientation="h",
                marker_color=cc["color"],text=cc["n"],textposition="outside"))
            fig.update_layout(paper_bgcolor="#f5f2ec",plot_bgcolor="#f5f2ec",
                font=dict(color="#555",family="JetBrains Mono",size=11),
                xaxis=dict(showgrid=False,zeroline=False,visible=False),
                yaxis=dict(showgrid=False),
                margin=dict(l=0,r=30,t=5,b=5),height=280)
            st.plotly_chart(fig,use_container_width=True)
        with cR:
            st.markdown('<div class="section-rule-light">Últimas noticias marcadas</div>', unsafe_allow_html=True)
            for _,row in df_al.head(6).iterrows():
                cat=str(row.get("category","?")); status=str(row.get("status",""))
                title=str(row.get("title",""))[:75]; source=str(row.get("source_name",""))
                conf=float(row.get("confidence_score",0)); color=CAT_INFO.get(cat,("?","#888"))[1]
                st.markdown(f"""
                <div class="alert-card" style="--ac:{color};">
                    <div class="alert-headline">{title}…</div>
                    <div class="alert-byline">
                        <span class="badge b-{cat}">{CAT_INFO.get(cat,("?",""))[0]}</span>
                        <span class="badge b-{status}">{status}</span>
                        · {source} · {conf:.0%}
                    </div>
                </div>""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# PÁGINA 3 — DECISIONES PENDIENTES
# ══════════════════════════════════════════════════════════════════════════════
elif page == "✅ Decisiones pendientes":
    st.markdown('<div class="section-rule">Noticias pendientes de revisión</div>', unsafe_allow_html=True)
    df = qry("""SELECT alert_id,category,status,confidence_score,nl_justification,
               source_name,title,content_url FROM arcas_processed.alerts
               WHERE status='pending' ORDER BY confidence_score DESC""")
    if df.empty:
        st.success("✅ No hay noticias pendientes de revisión.")
    else:
        st.markdown(f"**{len(df)} noticia(s) esperan tu decisión**")
        with st.expander("⚡ Actuar sobre varias a la vez"):
            sel_ids=[]
            for _,row in df.iterrows():
                aid=str(row["alert_id"]); cat=str(row["category"])
                t=str(row["title"])[:65]; conf=float(row["confidence_score"])
                if st.checkbox(f"[{CAT_INFO.get(cat,('?',''))[0]}] {t}… ({conf:.0%})",key=f"chk_{aid}"):
                    sel_ids.append(aid)
            if sel_ids:
                st.markdown(f"**{len(sel_ids)} seleccionadas**")
                ca,cr,ce=st.columns(3)
                with ca:
                    if st.button("✅ Aprobar seleccionadas",key="bulk_a"):
                        if update_alerts(sel_ids,"approved"): st.success("Aprobadas."); st.rerun()
                with cr:
                    if st.button("❌ Rechazar seleccionadas",key="bulk_r"):
                        if update_alerts(sel_ids,"rejected"): st.warning("Rechazadas."); st.rerun()
                with ce:
                    if st.button("⬆️ Escalar seleccionadas",key="bulk_e"):
                        if update_alerts(sel_ids,"escalated"): st.info("Escaladas."); st.rerun()
        st.markdown("---")
        for _,row in df.iterrows():
            cat=str(row["category"]); conf=float(row["confidence_score"])
            title=str(row["title"]); source=str(row["source_name"])
            url=str(row["content_url"]); analysis=str(row["nl_justification"])
            alert_id=str(row["alert_id"])
            color=CAT_INFO.get(cat,("?","#888"))[1]
            with st.expander(f"[{CAT_INFO.get(cat,('?',''))[0]}] {title[:70]}… · {conf:.0%}"):
                st.markdown(f"""
                <div class="alert-byline" style="margin-bottom:0.8rem;">
                    <span class="badge b-{cat}">{CAT_INFO.get(cat,('?',''))[0]}</span>
                    · {CAT_EXPLAIN.get(cat,'')} · {source} · {conf:.0%} certeza
                </div>""", unsafe_allow_html=True)
                if url: st.markdown(f"🔗 [Ver noticia original]({url})")
                st.markdown("**Análisis del sistema:**")
                st.markdown(f'<div class="analysis-box">{analysis}</div>', unsafe_allow_html=True)
                st.markdown("<br>", unsafe_allow_html=True)
                notes=st.text_area("Notas",key=f"n_{alert_id}",height=60,label_visibility="collapsed",placeholder="Añade contexto opcional...")
                ca2,cr2,ce2,cem=st.columns(4)
                with ca2:
                    if st.button("✅ Publicar",key=f"a_{alert_id}"):
                        if update_alerts([alert_id],"approved"): st.success("Aprobada."); st.rerun()
                with cr2:
                    if st.button("❌ Descartar",key=f"r_{alert_id}"):
                        if update_alerts([alert_id],"rejected"): st.warning("Descartada."); st.rerun()
                with ce2:
                    if st.button("⬆️ Escalar",key=f"e_{alert_id}"):
                        if update_alerts([alert_id],"escalated"): st.info("Escalada."); st.rerun()
                with cem:
                    if st.button("📧 Email",key=f"em_{alert_id}"):
                        st.session_state[f"show_em_{alert_id}"]=True
                if st.session_state.get(f"show_em_{alert_id}"):
                    ea=st.text_input("Email",key=f"ea_{alert_id}",placeholder="nombre@dominio.com")
                    if st.button("Enviar",key=f"send_{alert_id}"):
                        if not validate_email(ea): st.error("Email no válido.")
                        else:
                            body=f"ALERTA ARCAS — {CAT_INFO.get(cat,('?',''))[0]}\n{'='*50}\n\nTitular: {title}\nFuente: {source}\nURL: {url}\n\nAnálisis:\n{analysis}\n\nNotas: {notes or '(ninguna)'}"
                            ok,err=send_email(ea,f"[ARCAS] {title[:50]}",body)
                            if ok: st.success(f"Enviado a {ea}"); st.session_state[f"show_em_{alert_id}"]=False
                            else: st.error(f"Error: {err}")

# ══════════════════════════════════════════════════════════════════════════════
# PÁGINA 4 — HISTORIAL DE DECISIONES
# ══════════════════════════════════════════════════════════════════════════════
elif page == "📋 Historial de decisiones":
    st.markdown('<div class="section-rule">Decisiones tomadas — revisión y rectificación</div>', unsafe_allow_html=True)
    st.markdown("Aquí puedes revisar y cambiar decisiones ya tomadas.")

    df_hist = qry("""SELECT alert_id,category,status,confidence_score,
                   nl_justification,source_name,title,content_url
                   FROM arcas_processed.alerts
                   WHERE status != 'pending'
                   ORDER BY status, confidence_score DESC""")

    if df_hist.empty:
        st.info("Todavía no hay decisiones tomadas.")
    else:
        # Agrupar por estado
        for status_val, label, icon in [
            ("approved","Aprobadas — listas para publicar","✅"),
            ("rejected","Descartadas","❌"),
            ("escalated","Escaladas a nivel superior","⬆️"),
        ]:
            grupo = df_hist[df_hist["status"]==status_val]
            if grupo.empty: continue

            st.markdown(f"""
            <div class="section-rule-light">{icon} {label} ({len(grupo)})</div>
            """, unsafe_allow_html=True)

            for _,row in grupo.iterrows():
                cat=str(row["category"]); conf=float(row["confidence_score"])
                title=str(row["title"]); source=str(row["source_name"])
                url=str(row["content_url"]); analysis=str(row["nl_justification"])
                alert_id=str(row["alert_id"])
                color=CAT_INFO.get(cat,("?","#888"))[1]

                with st.expander(f"[{CAT_INFO.get(cat,('?',''))[0]}] {title[:65]}… · {conf:.0%}"):
                    st.markdown(f"""
                    <div class="alert-byline" style="margin-bottom:0.6rem;">
                        <span class="badge b-{cat}">{CAT_INFO.get(cat,('?',''))[0]}</span>
                        <span class="badge b-{status_val}">{status_val}</span>
                        · {source}
                    </div>""", unsafe_allow_html=True)
                    if url: st.markdown(f"🔗 [Ver noticia original]({url})")
                    with st.expander("Ver análisis completo"):
                        st.markdown(f'<div class="analysis-box">{analysis}</div>', unsafe_allow_html=True)

                    st.markdown("**Cambiar decisión:**")
                    opciones = [s for s in ["approved","rejected","escalated","pending"] if s != status_val]
                    labels   = {"approved":"✅ Aprobar","rejected":"❌ Rechazar",
                                "escalated":"⬆️ Escalar","pending":"↩️ Volver a pendiente"}
                    cols = st.columns(len(opciones))
                    for i, nueva in enumerate(opciones):
                        with cols[i]:
                            if st.button(labels[nueva], key=f"hist_{alert_id}_{nueva}"):
                                if update_alerts([alert_id], nueva):
                                    st.success(f"Cambiada a: {nueva}")
                                    st.rerun()

# ══════════════════════════════════════════════════════════════════════════════
# PÁGINA 5 — GENERAR PUBLICACIONES
# ══════════════════════════════════════════════════════════════════════════════
elif page == "✍️ Generar publicaciones":
    st.markdown('<div class="section-rule">Redactar publicación para redes sociales</div>', unsafe_allow_html=True)
    df = qry("""SELECT alert_id,category,confidence_score,nl_justification,
               source_name,title,content_url FROM arcas_processed.alerts
               WHERE status IN ('approved','pending') ORDER BY confidence_score DESC LIMIT 50""")
    if df.empty:
        st.info("No hay alertas disponibles.")
    else:
        cS,cL=st.columns([2,1])
        with cS:
            options={f"[{row['category']}] {str(row['title'])[:60]}…":row for _,row in df.iterrows()}
            sel_label=st.selectbox("Noticia a publicar",list(options.keys()))
            sel=options[sel_label]
        with cL:
            lang_label=st.selectbox("Idioma",list(LANGUAGES.keys()))
            lang_name=LANGUAGES[lang_label]

        cP,cT=st.columns(2)
        with cP:
            platform=st.radio("Red social",["Facebook / LinkedIn","X (Twitter)","Instagram"],horizontal=True)
        with cT:
            tone=st.radio("Tono",["Informativo","Analítico","Urgente","Pedagógico"],horizontal=True)

        char_limits={"Facebook / LinkedIn":3000,"X (Twitter)":280,"Instagram":2200}
        char_limit=char_limits[platform]

        st.markdown('<div class="section-rule-light">Instrucciones al redactor</div>', unsafe_allow_html=True)
        st.caption("Personaliza cómo quieres que se redacte la publicación.")
        DEFAULT_PROMPT="""Eres un periodista de investigación riguroso y políticamente neutral.
Informa sobre hechos concretos, no sobre generalidades.
Nunca acuses directamente — señala patrones y hechos verificables.
Usa lenguaje accesible, sin jerga técnica.
Indica explícitamente si hay pruebas o si la información se basa en declaraciones sin respaldo.
Invita a la ciudadanía a investigar y verificar por su cuenta."""
        master=st.text_area("instrucciones",value=DEFAULT_PROMPT,height=140,label_visibility="collapsed")

        if st.button("✍️ Redactar publicación",use_container_width=True):
            cat=sel["category"]; cat_name=CAT_INFO.get(cat,(cat,""))[0]
            justif=str(sel["nl_justification"]); title=str(sel["title"])
            source=str(sel["source_name"]); conf=float(sel["confidence_score"])
            full_prompt=f"""INSTRUCCIONES DEL OPERADOR:\n{master}\n\n---\nNOTICIA ANALIZADA:\n- Tipo: {cat_name}\n- Fuente: {source}\n- Titular: {title}\n- Análisis: {justif[:600]}\n- Certeza: {conf:.0%}\n\n---\nREQUISITOS:\n- Idioma: {lang_name}\n- Red social: {platform} (máx {char_limit} caracteres)\n- Tono: {tone}\n- Formato: Markdown con **negrita**, *cursiva* y #hashtags\n- NO menciones IA ni sistemas automatizados\n- Céntrate en ESTA noticia específica\n\nEscribe únicamente el texto de la publicación en Markdown."""
            with st.spinner("Redactando..."):
                post_md=groq_gen(full_prompt)
            st.markdown('<div class="section-rule-light">Texto Markdown (raw)</div>', unsafe_allow_html=True)
            st.code(post_md, language="markdown")
            st.markdown('<div class="section-rule-light">Vista previa</div>', unsafe_allow_html=True)
            st.markdown(post_md)
            char_count=len(post_md)
            color="#1a7a4a" if char_count<=char_limit else "#c0392b"
            st.markdown(f'<span style="font-family:JetBrains Mono,monospace;font-size:0.68rem;color:{color};">{char_count}/{char_limit} caracteres</span>',unsafe_allow_html=True)
            c1,c2=st.columns(2)
            with c1: st.download_button("⬇️ .md",post_md,f"arcas_{lang_name}_{cat}.md","text/markdown")
            with c2: st.download_button("⬇️ .txt",post_md,f"arcas_{lang_name}_{cat}.txt","text/plain")
