"""
ARCAS v6 - Cuadro de mandos, filtros por fecha/topic, prompt socrático
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import requests, re, smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta

st.set_page_config(
    page_title="ARCAS · Vigilancia Anticorrupción",
    page_icon="🔍", layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Playfair+Display:wght@400;600;700;900&family=Source+Serif+4:wght@300;400;600&family=JetBrains+Mono:wght@400;500&display=swap');

html,body,[class*="css"]{font-family:"Source Serif 4",Georgia,serif;}
.stApp{background:#f5f2ec;color:#1a1a1a;}
h1,h2,h3{font-family:"Playfair Display",Georgia,serif;font-weight:700;color:#0d0d0d;}

.arcas-header{background:#1a1a1a;color:#f5f2ec;padding:1.8rem 2.5rem;
    margin-bottom:2rem;border-radius:4px;position:relative;}
.arcas-header::after{content:"";display:block;margin-top:0.8rem;
    border-bottom:3px double #c8a96e;}
.arcas-title{font-family:"Playfair Display",serif;font-size:2.2rem;
    font-weight:900;color:#f5f2ec;letter-spacing:-0.02em;margin:0;}
.arcas-subtitle{font-family:"JetBrains Mono",monospace;font-size:0.68rem;
    color:#9a9080;text-transform:uppercase;letter-spacing:0.1em;margin-top:0.3rem;}
.arcas-date{font-family:"JetBrains Mono",monospace;font-size:0.72rem;
    color:#c8a96e;margin-top:0.15rem;}

.kpi-card{background:#fff;border:1px solid #ddd8ce;
    border-top:3px solid var(--kc,#1a1a1a);padding:1.2rem 1.5rem;border-radius:2px;}
.kpi-value{font-family:"Playfair Display",serif;font-size:2.8rem;
    font-weight:900;color:#0d0d0d;line-height:1;}
.kpi-label{font-family:"JetBrains Mono",monospace;font-size:0.62rem;
    color:#888;text-transform:uppercase;letter-spacing:0.1em;margin-top:0.3rem;}
.kpi-sub{font-size:0.75rem;color:#888;font-style:italic;margin-top:0.1rem;}

.alert-card{background:#fff;border:1px solid #ddd8ce;
    border-left:4px solid var(--ac,#888);
    padding:1rem 1.3rem;margin-bottom:0.8rem;border-radius:2px;}
.alert-headline{font-family:"Playfair Display",serif;font-weight:700;
    font-size:0.95rem;color:#0d0d0d;margin-bottom:0.3rem;line-height:1.3;}
.alert-byline{font-family:"JetBrains Mono",monospace;font-size:0.62rem;
    color:#888;letter-spacing:0.03em;}

.badge{display:inline-block;padding:0.1rem 0.4rem;border-radius:2px;
    font-family:"JetBrains Mono",monospace;font-size:0.6rem;
    font-weight:600;letter-spacing:0.06em;text-transform:uppercase;
    border:1px solid currentColor;}
.b-A{color:#c0392b;background:#fdf2f0;} .b-B{color:#d35400;background:#fef5ec;}
.b-C{color:#8B6914;background:#fef9e7;} .b-D{color:#1a7a4a;background:#edfaf4;}
.b-E{color:#1a5276;background:#eaf4fb;} .b-F{color:#6c3483;background:#f5eef8;}
.b-pending{color:#666;background:#f5f5f5;border-color:#ccc;}
.b-approved{color:#1a7a4a;background:#edfaf4;}
.b-rejected{color:#c0392b;background:#fdf2f0;}
.b-escalated{color:#d35400;background:#fef5ec;}
.topic-pill{display:inline-block;padding:0.1rem 0.5rem;border-radius:10px;
    font-family:"JetBrains Mono",monospace;font-size:0.58rem;
    font-weight:600;letter-spacing:0.05em;text-transform:uppercase;
    background:#f0ede6;color:#555;border:1px solid #ddd8ce;margin-left:0.3rem;}

.section-rule{font-family:"JetBrains Mono",monospace;font-size:0.62rem;
    color:#888;text-transform:uppercase;letter-spacing:0.12em;
    border-bottom:2px solid #1a1a1a;padding-bottom:0.3rem;margin:1.5rem 0 1rem 0;}
.section-rule-light{font-family:"JetBrains Mono",monospace;font-size:0.62rem;
    color:#888;text-transform:uppercase;letter-spacing:0.12em;
    border-bottom:1px solid #ddd8ce;padding-bottom:0.3rem;margin:1.2rem 0 0.8rem 0;}
.analysis-box{background:#fffef9;border:1px solid #ddd8ce;
    border-left:3px solid #c8a96e;padding:1.2rem 1.5rem;border-radius:2px;
    font-size:0.87rem;line-height:1.75;color:#2a2a2a;font-family:"Source Serif 4",serif;}

section[data-testid="stSidebar"]{background:#1a1a1a !important;}
section[data-testid="stSidebar"] *{color:#c8c0b4 !important;}
section[data-testid="stSidebar"] hr{border-color:#333 !important;}

.stButton>button{background:#1a1a1a !important;color:#f5f2ec !important;
    border:1px solid #1a1a1a !important;border-radius:2px !important;
    font-family:"JetBrains Mono",monospace !important;
    font-size:0.7rem !important;letter-spacing:0.06em !important;
    text-transform:uppercase !important;}
.stButton>button:hover{background:#333 !important;}
.stTextArea textarea,.stTextInput input{background:#fff !important;
    border:1px solid #ddd8ce !important;border-radius:2px !important;
    font-family:"Source Serif 4",serif !important;color:#1a1a1a !important;}
.stSelectbox>div>div{background:#fff !important;border-color:#ddd8ce !important;}
.stCheckbox label{color:#1a1a1a !important;font-size:0.85rem !important;}
.day-btn button{font-size:0.65rem !important;padding:0.2rem 0.6rem !important;}
</style>
""", unsafe_allow_html=True)

DATABRICKS_HOST   = st.secrets.get("DATABRICKS_HOST","").rstrip("/")
DATABRICKS_TOKEN  = st.secrets.get("DATABRICKS_TOKEN","")
DATABRICKS_HTTP   = st.secrets.get("DATABRICKS_HTTP_PATH","")
GROQ_API_KEY      = st.secrets.get("GROQ_API_KEY","")
DATABRICKS_JOB_ID = "998370935632321"

CAT_INFO={
    "A":("Contratación Pública","#c0392b"),
    "B":("Enriquecimiento","#d35400"),
    "C":("Sesgo Judicial","#8B6914"),
    "D":("Desinformación / Pseudociencia","#1a7a4a"),
    "E":("Redes de Influencia","#1a5276"),
    "F":("Nepotismo","#6c3483"),
}
CAT_EXPLAIN={
    "A":"Irregularidades en contratos o gasto público",
    "B":"Enriquecimiento ilícito o puertas giratorias",
    "C":"Trato judicial diferente según partido",
    "D":"Noticias falsas, manipuladas o pseudociencias",
    "E":"Redes de influencia o financiación ilegal",
    "F":"Enchufismo o nepotismo en cargos",
}
TOPIC_COLORS={
    "POLITICA":"#c0392b","JUDICIAL":"#8B6914","ECONOMIA":"#1a5276",
    "SALUD":"#1a7a4a","DESINFORMACION":"#d35400","PSEUDOCIENCIA":"#6c3483","OTRO":"#888",
}
LANGUAGES={"🇪🇸 Español":"español","🇩🇪 Alemán":"alemán","🇫🇷 Francés":"francés",
           "🇮🇹 Italiano":"italiano","🇵🇱 Polaco":"polaco","🇷🇴 Rumano":"rumano"}

@st.cache_resource
def get_conn():
    from databricks import sql
    return sql.connect(
        server_hostname=DATABRICKS_HOST.replace("https://",""),
        http_path=DATABRICKS_HTTP, access_token=DATABRICKS_TOKEN)

def qry(q):
    try:
        c=get_conn()
        with c.cursor() as cur:
            cur.execute(q)
            cols=[d[0] for d in cur.description]
            return pd.DataFrame(cur.fetchall(),columns=cols)
    except Exception as e:
        st.error(str(e)); return pd.DataFrame()

def update_alerts(ids, status):
    try:
        c=get_conn()
        ids_str=",".join(f"\'{i}\'" for i in ids)
        with c.cursor() as cur:
            cur.execute(f"UPDATE arcas_processed.alerts SET status=\'{status}\' WHERE alert_id IN ({ids_str})")
        return True
    except Exception as e:
        st.error(str(e)); return False

def trigger_job(extra_params={}):
    try:
        payload = {"job_id": int(DATABRICKS_JOB_ID)}
        if extra_params:
            payload["job_parameters"] = extra_params
        r=requests.post(f"{DATABRICKS_HOST}/api/2.1/jobs/run-now",
            headers={"Authorization":f"Bearer {DATABRICKS_TOKEN}","Content-Type":"application/json"},
            json=payload, timeout=15)
        r.raise_for_status()
        return True,str(r.json().get("run_id","?"))
    except Exception as e: return False,str(e)

def get_run_status(run_id: str) -> dict:
    """Consulta el estado de una ejecucion del Job en Databricks."""
    try:
        r = requests.get(
            f"{DATABRICKS_HOST}/api/2.1/jobs/runs/get",
            headers={"Authorization": f"Bearer {DATABRICKS_TOKEN}"},
            params={"run_id": run_id},
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()
        state = data.get("state", {})
        return {
            "life_cycle": state.get("life_cycle_state", "UNKNOWN"),
            "result":     state.get("result_state", ""),
            "start_time": data.get("start_time", 0),
            "end_time":   data.get("end_time", 0),
        }
    except Exception:
        return {"life_cycle": "UNKNOWN", "result": "", "start_time": 0, "end_time": 0}

def validate_email(e):
    return bool(re.match(r"^[\w\.\+\-]+@[\w\-]+\.[a-z]{2,}$",e.strip(),re.IGNORECASE))

def send_email(to,subject,body):
    s=st.secrets.get("EMAIL_SENDER",""); p=st.secrets.get("EMAIL_PASSWORD","")
    if not s or not p: return False,"Credenciales no configuradas."
    try:
        msg=MIMEMultipart("alternative")
        msg["Subject"]=subject;msg["From"]=s;msg["To"]=to
        msg.attach(MIMEText(body,"plain","utf-8"))
        with smtplib.SMTP_SSL("smtp.gmail.com",465) as srv:
            srv.login(s,p);srv.sendmail(s,to,msg.as_string())
        return True,""
    except Exception as e: return False,str(e)

def groq_gen(prompt):
    try:
        import groq as g
        c=g.Groq(api_key=GROQ_API_KEY)
        r=c.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role":"user","content":prompt}],max_tokens=800)
        return r.choices[0].message.content.strip()
    except Exception as e: return f"[Error: {e}]"

with st.sidebar:
    st.markdown("""<div style="padding:1rem 0 1rem;">
        <div style="font-family:'Playfair Display',serif;font-weight:900;font-size:1.6rem;color:#f5f2ec;">ARCAS</div>
        <div style="font-family:'JetBrains Mono',monospace;font-size:0.58rem;color:#666;
                    text-transform:uppercase;letter-spacing:0.1em;margin-top:0.1rem;">Vigilancia Anticorrupción</div>
        <div style="border-bottom:1px solid #333;margin:0.8rem 0;"></div></div>""",
        unsafe_allow_html=True)
    page=st.radio("nav",[
        "🔄 Recargar información",
        "📊 Cuadro de mandos",
        "✅ Decisiones pendientes",
        "📋 Historial de decisiones",
        "✍️ Generar publicaciones",
    ],label_visibility="collapsed")
    st.markdown("---")
    st.markdown("""<div style="font-family:'JetBrains Mono',monospace;font-size:0.56rem;
        color:#444;text-transform:uppercase;letter-spacing:0.07em;line-height:2;">
        Fuentes<br><span style="color:#666;text-transform:none;letter-spacing:0;">
        El País · El Mundo · ABC · La Vanguardia<br>
        Público · elDiario · El Confidencial<br>
        OK Diario · La Razón · RTVE · La Sexta<br>
        Maldita · Maldita Ciencia · Newtral<br>
        EFE Verifica · RTVE Verifica · Civio<br>
        El País Salud · El Mundo Salud · 20min
        </span></div>""",unsafe_allow_html=True)

st.markdown(f"""<div class="arcas-header">
    <div class="arcas-title">Sistema ARCAS</div>
    <div class="arcas-subtitle">Anti-Corruption &amp; Accountability Research System</div>
    <div class="arcas-date">{datetime.now().strftime("%A, %d de %B de %Y · %H:%M")}</div>
</div>""",unsafe_allow_html=True)

# ══ RECARGAR ══════════════════════════════════════════════════════════════════
if page=="🔄 Recargar información":
    st.markdown('<div class="section-rule">Actualización de fuentes</div>', unsafe_allow_html=True)
    st.markdown("El sistema analiza automáticamente las noticias cada día a las **09:30**. Si necesitas analizar noticias de ahora mismo, usa el botón.")

    truncar = st.checkbox(
        "⚠️ Vaciar todas las tablas antes de recargar",
        value=False,
        help="Borra todos los artículos y alertas existentes y hace una recarga completa desde cero. Úsalo solo si los datos están corruptos o quieres empezar de nuevo.",
    )

    confirmacion_ok = True
    if truncar:
        st.warning("Esta acción borrará todos los artículos, alertas y entidades existentes. No se puede deshacer.")
        confirmacion = st.text_input("Escribe **CONFIRMAR** para proceder con el vaciado:")
        confirmacion_ok = confirmacion.strip() == "CONFIRMAR"
        if not confirmacion_ok and confirmacion.strip():
            st.error("El texto no coincide. Escribe exactamente: CONFIRMAR")

    col_btn, col_info = st.columns([1, 2])
    with col_btn:
        boton_label = "🔄 Vaciar y recargar" if truncar else "🔄 Analizar noticias ahora"
        if st.button(boton_label, use_container_width=True, disabled=(truncar and not confirmacion_ok)):
            inicio = datetime.now()
            job_params = {"TRUNCATE_TABLES": "true"} if truncar else {}
            ok, run_id = trigger_job(job_params)
            if ok:
                st.session_state["run_id"]    = run_id
                st.session_state["run_inicio"] = inicio
                msg = "con vaciado de tablas" if truncar else "incremental"
                st.success(f"Proceso iniciado ({msg}). ID: {run_id}")
            else:
                st.error(f"No se pudo iniciar: {run_id}")

    # Panel de seguimiento si hay un run activo
    if st.session_state.get("run_id"):
        run_id    = st.session_state["run_id"]
        inicio    = st.session_state.get("run_inicio", datetime.now())
        status    = get_run_status(run_id)
        lc        = status["life_cycle"]
        result    = status["result"]
        ahora     = datetime.now()
        duracion  = int((ahora - inicio).total_seconds())
        mm, ss    = divmod(duracion, 60)

        ESTADO_ES = {
            "PENDING":    ("⏳ En cola...",         "#8B6914"),
            "RUNNING":    ("⚙️ Ejecutándose...",    "#1a5276"),
            "TERMINATING":("⚙️ Finalizando...",     "#1a5276"),
            "TERMINATED": ("✅ Completado",          "#1a7a4a"),
            "SKIPPED":    ("⏭️ Omitido",             "#888"),
            "INTERNAL_ERROR": ("❌ Error interno",  "#c0392b"),
            "UNKNOWN":    ("❓ Desconocido",         "#888"),
        }
        etiqueta, color = ESTADO_ES.get(lc, (lc, "#888"))
        if lc == "TERMINATED" and result not in ("SUCCESS", ""):
            etiqueta, color = f"❌ Fallido ({result})", "#c0392b"

        # Calcular duración real desde Databricks si ya terminó
        if lc == "TERMINATED" and status["start_time"] and status["end_time"]:
            dur_real = int((status["end_time"] - status["start_time"]) / 1000)
            mm2, ss2 = divmod(dur_real, 60)
            dur_str  = f"{mm2}m {ss2}s"
        else:
            dur_str  = f"{mm}m {ss}s (en curso)"

        st.markdown(f"""
        <div style="background:#fff;border:1px solid #ddd8ce;border-left:4px solid {color};
                    padding:1rem 1.5rem;border-radius:2px;margin-top:1rem;">
            <div style="font-family:'JetBrains Mono',monospace;font-size:0.65rem;
                        color:#888;text-transform:uppercase;letter-spacing:0.08em;">
                Estado del proceso · Run {run_id}
            </div>
            <div style="font-size:1.2rem;font-weight:700;color:{color};margin-top:0.3rem;">
                {etiqueta}
            </div>
            <div style="font-size:0.8rem;color:#888;margin-top:0.2rem;">
                Inicio: {inicio.strftime("%d/%m/%Y %H:%M:%S")} · Duración: {dur_str}
            </div>
        </div>
        """, unsafe_allow_html=True)

        if lc in ("PENDING", "RUNNING", "TERMINATING"):
            if st.button("🔃 Actualizar estado", key="refresh_status"):
                st.rerun()
        elif lc == "TERMINATED":
            if st.button("✖ Cerrar seguimiento", key="clear_run"):
                del st.session_state["run_id"]
                del st.session_state["run_inicio"]
                st.rerun()

    with col_info:
        df_last = qry("SELECT max(ingested_at) AS ultima, count(*) AS total FROM arcas_raw.articles WHERE source_type!='gazette'")
        if not df_last.empty and df_last["ultima"].iloc[0]:
            ts = df_last["ultima"].iloc[0]
            try:
                from datetime import timezone as tz
                if hasattr(ts, "strftime"):
                    ultima = ts.strftime("%d/%m/%Y %H:%M:%S")
                else:
                    ultima = str(ts)[:19].replace("T", " ")
                    partes = ultima.split(" ")
                    if len(partes) == 2:
                        d, t = partes
                        y, mo, dy = d.split("-")
                        ultima = f"{dy}/{mo}/{y} {t}"
            except Exception:
                ultima = str(ts)[:19]
            total = int(df_last["total"].iloc[0])
            st.markdown(f"""
            <div style="background:#fff;border:1px solid #ddd8ce;padding:1rem 1.3rem;border-radius:2px;">
                <div style="font-family:'JetBrains Mono',monospace;font-size:0.65rem;color:#888;
                            text-transform:uppercase;letter-spacing:0.08em;">Última recarga</div>
                <div style="font-size:1.1rem;font-weight:600;color:#0d0d0d;margin-top:0.2rem;">{ultima}</div>
                <div style="font-size:0.8rem;color:#888;font-style:italic;">{total:,} noticias almacenadas</div>
            </div>
            """, unsafe_allow_html=True)
# ══ CUADRO DE MANDOS ══════════════════════════════════════════════════════════
elif page=="📊 Cuadro de mandos":
    st.markdown('<div class="section-rule">Actividad por día</div>',unsafe_allow_html=True)

    # Selector de días
    today=datetime.now().date()
    days_back=st.slider("Días a mostrar",min_value=1,max_value=14,value=7)
    dates=[today-timedelta(days=i) for i in range(days_back-1,-1,-1)]

    # Botones de día
    if "selected_day" not in st.session_state:
        st.session_state.selected_day=str(today)
    st.markdown('<div class="section-rule-light">Selecciona un día para ver el detalle</div>',unsafe_allow_html=True)
    day_cols=st.columns(len(dates))
    for i,d in enumerate(dates):
        label="HOY" if d==today else d.strftime("%d/%m")
        with day_cols[i]:
            if st.button(label,key=f"day_{d}",use_container_width=True):
                st.session_state.selected_day=str(d)

    sel_day=st.session_state.selected_day
    st.markdown(f"**Detalle del día {sel_day}**")

    # KPIs del día seleccionado
    df_day=qry(f"""SELECT category,topic,status,confidence_score,source_name,title,content_url
        FROM arcas_processed.alerts
        WHERE date(created_at)='{sel_day}'
        ORDER BY confidence_score DESC""")
    df_art_day=qry(f"""SELECT count(*) AS n FROM arcas_raw.articles
        WHERE source_type!='gazette' AND date(ingested_at)='{sel_day}'""")

    art_day=int(df_art_day["n"].iloc[0]) if not df_art_day.empty else 0
    al_day=len(df_day)
    pen_day=len(df_day[df_day["status"]=="pending"]) if not df_day.empty else 0

    c1,c2,c3=st.columns(3)
    for col,val,label,color in [
        (c1,art_day,"Noticias analizadas","#1a5276"),
        (c2,al_day,"Alertas detectadas","#c0392b"),
        (c3,pen_day,"Pendientes revisión","#8B6914"),
    ]:
        col.markdown(f"""<div class="kpi-card" style="--kc:{color};">
            <div class="kpi-value">{val}</div>
            <div class="kpi-label">{label}</div>
        </div>""",unsafe_allow_html=True)

    st.markdown("<br>",unsafe_allow_html=True)

    # Evolución temporal
    df_evo=qry(f"""SELECT date(created_at) AS dia, count(*) AS alertas
        FROM arcas_processed.alerts
        WHERE date(created_at) >= '{today-timedelta(days=days_back)}'
        GROUP BY dia ORDER BY dia""")
    if not df_evo.empty:
        fig=go.Figure(go.Scatter(
            x=df_evo["dia"],y=df_evo["alertas"],
            mode="lines+markers",line=dict(color="#c0392b",width=2),
            marker=dict(size=6,color="#c0392b"),
            fill="tozeroy",fillcolor="rgba(192,57,43,0.08)"))
        fig.update_layout(paper_bgcolor="#f5f2ec",plot_bgcolor="#f5f2ec",
            font=dict(color="#555",family="JetBrains Mono",size=10),
            xaxis=dict(showgrid=False,zeroline=False),
            yaxis=dict(showgrid=True,gridcolor="#e8e4dc",zeroline=False),
            margin=dict(l=0,r=10,t=10,b=10),height=180,
            title=dict(text="Alertas detectadas por día",font=dict(size=11)))
        st.plotly_chart(fig,use_container_width=True)

    # Alertas del día con desglose por topic
    if not df_day.empty:
        cL,cR=st.columns(2)
        with cL:
            st.markdown('<div class="section-rule-light">Por tipo de irregularidad</div>',unsafe_allow_html=True)
            cc=df_day["category"].value_counts().reset_index()
            cc.columns=["cat","n"]
            cc["label"]=cc["cat"].map(lambda c:CAT_EXPLAIN.get(c,c))
            cc["color"]=cc["cat"].map(lambda c:CAT_INFO.get(c,("?","#888"))[1])
            fig2=go.Figure(go.Bar(x=cc["n"],y=cc["label"],orientation="h",
                marker_color=cc["color"],text=cc["n"],textposition="outside"))
            fig2.update_layout(paper_bgcolor="#f5f2ec",plot_bgcolor="#f5f2ec",
                font=dict(color="#555",family="JetBrains Mono",size=10),
                xaxis=dict(showgrid=False,zeroline=False,visible=False),
                yaxis=dict(showgrid=False),
                margin=dict(l=0,r=30,t=5,b=5),height=200)
            st.plotly_chart(fig2,use_container_width=True)
        with cR:
            st.markdown('<div class="section-rule-light">Por temática</div>',unsafe_allow_html=True)
            if "topic" in df_day.columns and df_day["topic"].notna().any():
                tc=df_day["topic"].value_counts().reset_index()
                tc.columns=["topic","n"]
                tc["color"]=tc["topic"].map(lambda t:TOPIC_COLORS.get(t,"#888"))
                fig3=go.Figure(go.Bar(x=tc["n"],y=tc["topic"],orientation="h",
                    marker_color=tc["color"],text=tc["n"],textposition="outside"))
                fig3.update_layout(paper_bgcolor="#f5f2ec",plot_bgcolor="#f5f2ec",
                    font=dict(color="#555",family="JetBrains Mono",size=10),
                    xaxis=dict(showgrid=False,zeroline=False,visible=False),
                    yaxis=dict(showgrid=False),
                    margin=dict(l=0,r=30,t=5,b=5),height=200)
                st.plotly_chart(fig3,use_container_width=True)
            else:
                st.info("Campo topic disponible desde la próxima ingesta.")

        st.markdown('<div class="section-rule-light">Noticias del día</div>',unsafe_allow_html=True)
        for _,row in df_day.iterrows():
            cat=str(row.get("category","?")); topic=str(row.get("topic","") or "")
            title=str(row.get("title",""))[:75]; source=str(row.get("source_name",""))
            conf=float(row.get("confidence_score",0)); color=CAT_INFO.get(cat,("?","#888"))[1]
            topic_html=f'<span class="topic-pill">{topic}</span>' if topic and topic!="None" else ""
            st.markdown(f"""<div class="alert-card" style="--ac:{color};">
                <div class="alert-headline">{title}…</div>
                <div class="alert-byline">
                    <span class="badge b-{cat}">{CAT_INFO.get(cat,("?",""))[0]}</span>
                    {topic_html} · {source} · {conf:.0%}
                </div></div>""",unsafe_allow_html=True)
    else:
        st.info(f"No hay alertas para el día {sel_day}.")

# ══ DECISIONES PENDIENTES ═════════════════════════════════════════════════════
elif page=="✅ Decisiones pendientes":
    st.markdown('<div class="section-rule">Noticias pendientes de revisión</div>',unsafe_allow_html=True)

    df_all=qry("""SELECT alert_id,category,topic,status,confidence_score,nl_justification,
               source_name,title,content_url FROM arcas_processed.alerts
               WHERE status='pending' ORDER BY confidence_score DESC""")

    if df_all.empty:
        st.success("✅ No hay noticias pendientes.")
    else:
        # Filtros
        col_f1,col_f2,col_f3=st.columns(3)
        with col_f1:
            cats=["Todas"]+sorted(df_all["category"].dropna().unique().tolist())
            cat_filter=st.selectbox("Tipo de irregularidad",cats)
        with col_f2:
            topics=["Todas"]+sorted(df_all["topic"].dropna().unique().tolist()) if "topic" in df_all.columns else ["Todas"]
            topic_filter=st.selectbox("Temática",topics)
        with col_f3:
            conf_min=st.slider("Certeza mínima",0,100,0,5)

        df=df_all.copy()
        if cat_filter!="Todas": df=df[df["category"]==cat_filter]
        if topic_filter!="Todas" and "topic" in df.columns: df=df[df["topic"]==topic_filter]
        df=df[df["confidence_score"]>=conf_min/100]

        st.markdown(f"**{len(df)} noticia(s) · {len(df_all)} total pendientes**")

        with st.expander("⚡ Actuar sobre varias a la vez"):
            sel_ids=[]
            for _,row in df.iterrows():
                aid=str(row["alert_id"]); cat=str(row["category"])
                t=str(row["title"])[:60]; conf=float(row["confidence_score"])
                topic=str(row.get("topic","") or "")
                lbl=f"[{CAT_INFO.get(cat,('?',''))[0]}]{f' [{topic}]' if topic and topic!='None' else ''} {t}… ({conf:.0%})"
                if st.checkbox(lbl,key=f"chk_{aid}"): sel_ids.append(aid)
            if sel_ids:
                ca,cr,ce=st.columns(3)
                with ca:
                    if st.button("✅ Aprobar",key="ba"): update_alerts(sel_ids,"approved"); st.rerun()
                with cr:
                    if st.button("❌ Rechazar",key="br"): update_alerts(sel_ids,"rejected"); st.rerun()
                with ce:
                    if st.button("⬆️ Escalar",key="be"): update_alerts(sel_ids,"escalated"); st.rerun()
        st.markdown("---")

        for _,row in df.iterrows():
            cat=str(row["category"]); conf=float(row["confidence_score"])
            title=str(row["title"]); source=str(row["source_name"])
            url=str(row["content_url"]); analysis=str(row["nl_justification"])
            alert_id=str(row["alert_id"]); topic=str(row.get("topic","") or "")
            color=CAT_INFO.get(cat,("?","#888"))[1]
            topic_html=f'<span class="topic-pill">{topic}</span>' if topic and topic!="None" else ""
            with st.expander(f"[{CAT_INFO.get(cat,('?',''))[0]}] {title[:65]}… · {conf:.0%}"):
                st.markdown(f"""<div class="alert-byline" style="margin-bottom:0.8rem;">
                    <span class="badge b-{cat}">{CAT_INFO.get(cat,('?',''))[0]}</span>
                    {topic_html} · {source} · {conf:.0%}
                </div>""",unsafe_allow_html=True)
                if url: st.markdown(f"🔗 [Ver noticia]({url})")
                st.markdown("**Análisis del sistema:**")
                st.markdown(f'<div class="analysis-box">{analysis}</div>',unsafe_allow_html=True)
                st.markdown("<br>",unsafe_allow_html=True)
                notes=st.text_area("Notas",key=f"n_{alert_id}",height=60,label_visibility="collapsed",placeholder="Contexto opcional...")
                ca2,cr2,ce2,cem=st.columns(4)
                with ca2:
                    if st.button("✅ Publicar",key=f"a_{alert_id}"):
                        update_alerts([alert_id],"approved"); st.rerun()
                with cr2:
                    if st.button("❌ Descartar",key=f"r_{alert_id}"):
                        update_alerts([alert_id],"rejected"); st.rerun()
                with ce2:
                    if st.button("⬆️ Escalar",key=f"e_{alert_id}"):
                        update_alerts([alert_id],"escalated"); st.rerun()
                with cem:
                    if st.button("📧 Email",key=f"em_{alert_id}"):
                        st.session_state[f"show_em_{alert_id}"]=True
                if st.session_state.get(f"show_em_{alert_id}"):
                    ea=st.text_input("Email",key=f"ea_{alert_id}",placeholder="nombre@dominio.com")
                    if st.button("Enviar",key=f"send_{alert_id}"):
                        if not validate_email(ea): st.error("Email no válido.")
                        else:
                            body=f"ALERTA ARCAS — {CAT_INFO.get(cat,('?',''))[0]}\nTitular: {title}\nFuente: {source}\nURL: {url}\n\nAnálisis:\n{analysis}"
                            ok,err=send_email(ea,f"[ARCAS] {title[:50]}",body)
                            if ok: st.success("Enviado."); st.session_state[f"show_em_{alert_id}"]=False
                            else: st.error(err)

# ══ HISTORIAL ════════════════════════════════════════════════════════════════
elif page=="📋 Historial de decisiones":
    st.markdown('<div class="section-rule">Decisiones tomadas — revisión y rectificación</div>',unsafe_allow_html=True)
    df_hist=qry("""SELECT alert_id,category,topic,status,confidence_score,
                   nl_justification,source_name,title,content_url
                   FROM arcas_processed.alerts WHERE status!='pending'
                   ORDER BY status,confidence_score DESC""")
    if df_hist.empty:
        st.info("No hay decisiones tomadas todavía.")
    else:
        for sv,label,icon in [
            ("approved","Aprobadas","✅"),
            ("rejected","Descartadas","❌"),
            ("escalated","Escaladas","⬆️"),
        ]:
            grupo=df_hist[df_hist["status"]==sv]
            if grupo.empty: continue
            st.markdown(f'<div class="section-rule-light">{icon} {label} ({len(grupo)})</div>',unsafe_allow_html=True)
            for _,row in grupo.iterrows():
                cat=str(row["category"]); conf=float(row["confidence_score"])
                title=str(row["title"]); source=str(row["source_name"])
                url=str(row["content_url"]); analysis=str(row["nl_justification"])
                alert_id=str(row["alert_id"]); topic=str(row.get("topic","") or "")
                color=CAT_INFO.get(cat,("?","#888"))[1]
                topic_html=f'<span class="topic-pill">{topic}</span>' if topic and topic!="None" else ""
                with st.expander(f"[{CAT_INFO.get(cat,('?',''))[0]}] {title[:60]}… · {conf:.0%}"):
                    st.markdown(f"""<div class="alert-byline" style="margin-bottom:0.6rem;">
                        <span class="badge b-{cat}">{CAT_INFO.get(cat,('?',''))[0]}</span>
                        {topic_html} · {source}
                    </div>""",unsafe_allow_html=True)
                    if url: st.markdown(f"🔗 [Ver noticia]({url})")
                    with st.expander("Ver análisis"):
                        st.markdown(f'<div class="analysis-box">{analysis}</div>',unsafe_allow_html=True)
                    st.markdown("**Cambiar decisión:**")
                    opciones=[s for s in ["approved","rejected","escalated","pending"] if s!=sv]
                    labels={"approved":"✅ Aprobar","rejected":"❌ Rechazar",
                            "escalated":"⬆️ Escalar","pending":"↩️ Volver a pendiente"}
                    cols=st.columns(len(opciones))
                    for i,nueva in enumerate(opciones):
                        with cols[i]:
                            if st.button(labels[nueva],key=f"hist_{alert_id}_{nueva}"):
                                update_alerts([alert_id],nueva); st.rerun()

# ══ GENERAR PUBLICACIONES ═════════════════════════════════════════════════════
elif page=="✍️ Generar publicaciones":
    st.markdown('<div class="section-rule">Redactar publicación para redes sociales</div>',unsafe_allow_html=True)
    df=qry("""SELECT alert_id,category,topic,confidence_score,nl_justification,
               source_name,title,content_url FROM arcas_processed.alerts
               WHERE status IN ('approved','pending') ORDER BY confidence_score DESC LIMIT 50""")
    if df.empty:
        st.info("No hay alertas disponibles.")
    else:
        cS,cL=st.columns([2,1])
        with cS:
            options={f"[{row['category']}] {str(row['title'])[:55]}…":row for _,row in df.iterrows()}
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

        st.markdown('<div class="section-rule-light">Instrucciones al redactor</div>',unsafe_allow_html=True)
        DEFAULT_PROMPT="""Eres un periodista de investigación riguroso y políticamente neutral.
Informa sobre hechos concretos de la noticia analizada, no generalidades.
Nunca acuses directamente — señala patrones y hechos verificables.
Usa lenguaje accesible, sin jerga técnica ni legal.
Indica explícitamente si hay pruebas o si la información se basa en declaraciones sin respaldo.
Invita a la ciudadanía a investigar y verificar por su cuenta.

El texto usará un estilo socrático. Tendrá un máximo de 5 párrafos.
Cada párrafo irá introducido con una pregunta en MAYÚSCULAS precedida de un emoji, en modo socrático.
Usaremos hashtags durante el texto, con un máximo de 15.
Puedes ir a la fuente para enriquecer la noticia con detalles adicionales.
Cada párrafo contendrá 5 ó 6 frases, explicando con detalle y animando a leer el siguiente.
Usa un lenguaje sencillo, entendible por todo el mundo."""
        master=st.text_area("instrucciones",value=DEFAULT_PROMPT,height=200,label_visibility="collapsed")

        if st.button("✍️ Redactar publicación",use_container_width=True):
            cat=sel["category"]; cat_name=CAT_INFO.get(cat,(cat,""))[0]
            justif=str(sel["nl_justification"]); title=str(sel["title"])
            source=str(sel["source_name"]); conf=float(sel["confidence_score"])
            url=str(sel.get("content_url",""))
            full_prompt=f"""INSTRUCCIONES DEL OPERADOR:
{master}

---
NOTICIA ANALIZADA:
- Tipo de irregularidad: {cat_name}
- Fuente: {source}
- Titular: {title}
- URL original: {url}
- Análisis del sistema: {justif[:600]}
- Nivel de certeza: {conf:.0%}

---
REQUISITOS TÉCNICOS:
- Idioma: {lang_name}
- Red social: {platform} (máximo {char_limit} caracteres)
- Tono: {tone}
- Formato: Markdown con **negrita**, *cursiva* y #hashtags integrados en el texto
- NO menciones IA ni sistemas automatizados
- Céntrate en ESTA noticia específica, no en generalidades

Escribe únicamente el texto de la publicación en Markdown."""
            with st.spinner("Redactando..."):
                post_md=groq_gen(full_prompt)
            st.markdown('<div class="section-rule-light">Texto Markdown (raw)</div>',unsafe_allow_html=True)
            st.code(post_md,language="markdown")
            st.markdown('<div class="section-rule-light">Vista previa renderizada</div>',unsafe_allow_html=True)
            st.markdown(post_md)
            char_count=len(post_md)
            color="#1a7a4a" if char_count<=char_limit else "#c0392b"
            st.markdown(f'<span style="font-family:JetBrains Mono,monospace;font-size:0.68rem;color:{color};">{char_count}/{char_limit} caracteres</span>',unsafe_allow_html=True)
            c1,c2=st.columns(2)
            with c1: st.download_button("⬇️ .md",post_md,f"arcas_{lang_name}_{cat}.md","text/markdown")
            with c2: st.download_button("⬇️ .txt",post_md,f"arcas_{lang_name}_{cat}.txt","text/plain")
