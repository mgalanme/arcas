# ARCAS — Anti-Corruption & Accountability Research System

Sistema agentivo de vigilancia anticorrupción que monitoriza fuentes públicas españolas para detectar patrones de corrupción, desinformación y pseudociencias.

## Arquitectura

```
Databricks Serverless (Job diario 09:30 CET)
    → Scraping 32 fuentes (medios + fact-checkers + salud/pseudociencias)
    → BOE: almacenado solo para contraste, no genera alertas
    → Traducción automática (Groq) de títulos en inglés
    → Clasificación temática: POLITICA / JUDICIAL / ECONOMIA / SALUD / DESINFORMACION / PSEUDOCIENCIA
    → Scoring por keywords (categorías A-F)
    → Análisis profundo por artículo: calidad probatoria, impacto ciudadanos, velocidad procesal
    → Extracción de entidades (personas, organismos, casos, partidos) → Neo4j Aura + Delta
    → Delta Lake: arcas_raw.articles, arcas_processed.alerts, entities, relations

Streamlit Community Cloud
    → 5 páginas: Recargar / Cuadro de mandos / Decisiones humanas / Historial / Publicaciones / Mantenimiento
    → HITL: aprobar / rechazar / escalar — individual o en bloque con checkboxes
    → Email via Gmail SMTP
    → Recarga manual con seguimiento de estado en tiempo real
    → Vaciado de tablas con confirmación desde la app
    → Optimización Delta on-demand
    → Generador de posts en Markdown con prompt maestro socrático

Neo4j Aura Free
    → Grafo de entidades: Persona → caso → fuente → alerta
    → Base para detección de patrones temporales (Nivel 2)
```

## Categorías de detección

| Categoría | Descripción |
|---|---|
| A | Fraude en contratación pública o malversación |
| B | Enriquecimiento ilícito o puertas giratorias |
| C | Sesgo judicial o trato procesal diferencial por partido |
| D | Desinformación, bulos o pseudociencias |
| E | Redes de influencia o financiación ilegal |
| F | Nepotismo o enchufismo en cargos públicos |

## Stack tecnológico

| Componente | Tecnología |
|---|---|
| Ingesta y pipeline | Databricks Serverless, PySpark, Delta Lake |
| LLM | Groq API — llama-3.3-70b-versatile |
| Grafo de relaciones | Neo4j Aura Free |
| Frontend | Streamlit Community Cloud |
| Almacenamiento | Databricks Delta Lake (Free Tier) |
| Scheduling | Databricks Jobs |
| Control de versiones | GitHub |

## Fuentes monitorizadas

**Prensa generalista:** El Mundo, ABC, La Vanguardia, Público, elDiario.es, OK Diario, La Razón, El Español, infoLibre, El Confidencial, La Sexta, RTVE, Expansión

**Fuentes judiciales y transparencia:** Poder Judicial, Transparencia.gob.es, Civio, El Salto

**Salud y pseudociencias:** El Mundo Salud, El Confidencial Salud, 20minutos Ciencia

**Fact-checkers:** Maldita.es, Maldita Ciencia, Newtral, EFE Verifica, RTVE Verifica, Snopes, PolitiFact, APETP

**Internacionales:** AP News Spain, Transparency International

**Referencia oficial:** BOE (contraste únicamente)

## Estructura del repositorio

```
arcas/
├── notebooks/
│   └── arcas_01_ingestion_daily.py   # Pipeline diario de ingesta y análisis
├── streamlit/
│   ├── app.py                         # Aplicación Streamlit
│   └── requirements.txt
└── README.md
```

## Variables de configuración

Todos los secretos se gestionan via Databricks Job parameters y Streamlit secrets. Nunca se hardcodean en el código.

| Variable | Dónde |
|---|---|
| GROQ_API_KEY | Databricks Job params + Streamlit secrets |
| NEO4J_URI / NEO4J_USERNAME / NEO4J_PASSWORD | Databricks Job params + Streamlit secrets |
| DATABRICKS_HOST / TOKEN / HTTP_PATH | Streamlit secrets |
| EMAIL_SENDER / EMAIL_PASSWORD | Streamlit secrets |

## Decisiones arquitectónicas clave

- El BOE se almacena en Delta para contraste pero **nunca genera alertas** — evita ruido de resoluciones oficiales sin contexto periodístico
- La confianza final se pondera por la credibilidad documentada de la fuente (OK Diario: 0.30, fact-checkers: 1.0)
- El análisis profundo evalúa explícitamente: *Con pruebas materiales / Con testimonios / Solo declaraciones / Sin pruebas*
- La clasificación temática (topic) se hace en ingesta, no en runtime, para mantener la app ligera
- HITL es obligatorio: ninguna alerta se publica sin decisión humana explícita

## Autor

Martín Galán — Senior Data Architect & Enterprise Architect  
[github.com/mgalanme](https://github.com/mgalanme)
