#!/bin/bash

rm -fr /home/pruebas/formacion/arcas

mkdir -p /home/pruebas/formacion/arcas/{.github/workflows,config/{k3s,kafka,flink,neo4j,postgres,qdrant,minio,redis,infisical,sam,otel,grafana,loki,tempo,prometheus},deploy/{base,overlays/{dev,prod}},src/{arcas_ingest/{connectors/{gazette,procurement,courts,media,icij},scraper},arcas_stream,arcas_nlp,arcas_knowledge_graph,arcas_vector_store,arcas_agents/{orchestrator,fraud,judicial,disinfo,network,enrichment,reporting},arcas_audit,arcas_vault,arcas_api,arcas_dashboard/{pages,components,hitl}},tests/{unit,integration,e2e},notebooks,scripts/{setup,maintenance,demo},docs,data/{raw,processed,iceberg},makefile_includes}
