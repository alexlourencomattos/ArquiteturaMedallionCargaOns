# Arquitetura medalhão com dados abertos do ONS

Pipeline bronze → silver → gold sobre a **Carga de Energia Diária** do
[Portal de Dados Abertos do ONS](https://dados.ons.org.br/dataset/carga-energia),
em Python + DuckDB, com logs estruturados e rastreabilidade ponta a ponta.

Projeto de estudo. Acompanha a série de posts "Setor elétrico e engenharia de dados".

## Como rodar

```bash
pip install -r requirements.txt
# baixe CARGA_ENERGIA_2026.parquet no portal do ONS (os dados não estão neste repositório)
python medalhao_ons_carga.py CARGA_ENERGIA_2026.parquet
```

Opções: `--lakehouse <pasta>` (padrão `lakehouse`) e `--sem-falhar` (publica mesmo com falha de qualidade; não recomendado).
Código de saída: `0` sucesso, `1` publicação bloqueada pelo quality gate.

## O que cada camada faz

| Camada | Regra | Saída |
|---|---|---|
| Bronze | Dado como chegou + metadados (run_id, hora UTC, arquivo, SHA-256, hash da linha). Append-only | `bronze/carga_energia_di/run_id=<id>/data.parquet` |
| Silver | Tipos corretos, dedup pela chave (`id_subsistema`, `data_referencia`), validações | `silver/carga_energia_di.parquet` |
| Gold | Média mensal por subsistema; carga diária do SIN (soma dos 4 subsistemas, só dias completos) | `gold/*.parquet` |

## Boas práticas de rastreabilidade

- **run_id** em toda linha de log, em cada linha do bronze e no manifesto
- **Fingerprint** SHA-256 do arquivo de origem
- **Bronze imutável**: uma pasta por execução, nunca sobrescreve histórico
- **Idempotência**: rodar o mesmo arquivo duas vezes não duplica a silver
- **Quality gate**: regra com valor, esperado e severidade (AVISO ou ERRO). Com erro, silver e gold não são publicadas e o bronze é preservado
- **Escrita atômica**: grava em `.tmp` e renomeia
- **Manifesto JSON** por execução: contagens, regras de qualidade, hashes e versões
- **Logs em UTC** com duração de cada etapa (`lakehouse/logs/<run_id>.log`)

Exemplos reais em [`docs/`](docs): log de sucesso, log de falha de qualidade e manifesto.

## Limitações (leia antes de reutilizar)

- A silver é reconstruída a partir de **todas** as execuções do bronze, mantendo a ingestão mais recente de cada chave. Isso dá idempotência, mas significa que um arquivo novo **incompleto** pode ser mascarado por dados de execuções anteriores. Para validar cada carga isoladamente, aplique o gate também ao bronze da execução.
- As regras de qualidade são as listadas no código; "passou" significa apenas isso.
- Não há orquestração, testes automatizados nem controle de concorrência (duas execuções simultâneas no mesmo diretório podem conflitar).
- Os dados mudam: o ONS atualiza o arquivo diariamente e pode revisar valores já publicados.

## Fonte e licença dos dados

Portal de Dados Abertos do ONS, dataset *Carga de Energia Diária*, licença CC-BY.
Os dados não são redistribuídos aqui. Defina a licença do **código** (por exemplo MIT) antes de publicar o repositório.
