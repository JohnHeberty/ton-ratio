# ton-ratio

Este projeto foi desenvolvido para a CODEMGE por John Heberty de Freitas, com o objetivo de auditar e aprimorar a infraestrutura e os processos de ETL (Extract, Transform, Load) implementados anteriormente pela INFRA SA.

## Objetivo
O foco do projeto é a familiarização, auditoria e processamento de dados de tonelagem (TON) por NCM (Nomenclatura Comum do Mercosul) e por município, utilizando dados de exportação e importação. O sistema realiza extração, transformação, carga e análise desses dados, além de criar visões analíticas no banco de dados PostgreSQL.

## Estrutura do Projeto

- **main.py**: Ponto de entrada principal do pipeline ETL.
- **compose.yaml / Dockerfile**: Infraestrutura para execução do projeto em containers Docker, incluindo aplicação Python e banco PostgreSQL.
- **.env**: Variáveis de ambiente para configuração do banco e parâmetros do ETL.
- **data/**: Diretório para armazenamento dos dados brutos e processados (não versionado).
- **etl/**: Módulos de extração, carga e transformação dos dados.
  - **extract/**: Scripts para extração dos dados de fontes externas.
  - **load/**: Scripts para criação de tabelas e inserção de dados no banco.
  - **transform/**: Scripts para transformação dos dados e criação de views.
- **modules/**: Componentes reutilizáveis e utilitários do projeto.
  - **database/**: Abstrações e implementações para conexão e manipulação do banco PostgreSQL.
  - **Config/**: Carregamento e gerenciamento das configurações do projeto.
  - **ComexStatDownloader/**: Utilitários para download e manipulação de arquivos de dados.
- **repository/querys/**: Scripts SQL para criação de tabelas e views no banco de dados.
- **notebooks/**: Jupyter Notebooks para exploração, testes e validação dos dados.
- **requirements.txt**: Dependências Python do projeto.

## Como Executar

1. **Configuração**
   - Edite o arquivo `.env` com as configurações do banco e parâmetros desejados.

2. **Suba a infraestrutura com Docker Compose**
   ```sh
   docker compose up --build
   ```

3. **O pipeline ETL será executado automaticamente ao iniciar o container `server`.**

## Observações de Auditoria
- O projeto foi reestruturado para garantir portabilidade, reprodutibilidade e facilidade de manutenção.
- O uso de Docker garante que o ambiente seja idêntico em qualquer sistema operacional.
- O carregamento de variáveis de ambiente foi padronizado para funcionar tanto localmente quanto em containers.
- O código foi modularizado para facilitar auditorias, testes e futuras expansões.

## Autor
Desenvolvido por John Heberty de Freitas para a CODEMGE.

---

**Este README foi gerado para fins de auditoria e documentação do projeto originalmente entregue pela INFRA SA.**
