---
name: sync-treasury-sql
description: Sincroniza e reflete alterações na estrutura do banco de dados PostgreSQL axie_treasury diretamente no arquivo schema.sql em treasury-update.
---

# Sync Treasury SQL Skill

Esta skill é responsável por garantir que o arquivo `schema.sql` na raiz de `treasury-update` esteja sempre perfeitamente sincronizado com o banco de dados PostgreSQL (`axie_treasury`).

## Quando utilizar esta skill
- Sempre que houver criação de novas tabelas, colunas, tipos, índices ou views no PostgreSQL.
- Sempre que qualquer alteração de schema (DDL, migrações, Prisma migrations) for executada no banco `axie_treasury`.
- Quando o usuário solicitar atualização ou verificação do schema SQL do banco de dados.

## Como Executar a Sincronização

Execute o script de introspecção e sincronização automática:

```powershell
python "C:\Users\danma\OneDrive\Área de Trabalho\PROD VPS\treasury-update\postgres\sync_schema.py"
```

### O que o script faz:
1. Conecta ao PostgreSQL através das credenciais do arquivo `.env` (utilizando túnel SSH automaticamente se `SSH_HOST` estiver presente, ou direto se estiver rodando dentro da VPS).
2. Faz a varredura completa das tabelas públicas, colunas, tipos de dados, chaves primárias e índices (`information_schema` e `pg_indexes`).
3. Recupera a definição de todas as views criadas.
4. Gera e sobrescreve o arquivo `schema.sql` com o DDL completo, atualizado e formatado.

## Regras Obrigatórias para Modificações no Banco
1. Nunca altere o banco sem em seguida rodar a sincronização.
2. Certifique-se de que os nomes de tabelas CamelCase (`"overviewData"`, `"txns"`, `"dailyAXS"`, etc.) e as views snake_case correspondentes estejam documentadas.
3. Valide que `schema.sql` reflete fielmente as novas colunas ou índices criados.
