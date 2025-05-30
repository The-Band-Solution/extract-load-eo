# 🔗 GitHub Team Extractor + Neo4j Mapper

Este projeto conecta-se à API do GitHub para extrair os dados de **times** e **membros** de uma organização, e os salva em um banco de dados **Neo4j** modelado com base na ontologia **SEON/UFO-C**.

---

## 📌 Funcionalidades

- ✅ Busca todas as equipes da organização no GitHub
- ✅ Busca os membros de cada equipe
- ✅ Modela com Pydantic (Team, Member, TeamWithMembers)
- ✅ Persiste os dados em Neo4j com estrutura semântica:
  - `Person`, `Team`, `TeamMembership`, `TeamMember`, `OrganizationalRole`
- ✅ Usa o driver oficial `neo4j-driver` (suporte oficial da Neo4j)

---

## 🧱 Estrutura Ontológica (SEON/UFO-C)

```plaintext
(:Person)-[:ALLOCATES]->(:TeamMember)
(:TeamMember)<-[:ALLOCATES]-(:TeamMembership)
(:TeamMembership)-[:IS_TO_PLAY]->(:OrganizationalRole)
(:TeamMembership)-[:DONE_FOR]->(:Team)
```

---

## 🚀 Como executar

### 1. Clone o repositório

```bash
git clone https://github.com/seu-usuario/github-neo4j-extractor.git
cd github-neo4j-extractor
```

### 2. Instale as dependências

```bash
pip install -r requirements.txt
```

Ou manualmente:

```bash
pip install PyGithub neo4j pydantic
```

### 3. Configure suas variáveis

Crie um arquivo `.env` com:

```
GITHUB_TOKEN=ghp_seu_token
GITHUB_ORG=nome-da-organizacao
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=sua_senha
```

### 4. Execute o script principal

```bash
python src/main.py
```

---

## 🛠 Estrutura do Projeto

```
📁 src/
├── main.py                # Ponto de entrada
├── github_client.py       # Classe GitHubClient com PyGithub + Pydantic
├── sync_neo4j.py          # Classe que envia para o Neo4j
📁 models/
├── team.py                # Team e TeamWithMembers
├── member.py              # MemberModel
```

---

## 🔎 Visualização no Neo4j

Acesse:

```
http://localhost:7474
```

E use estas queries:

```cypher
MATCH (n) RETURN n LIMIT 100
```

```cypher
MATCH (p:Person)-[:ALLOCATES]->(:TeamMember)<-[:ALLOCATES]-(:TeamMembership)-[:IS_TO_PLAY]->(:OrganizationalRole),
      (:TeamMembership)-[:DONE_FOR]->(:Team)
RETURN p, TeamMember, TeamMembership, OrganizationalRole, Team
```

---

## 📦 Requisitos

- Python 3.8+
- Neo4j Desktop ou Neo4j em Docker
- GitHub Personal Access Token com scope `read:org`

---

