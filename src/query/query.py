from py2neo import Graph  # noqa: I001
import os  # noqa: I001
from collections import defaultdict  # noqa: I001


# Conexão
graph = Graph(
    os.getenv("bolt://localhost:7687", ""), auth=("neo4j", "G7q!rX#9Lp@eZ1vK")
)


# Query Cypher
query = """
MATCH (c:Commit)-[r]->(n)
RETURN c.sha AS sha, c.message AS message, c.date AS date,
       type(r) AS rel_type, labels(n) AS labels, properties(n) AS props
ORDER BY c.sha
LIMIT 7000
"""

# Executa a query
results = graph.run(query).data()

# Agrupa os relacionamentos por commit SHA
commits = defaultdict(list)
for row in results:
    commits[row["sha"]].append(row)

# Gera o conteúdo em Markdown
markdown = ["# 📘 Commits e Relacionamentos\n"]

for sha, rows in commits.items():
    commit_info = rows[0]
    markdown.append(f"### 🔨 Commit: `{sha}`")
    markdown.append(f"**Mensagem:** {commit_info['message'] or '-'}  ")
    markdown.append(f"**Data:** {commit_info['date'] or '-'}\n")
    markdown.append("| Tipo de Relacionamento | Tipo de Nó Conectado | Atributos |")
    markdown.append("|------------------------|----------------------|-----------|")

    for row in rows:
        rel_type = row["rel_type"]
        label = ", ".join(row["labels"])
        props = ", ".join(f"{k}: {str(v)}" for k, v in row["props"].items())
        markdown.append(f"| {rel_type} | {label} | {props} |")

    markdown.append("")  # quebra de linha

# Salva em um arquivo commits.md
with open("commits.md", "w", encoding="utf-8") as f:
    f.write("\n".join(markdown))

print("✅ Arquivo 'commits.md' gerado com sucesso!")
