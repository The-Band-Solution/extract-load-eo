from py2neo import Graph  # noqa: I001
import os  # noqa: I001


# Conexão
graph = Graph(
    os.getenv("bolt://localhost:7687", ""), auth=("neo4j", "G7q!rX#9Lp@eZ1vK")
)

# Query Cypher
# Query para contar commits
query = "MATCH (c:Commit) RETURN count(c) AS total_commits"

# Executar e obter o resultado
result = graph.run(query).data()

# Acessar o valor
total = result[0]["total_commits"]
print(f"📦 Total de commits: {total}")
