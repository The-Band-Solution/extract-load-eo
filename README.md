
# 🔗 EO Extractor

This project connects to the GitHub API to extract **teams** and **members** from an organization and saves them to a **Neo4j** graph database modeled based on the **SEON/UFO-C ontology**.

---

## 📌 Features

- ✅ Fetches all teams from a GitHub organization
- ✅ Retrieves members of each team
- ✅ Models data using Pydantic (Team, Member, TeamWithMembers)
- ✅ Persists data in Neo4j using a semantic structure:
  - `Person`, `Team`, `TeamMembership`, `TeamMember`, `OrganizationalRole`

---

## 🧱 Continuum (SEON/UFO-C)

Continuous Software Engineering (CSE) is a complex domain that involves Business, Software Engineering, Operations, and Innovation domains to deliver products or services thatfulfill the customers’ demand. Aiming to provide knowledge about CSE, we have worked on an ontology network, called Continuum1, which aims at representing the conceptualization related to the processes involved in CSE. In the SE big picture, CSE appears as a (large) subdomain involving other subdomains. Thus, we developed Continuum as a subnetwork of [SEON](https://dev.nemo.inf.ufes.br/seon/). In this sense, we reused some elements of SEON (such as its architecture, integration mechanisms, and networked ontologies) to develop Continuum.

![Continuum](./figures/continuum.png)


### Enterprise Ontology (EO)

The Enterprise Ontology (EO) aims at establishing a common conceptualization on the Entreprise domain, including organizations, organizational units, people, roles, teams and projects.

![texto alternativo](./figures/eo.png)


```plaintext
(:Person)-[:ALLOCATES]->(:TeamMember)
(:TeamMember)<-[:ALLOCATES]-(:TeamMembership)
(:TeamMembership)-[:IS_TO_PLAY]->(:OrganizationalRole)
(:TeamMembership)-[:DONE_FOR]->(:Team)
````


### Configuration Management Process Ontology (CMPO)

The Configuration Management Process Ontology (CMPO) aims at representing the activities, artifacts and stakeholders involved in the software Configuration Management Process. Since CMPO can be applied in the context of several SE subdomains, it describes some general notions applicable for diverse SEON concepts.


### Continuous Integration Reference Ontology (CIRO)



---

## 🚀 How to Run

### 1. Clone the repositorys

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure environment variables

Create a `.env` file with:

```
GITHUB_TOKEN=ghp_your_token
GITHUB_ORG=your-org-name
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_password
```

### 4. Run the main script

```bash
python src/main.py
```

---

## 🛠 Project Structure

```
📁 src/
├── main.py                # Entry point
├── github_client.py       # GitHubClient class using PyGithub + Pydantic
├── sync_neo4j.py          # Class to send data to Neo4j
📁 models/
├── team.py                # Team and TeamWithMembers models
├── member.py              # MemberModel
```

---

## 🔎 Visualizing in Neo4j

Access:

```
http://localhost:7474
```

Then run these queries:

```cypher
MATCH (n) RETURN n LIMIT 100
```

```cypher
MATCH (p:Person)-[:ALLOCATES]->(:TeamMember)<-[:ALLOCATES]-(:TeamMembership)-[:IS_TO_PLAY]->(:OrganizationalRole),
      (:TeamMembership)-[:DONE_FOR]->(:Team)
RETURN p, TeamMember, TeamMembership, OrganizationalRole, Team
```

---

## 📦 Requirements

* Python 3.8+
* Neo4j Desktop or Neo4j running in Docker
* GitHub Personal Access Token with `read:org` scope
