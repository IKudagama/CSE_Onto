# CSE Ontology System — Setup Guide

## Architecture

```
React Frontend  ──►  FastAPI (port 8000)  ──►  Apache Jena Fuseki (port 3030)
                         main.py                   /CSE dataset
```

---

## 1. Apache Jena Fuseki

**Download:** https://jena.apache.org/download/

```bash
# Start Fuseki with an in-memory dataset named /CSE
./fuseki-server --update --mem /CSE

# OR with a persistent dataset
./fuseki-server --update --loc=/data/CSE /CSE
```

**Load the ontology:**

1. Open http://localhost:3030
2. Go to **dataset** → **/CSE** → **upload files**
3. Upload `cse_ontology.owl`
4. Click **Upload now**

Verify: http://localhost:3030/CSE/query

---

## 2. FastAPI Backend

```bash
# Install dependencies
pip install -r requirements.txt

# Run (auto-reload for development)
uvicorn main:app --reload --port 8000
```

**API docs:** http://localhost:8000/docs

**Endpoints:**

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | Health check |
| GET | `/health` | API + Fuseki connectivity |
| GET | `/queries` | List all 8 competency queries |
| GET | `/queries/{id}` | Execute query CQ1–CQ8 |
| POST | `/sparql` | Execute custom SPARQL (body: `{"sparql": "..."}`) |

---

## 3. React Frontend

```bash
# In your React project root
npm install
# Place CSEOntologyExplorer.jsx in src/
# Import in App.jsx:
#   import CSEOntologyExplorer from './CSEOntologyExplorer';

npm run dev
```

The frontend auto-detects FastAPI on startup. If unavailable, it falls back to embedded demo data.

---

## Files

```
cse_ontology.owl          OWL 2 ontology (simplified)
main.py                   FastAPI backend
requirements.txt          Python dependencies
CSEOntologyExplorer.jsx   React frontend component
README.md                 This file
```

---

## SPARQL Prefix (auto-prepended by FastAPI)

```sparql
PREFIX cse:  <http://www.semanticweb.org/ontologies/CSE#>
PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
PREFIX owl:  <http://www.w3.org/2002/07/owl#>
```
