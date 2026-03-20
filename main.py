"""
CSE Ontology — FastAPI Backend
================================
Zero hardcoded SPARQL queries.  Every query that reaches Fuseki is either:
  (a) typed by the user in the Query Explorer, or
  (b) generated on-the-fly by Claude for the chat endpoint.

Endpoints
---------
  GET  /health          — connectivity check (API / Fuseki / Claude key)
  POST /sparql          — execute any raw SPARQL SELECT against Fuseki
  POST /chat            — agentic pipeline:
                           1. Claude writes SPARQL from user question
                           2. FastAPI runs it on Fuseki
                           3. On error, Claude auto-fixes and retries once
                           4. Claude answers from the real triplestore rows

Setup
-----
  pip install fastapi uvicorn httpx
  export ANTHROPIC_API_KEY=sk-ant-...
  uvicorn main:app --reload --port 8000

  Fuseki: ./fuseki-server --update --mem /CSE
          then upload cse_ontology.owl at http://localhost:3030
"""

import os, json, re
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
import httpx

# ── Configuration ──────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
FUSEKI_URL        = "http://localhost:3030/Stock_Onto/query"
CLAUDE_MODEL      = "claude-sonnet-4-20250514"

# These prefixes are auto-prepended to every query so the client/Claude never
# needs to include them.
SPARQL_PREFIXES = """\
PREFIX cse:  <http://www.semanticweb.org/ontologies/CSE#>
PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
PREFIX owl:  <http://www.w3.org/2002/07/owl#>
"""

# ── Ontology description injected into every Claude call ──────────────────────
# This is schema documentation, not data — correctly placed here.
ONTOLOGY_SCHEMA = """\
=== CSE OWL 2 ONTOLOGY  (Colombo Stock Exchange) ===
Namespace  cse: = http://www.semanticweb.org/ontologies/CSE#
Prefixes are auto-prepended — NEVER include PREFIX lines in your output.

── CLASSES & INDIVIDUALS ──────────────────────────────────────────
cse:Stock  (abstract)
  └─ cse:BlueChipStock   individuals: cse:HNB  cse:COMB  cse:JKH
  └─ cse:GrowthStock     individuals: cse:DIAL  cse:LOLC
  └─ cse:ValueStock      individuals: cse:HAYL  cse:LIOC
  owl:AllDisjointClasses {BlueChipStock, GrowthStock, ValueStock}

cse:RiskLevel  (abstract)
  └─ cse:LowRisk    individual: cse:Low
  └─ cse:MediumRisk individual: cse:Medium
  └─ cse:HighRisk   individual: cse:High
  owl:AllDisjointClasses {LowRisk, MediumRisk, HighRisk}

cse:Sector  individuals: cse:Banking  cse:Diversified  cse:Manufacturing
                         cse:TelecomIT  cse:Energy

cse:Investor  (abstract)
  └─ cse:RetailInvestor         individuals: cse:Kasun  cse:Nimali
  └─ cse:InstitutionalInvestor  individual:  cse:EPF

cse:Transaction  (abstract)
  └─ cse:BuyTransaction   individuals: cse:TX001  cse:TX002  cse:TX004
  └─ cse:SellTransaction  individual:  cse:TX003

── OBJECT PROPERTIES  (all owl:FunctionalProperty) ────────────────
cse:hasRiskLevel     Stock       → RiskLevel    cse:Low / cse:Medium / cse:High
cse:belongsToSector  Stock       → Sector
cse:performedBy      Transaction → Investor
cse:involves         Transaction → Stock

OWL cardinality restriction: every Stock has exactly 1 hasRiskLevel
                              and exactly 1 belongsToSector.

── DATATYPE PROPERTIES ────────────────────────────────────────────
cse:ticker          xsd:string   Stock
cse:companyName     xsd:string   Stock
cse:price           xsd:decimal  Stock        (LKR)
cse:peRatio         xsd:decimal  Stock
cse:dividendYield   xsd:decimal  Stock        (%)
cse:beta            xsd:decimal  Stock
cse:investorName    xsd:string   Investor
cse:txDate          xsd:date     Transaction
cse:quantity        xsd:integer  Transaction
cse:txPrice         xsd:decimal  Transaction  (LKR per share)

── rdfs:label  (retrieve with: ?x rdfs:label ?label) ──────────────
Sectors : cse:Banking->"Banking & Finance"  cse:Diversified->"Diversified"
          cse:Manufacturing->"Manufacturing" cse:TelecomIT->"Telecom & IT"
          cse:Energy->"Power & Energy"
Risk    : cse:Low->"Low Risk"  cse:Medium->"Medium Risk"  cse:High->"High Risk"
Trans.  : cse:TX001 rdfs:label "TX001" … cse:TX004 rdfs:label "TX004"

── SPARQL WRITING RULES ────────────────────────────────────────────
1. SELECT queries only (no CONSTRUCT / DESCRIBE / ASK / UPDATE).
2. Do NOT include PREFIX lines — they are prepended automatically.
3. For readable sector/risk labels: ?sec rdfs:label ?sectorLabel
4. To filter unwanted rdf:type on transactions:
   FILTER(?txType != owl:NamedIndividual)
5. To compute total value: BIND((?quantity * ?txPrice) AS ?total)
6. Always ORDER BY something. LIMIT 20 maximum.
7. If a question cannot be answered from this ontology at all,
   output exactly the token: NO_SPARQL_NEEDED
"""

# ── App ────────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="CSE Ontology API",
    description=(
        "Fully dynamic — zero hardcoded SPARQL. "
        "Every query is either user-written or generated live by Claude."
    ),
    version="4.0.0",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── SPARQL helpers ─────────────────────────────────────────────────────────────
def add_prefixes(query_body: str) -> str:
    """Strip any PREFIX lines Claude may have included, then prepend the canonical set."""
    clean = "\n".join(
        line for line in query_body.splitlines()
        if not line.strip().upper().startswith("PREFIX")
    )
    return SPARQL_PREFIXES + "\n" + clean.strip()


def parse_bindings(raw: dict) -> dict:
    vars_ = raw["head"]["vars"]
    rows  = [
        {v: b[v]["value"] if v in b else None for v in vars_}
        for b in raw["results"]["bindings"]
    ]
    return {"vars": vars_, "rows": rows}


async def execute_sparql(query_body: str) -> dict:
    full = add_prefixes(query_body)
    async with httpx.AsyncClient(timeout=12.0) as c:
        r = await c.post(
            FUSEKI_URL,
            data={"query": full},
            headers={"Accept": "application/sparql-results+json"},
        )
    if r.status_code != 200:
        raise RuntimeError(f"Fuseki {r.status_code}: {r.text[:300]}")
    return parse_bindings(r.json())


# ── Claude helper ──────────────────────────────────────────────────────────────
async def call_claude(system: str, messages: list) -> str:
    if not ANTHROPIC_API_KEY:
        raise HTTPException(status_code=503, detail="ANTHROPIC_API_KEY not set.")
    async with httpx.AsyncClient(timeout=40.0) as c:
        r = await c.post(
            "https://api.anthropic.com/v1/messages",
            json={
                "model": CLAUDE_MODEL,
                "max_tokens": 1024,
                "system": system,
                "messages": messages,
            },
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
        )
    if r.status_code != 200:
        raise HTTPException(
            status_code=502,
            detail=f"Claude API {r.status_code}: {r.text[:300]}",
        )
    raw = "".join(b.get("text", "") for b in r.json().get("content", []))
    # Strip accidental markdown fences
    raw = re.sub(r"^```[a-z]*\n?", "", raw.strip(), flags=re.MULTILINE)
    raw = re.sub(r"\n?```$",       "", raw.strip(), flags=re.MULTILINE)
    return raw.strip()


def extract_select(text: str) -> Optional[str]:
    """Extract a SELECT … block from free-form text."""
    m = re.search(r"```(?:sparql)?\n(.*?)```", text, re.DOTALL | re.IGNORECASE)
    if m:
        return m.group(1).strip()
    m = re.search(r"(SELECT\b.+)", text, re.DOTALL | re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return None


# ── Pydantic models ────────────────────────────────────────────────────────────
class SparqlRequest(BaseModel):
    sparql: str

class ChatMessage(BaseModel):
    role: str       # "user" | "assistant"
    content: str

class ChatRequest(BaseModel):
    messages: List[ChatMessage]


# ── Routes ─────────────────────────────────────────────────────────────────────
@app.get("/health", summary="Check API / Fuseki / Claude key connectivity")
async def health():
    fuseki_ok = False
    try:
        async with httpx.AsyncClient(timeout=4.0) as c:
            # Test with a simple SPARQL query instead of ping
            r = await c.post(
                FUSEKI_URL,
                data={"query": "SELECT * WHERE { ?s ?p ?o } LIMIT 1"},
                headers={"Accept": "application/sparql-results+json"}
            )
        fuseki_ok = r.status_code < 400
    except Exception:
        pass
    return {
        "api":            "ok",
        "fuseki":         "ok" if fuseki_ok else "unreachable",
        "claude_key_set": bool(ANTHROPIC_API_KEY),
    }


@app.post(
    "/sparql",
    summary="Execute a raw SPARQL SELECT against Fuseki",
    description=(
        "Accepts any SPARQL SELECT query body (no PREFIX lines needed — they are "
        "prepended automatically).  Used by the Query Explorer tab in the frontend. "
        "Returns {vars: [...], rows: [...]}."
    ),
)
async def run_sparql(body: SparqlRequest):
    try:
        return await execute_sparql(body.sparql)
    except RuntimeError as e:
        raise HTTPException(status_code=502, detail=str(e))


@app.post(
    "/chat",
    summary="Agentic chat — NL question → Claude-generated SPARQL → Fuseki → grounded answer",
    description=(
        "Step 1: Claude reads the question and writes a SPARQL SELECT query.\n"
        "Step 2: FastAPI executes it on the live Fuseki triplestore.\n"
        "        If it fails, Claude auto-corrects and retries once.\n"
        "Step 3: Claude receives the real Fuseki rows and writes a grounded answer.\n"
        "        Conversation history is forwarded for multi-turn context.\n\n"
        "Returns {answer, sparql, vars, rows, insight, followUps}."
    ),
)
async def chat(body: ChatRequest):
    user_question = body.messages[-1].content

    # ── Step 1: Claude generates SPARQL ───────────────────────────────────────
    sparql_system = (
        "You are a SPARQL 1.1 expert for the Colombo Stock Exchange OWL 2 ontology.\n"
        "Your sole task: write ONE correct SPARQL SELECT query that answers the question.\n\n"
        "Strict output rules:\n"
        "  • Raw SPARQL only — no explanation, no markdown fences, no PREFIX lines.\n"
        "  • If the question is purely conceptual and cannot be answered by querying "
        "    instances (e.g. 'what is OWL?'), output exactly: NO_SPARQL_NEEDED\n\n"
        + ONTOLOGY_SCHEMA
    )
    raw_sparql = await call_claude(
        sparql_system,
        [{"role": "user",
          "content": f"Write SPARQL to answer this question:\n{user_question}"}],
    )

    # ── Step 2: Execute on Fuseki (with one auto-correction retry) ─────────────
    sparql_query  = None
    fuseki_result = {"vars": [], "rows": []}
    exec_note     = ""

    if raw_sparql.strip() != "NO_SPARQL_NEEDED":
        sparql_query = extract_select(raw_sparql) or raw_sparql.strip()

        try:
            fuseki_result = await execute_sparql(sparql_query)
        except Exception as err1:
            # Ask Claude to fix the broken query — one retry
            fix_raw = await call_claude(
                sparql_system,
                [{"role": "user",
                  "content": (
                      f"The following SPARQL query failed:\n\n{sparql_query}\n\n"
                      f"Error: {err1}\n\n"
                      "Rewrite it correctly. Raw SPARQL only, nothing else."
                  )}],
            )
            sparql_query = extract_select(fix_raw) or fix_raw.strip()
            try:
                fuseki_result = await execute_sparql(sparql_query)
                exec_note = " [auto-corrected on retry]"
            except Exception as err2:
                exec_note = f" [failed after retry: {err2}]"

    # ── Step 3: Claude answers from real Fuseki rows ───────────────────────────
    if fuseki_result["rows"]:
        data_block = (
            f"=== SPARQL EXECUTED ON FUSEKI{exec_note} ===\n"
            f"{sparql_query}\n\n"
            f"=== LIVE RESULTS ({len(fuseki_result['rows'])} rows) ===\n"
            f"{json.dumps(fuseki_result['rows'], indent=2)}"
        )
    elif exec_note:
        data_block = f"=== SPARQL FAILED{exec_note} ===\nNo triplestore data available."
    else:
        data_block = "=== NO SPARQL NEEDED — conceptual question ==="

    answer_system = (
        "You are CSEBot — expert assistant for the Colombo Stock Exchange OWL 2 ontology.\n"
        "You MUST base every factual claim on the Fuseki results below.\n"
        "Do NOT invent data.  Monetary values are in LKR (Sri Lankan Rupees).\n\n"
        + ONTOLOGY_SCHEMA + "\n\n"
        + data_block + "\n\n"
        "Reply with ONLY valid JSON — no markdown fences, no extra text:\n"
        "{\n"
        '  "answer":    "2-3 sentences grounded in the Fuseki results; cite real values",\n'
        '  "sparql":    "the exact SPARQL string executed, or null",\n'
        '  "vars":      ["col1", "col2"],\n'
        '  "rows":      [{"col": "val"}],\n'
        '  "insight":   "one concrete observation from the data, or null",\n'
        '  "followUps": ["follow-up 1?", "follow-up 2?", "follow-up 3?"]\n'
        "}\n"
        "vars/rows must come from the Fuseki results (max 8 rows, max 6 vars). "
        "vars keys must match row keys exactly. Always 3 followUps."
    )

    # Forward full conversation history for multi-turn context
    answer_raw = await call_claude(
        answer_system,
        [{"role": m.role, "content": m.content} for m in body.messages],
    )

    try:
        parsed = json.loads(answer_raw)
    except Exception:
        parsed = {
            "answer":    answer_raw[:600],
            "sparql":    sparql_query,
            "vars":      fuseki_result["vars"][:6],
            "rows":      fuseki_result["rows"][:8],
            "insight":   None,
            "followUps": [
                "What are the high-risk CSE stocks?",
                "Show all dividend yields",
                "Show transaction history",
            ],
        }

    # Always surface the SPARQL that actually ran
    if sparql_query and not parsed.get("sparql"):
        parsed["sparql"] = sparql_query

    return parsed


# ── Rule-based chat (No LLM required) ──────────────────────────────────────────
PATTERNS = [
    # High risk stocks
    (r"high.?risk|risky|dangerous", 
     "SELECT ?ticker ?companyName ?price ?riskLabel WHERE { ?stock cse:ticker ?ticker ; cse:companyName ?companyName ; cse:price ?price ; cse:hasRiskLevel ?risk . ?risk rdfs:label ?riskLabel . FILTER(CONTAINS(LCASE(?riskLabel), 'high')) } ORDER BY ?ticker",
     "High-risk stocks in the CSE ontology"),
    
    # Low risk stocks
    (r"low.?risk|safe|stable",
     "SELECT ?ticker ?companyName ?price ?riskLabel WHERE { ?stock cse:ticker ?ticker ; cse:companyName ?companyName ; cse:price ?price ; cse:hasRiskLevel ?risk . ?risk rdfs:label ?riskLabel . FILTER(CONTAINS(LCASE(?riskLabel), 'low')) } ORDER BY ?ticker",
     "Low-risk stocks in the CSE ontology"),
    
    # Dividend yield
    (r"dividend|yield",
     "SELECT ?ticker ?companyName ?dividendYield ?price WHERE { ?stock cse:ticker ?ticker ; cse:companyName ?companyName ; cse:dividendYield ?dividendYield ; cse:price ?price . } ORDER BY DESC(?dividendYield)",
     "Stocks ranked by dividend yield"),
    
    # Blue chip stocks
    (r"blue.?chip",
     "SELECT ?ticker ?companyName ?price ?riskLabel ?sectorLabel WHERE { ?stock a cse:BlueChipStock ; cse:ticker ?ticker ; cse:companyName ?companyName ; cse:price ?price ; cse:hasRiskLevel ?risk ; cse:belongsToSector ?sec . ?risk rdfs:label ?riskLabel . ?sec rdfs:label ?sectorLabel . } ORDER BY ?ticker",
     "All Blue Chip stocks"),
    
    # Growth stocks
    (r"growth.?stock",
     "SELECT ?ticker ?companyName ?price ?beta WHERE { ?stock a cse:GrowthStock ; cse:ticker ?ticker ; cse:companyName ?companyName ; cse:price ?price ; cse:beta ?beta . } ORDER BY ?ticker",
     "All Growth stocks"),
    
    # Value stocks
    (r"value.?stock",
     "SELECT ?ticker ?companyName ?price ?peRatio WHERE { ?stock a cse:ValueStock ; cse:ticker ?ticker ; cse:companyName ?companyName ; cse:price ?price ; cse:peRatio ?peRatio . } ORDER BY ?ticker",
     "All Value stocks"),
    
    # Transactions
    (r"transaction|trade|buy|sell",
     "SELECT ?id ?investorName ?ticker ?txType ?quantity ?txPrice ((?quantity * ?txPrice) AS ?total) ?txDate WHERE { ?tx rdfs:label ?id ; rdf:type ?txType ; cse:performedBy ?inv ; cse:involves ?stock ; cse:quantity ?quantity ; cse:txPrice ?txPrice ; cse:txDate ?txDate . ?inv cse:investorName ?investorName . ?stock cse:ticker ?ticker . FILTER(?txType != owl:NamedIndividual) } ORDER BY ?txDate",
     "All transactions with total values"),
    
    # Banking sector
    (r"bank|banking",
     "SELECT ?ticker ?companyName ?price ?sectorLabel WHERE { ?stock cse:ticker ?ticker ; cse:companyName ?companyName ; cse:price ?price ; cse:belongsToSector ?sec . ?sec rdfs:label ?sectorLabel . FILTER(CONTAINS(LCASE(?sectorLabel), 'bank')) } ORDER BY ?ticker",
     "Banking sector stocks"),
    
    # P/E ratio
    (r"p.?e.?ratio|price.?earning",
     "SELECT ?ticker ?companyName ?price ?peRatio WHERE { ?stock cse:ticker ?ticker ; cse:companyName ?companyName ; cse:price ?price ; cse:peRatio ?peRatio . } ORDER BY ?peRatio",
     "Stocks ranked by P/E ratio"),
    
    # Beta / volatility
    (r"beta|volatil|volatile",
     "SELECT ?ticker ?companyName ?price ?beta WHERE { ?stock cse:ticker ?ticker ; cse:companyName ?companyName ; cse:price ?price ; cse:beta ?beta . } ORDER BY DESC(?beta)",
     "Stocks ranked by beta (volatility)"),
    
    # Investors
    (r"investor|who.*invest",
     "SELECT ?investorName ?investorType WHERE { ?inv cse:investorName ?investorName ; rdf:type ?investorType . FILTER(?investorType != owl:NamedIndividual) FILTER(?investorType != cse:Investor) } ORDER BY ?investorName",
     "All investors in the system"),
    
    # All stocks
    (r"all.?stock|list.*stock|show.*stock",
     "SELECT ?ticker ?companyName ?price ?stockType WHERE { ?stock cse:ticker ?ticker ; cse:companyName ?companyName ; cse:price ?price ; rdf:type ?stockType . FILTER(?stockType != owl:NamedIndividual) FILTER(?stockType != cse:Stock) } ORDER BY ?ticker",
     "All stocks in the CSE ontology"),
    
    # Highest/most expensive
    (r"highest|most expensive|expensive|maximum price",
     "SELECT ?ticker ?companyName ?price ?stockType WHERE { ?stock cse:ticker ?ticker ; cse:companyName ?companyName ; cse:price ?price ; rdf:type ?stockType . FILTER(?stockType != owl:NamedIndividual) FILTER(?stockType != cse:Stock) } ORDER BY DESC(?price) LIMIT 5",
     "Most expensive stocks"),
    
    # Lowest/cheapest
    (r"lowest|cheapest|cheap|minimum price",
     "SELECT ?ticker ?companyName ?price ?stockType WHERE { ?stock cse:ticker ?ticker ; cse:companyName ?companyName ; cse:price ?price ; rdf:type ?stockType . FILTER(?stockType != owl:NamedIndividual) FILTER(?stockType != cse:Stock) } ORDER BY ?price LIMIT 5",
     "Cheapest stocks"),
]

FOLLOW_UPS = [
    "What are the high-risk CSE stocks?",
    "Show all stocks ranked by dividend yield",
    "List all transactions with totals",
    "Which stocks are in the banking sector?",
    "What are the blue chip stocks?",
    "Show the most expensive stocks",
]

@app.post(
    "/chat-simple",
    summary="Rule-based chat — No LLM required",
    description=(
        "Pattern-matching chatbot that converts common questions to SPARQL queries "
        "without requiring Claude API. Returns {answer, sparql, vars, rows, insight, followUps}."
    ),
)
async def chat_simple(body: ChatRequest):
    user_question = body.messages[-1].content.lower()
    
    # Find matching pattern
    sparql_query = None
    description = None
    for pattern, query, desc in PATTERNS:
        if re.search(pattern, user_question, re.IGNORECASE):
            sparql_query = query
            description = desc
            break
    
    # Default query if no match
    if not sparql_query:
        sparql_query = "SELECT ?ticker ?companyName ?price WHERE { ?stock cse:ticker ?ticker ; cse:companyName ?companyName ; cse:price ?price . } ORDER BY ?ticker LIMIT 10"
        description = "All stocks (default query)"
    
    # Execute query
    try:
        result = await execute_sparql(sparql_query)
        vars_ = result["vars"]
        rows = result["rows"][:8]  # Limit to 8 rows
        
        # Generate answer
        if rows:
            answer = f"{description}. Found {len(result['rows'])} result{'s' if len(result['rows']) != 1 else ''}."
            if len(result['rows']) > 8:
                answer += f" Showing first 8."
            
            # Add insight based on data
            insight = None
            if "price" in vars_ and rows:
                prices = [float(r.get("price", 0)) for r in rows if r.get("price")]
                if prices:
                    avg_price = sum(prices) / len(prices)
                    insight = f"Average price: LKR {avg_price:.2f}"
            elif "dividendYield" in vars_ and rows:
                top = rows[0].get("dividendYield")
                ticker = rows[0].get("ticker", "Unknown")
                if top:
                    insight = f"Highest dividend yield: {ticker} at {top}%"
        else:
            answer = "No results found for your query."
            insight = None
        
        return {
            "answer": answer,
            "sparql": sparql_query,
            "vars": vars_[:6],
            "rows": rows,
            "insight": insight,
            "followUps": FOLLOW_UPS[:3]
        }
    
    except Exception as e:
        return {
            "answer": f"Sorry, I encountered an error: {str(e)}",
            "sparql": sparql_query,
            "vars": [],
            "rows": [],
            "insight": None,
            "followUps": FOLLOW_UPS[:3]
        }