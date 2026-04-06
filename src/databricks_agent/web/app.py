"""FastAPI web configuration app for databricks-agent."""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

app = FastAPI(
    title="databricks-agent UI",
    description="Visual configuration tool for Databricks pipelines and connections",
    version="0.1.0",
)


@app.get("/", response_class=HTMLResponse)
async def index():
    return """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>databricks-agent — Tron Ares</title>
  <style>
    /* ── Tron Ares Theme ── electric cyan × fiery orange on void black ── */
    :root {
      --bg-void:    #04060f;
      --bg-panel:   #080d1a;
      --bg-card:    #0c1428;
      --cyan:       #00f0ff;
      --cyan-dim:   #007a88;
      --ares:       #ff4e00;
      --ares-dim:   #7a2500;
      --text:       #c8e8f0;
      --text-dim:   #4a7080;
      --border:     #0a3040;
      --glow-cyan:  0 0 12px #00f0ff66, 0 0 32px #00f0ff22;
      --glow-ares:  0 0 12px #ff4e0066, 0 0 32px #ff4e0022;
    }

    * { box-sizing: border-box; margin: 0; padding: 0; }

    body {
      font-family: 'Courier New', 'Lucida Console', monospace;
      background: var(--bg-void);
      color: var(--text);
      min-height: 100vh;
      overflow-x: hidden;
      position: relative;
    }

    /* Grid background */
    body::before {
      content: '';
      position: fixed; inset: 0;
      background-image:
        linear-gradient(var(--border) 1px, transparent 1px),
        linear-gradient(90deg, var(--border) 1px, transparent 1px);
      background-size: 40px 40px;
      opacity: 0.35;
      pointer-events: none;
      z-index: 0;
    }

    /* Scan-line overlay */
    body::after {
      content: '';
      position: fixed; inset: 0;
      background: repeating-linear-gradient(
        0deg, transparent, transparent 2px, rgba(0,240,255,0.015) 2px, rgba(0,240,255,0.015) 4px
      );
      pointer-events: none;
      z-index: 0;
    }

    .wrapper {
      position: relative; z-index: 1;
      display: flex; flex-direction: column; align-items: center;
      padding: 3rem 1.5rem;
      min-height: 100vh;
    }

    /* ── Header ── */
    header { text-align: center; margin-bottom: 3rem; }

    .logo-bar {
      display: inline-block;
      border: 1px solid var(--cyan);
      border-top: 3px solid var(--cyan);
      border-bottom: 3px solid var(--ares);
      padding: 1.2rem 3rem;
      box-shadow: var(--glow-cyan), inset 0 0 30px rgba(0,240,255,0.04);
      position: relative;
    }
    .logo-bar::before, .logo-bar::after {
      content: '';
      position: absolute; top: 50%; width: 2rem; height: 1px;
      background: var(--cyan);
    }
    .logo-bar::before { right: 100%; margin-right: 8px; }
    .logo-bar::after  { left: 100%;  margin-left: 8px; }

    h1 {
      font-size: 2.2rem; letter-spacing: 0.2em; font-weight: 700;
      color: var(--cyan);
      text-shadow: var(--glow-cyan);
      text-transform: uppercase;
    }
    h1 span { color: var(--ares); text-shadow: var(--glow-ares); }

    .tagline {
      margin-top: 0.6rem;
      font-size: 0.75rem; letter-spacing: 0.35em; text-transform: uppercase;
      color: var(--text-dim);
    }

    .author-line {
      margin-top: 0.5rem;
      font-size: 0.7rem; letter-spacing: 0.2em; color: var(--cyan-dim);
    }
    .author-line a { color: var(--ares); text-decoration: none; }
    .author-line a:hover { color: var(--cyan); text-shadow: var(--glow-cyan); }

    /* ── Status bar ── */
    .status-bar {
      width: 100%; max-width: 860px;
      display: flex; align-items: center; gap: 1rem;
      border: 1px solid var(--cyan-dim);
      padding: 0.6rem 1.2rem;
      margin-bottom: 2.5rem;
      font-size: 0.72rem; letter-spacing: 0.1em; text-transform: uppercase;
      background: rgba(0,240,255,0.03);
    }
    .status-dot {
      width: 8px; height: 8px; border-radius: 50%;
      background: var(--cyan);
      box-shadow: var(--glow-cyan);
      animation: pulse 2s infinite;
      flex-shrink: 0;
    }
    @keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.4; } }

    /* ── Cards ── */
    .grid {
      width: 100%; max-width: 860px;
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
      gap: 1.2rem;
    }

    .card {
      background: var(--bg-card);
      border: 1px solid var(--border);
      border-top: 2px solid var(--cyan-dim);
      padding: 1.5rem;
      transition: border-color 0.2s, box-shadow 0.2s;
    }
    .card:hover {
      border-color: var(--cyan);
      box-shadow: var(--glow-cyan);
    }
    .card.ares { border-top-color: var(--ares-dim); }
    .card.ares:hover { border-color: var(--ares); box-shadow: var(--glow-ares); }

    .card-label {
      font-size: 0.62rem; letter-spacing: 0.3em; text-transform: uppercase;
      color: var(--cyan-dim); margin-bottom: 0.8rem;
    }
    .card.ares .card-label { color: var(--ares-dim); }

    .card h3 { font-size: 0.95rem; color: var(--cyan); margin-bottom: 1rem; letter-spacing: 0.05em; }
    .card.ares h3 { color: var(--ares); }

    .endpoint-list { list-style: none; }
    .endpoint-list li { margin-bottom: 0.5rem; font-size: 0.8rem; display: flex; align-items: center; gap: 0.5rem; }
    .endpoint-list li::before { content: '//'; color: var(--cyan-dim); font-size: 0.7rem; }
    .endpoint-list a { color: var(--text); text-decoration: none; transition: color 0.2s; }
    .endpoint-list a:hover { color: var(--cyan); text-shadow: var(--glow-cyan); }

    code {
      background: rgba(0,240,255,0.08); border: 1px solid var(--border);
      padding: 0.15rem 0.5rem; font-family: inherit; font-size: 0.8rem;
      color: var(--cyan);
    }

    .cmd-block {
      background: var(--bg-void); border: 1px solid var(--border);
      padding: 1rem; margin-top: 0.5rem; font-size: 0.78rem;
      color: var(--text-dim); line-height: 1.7;
    }
    .cmd-block .prompt { color: var(--ares); }
    .cmd-block .cmd    { color: var(--cyan); }

    /* ── Footer ── */
    footer {
      margin-top: 3rem; text-align: center;
      font-size: 0.65rem; letter-spacing: 0.2em; text-transform: uppercase;
      color: var(--text-dim);
    }
    footer span { color: var(--ares); }
  </style>
</head>
<body>
  <div class="wrapper">
    <header>
      <div class="logo-bar">
        <h1>DATABRICKS<span>-</span>AGENT</h1>
        <p class="tagline">AI-powered analytics engineering // Tron Ares Interface</p>
        <p class="author-line">
          Santosh Kanthety &nbsp;·&nbsp;
          <a href="https://github.com/santoshkanthety/databricks-agent" target="_blank">github</a>
        </p>
      </div>
    </header>

    <div class="status-bar">
      <div class="status-dot"></div>
      <span>SYSTEM STATUS: ONLINE</span>
      <span style="margin-left:auto; color: var(--text-dim)">
        Run <code>databricks-agent doctor</code> for full diagnostics
      </span>
    </div>

    <div class="grid">

      <div class="card">
        <div class="card-label">// quickstart</div>
        <h3>Connect to Workspace</h3>
        <div class="cmd-block">
          <div><span class="prompt">$</span> <span class="cmd">databricks-agent connect setup</span></div>
          <div><span class="prompt">$</span> <span class="cmd">databricks-agent skills install</span></div>
          <div><span class="prompt">$</span> <span class="cmd">databricks-agent doctor</span></div>
        </div>
      </div>

      <div class="card ares">
        <div class="card-label">// api endpoints</div>
        <h3>REST Interface</h3>
        <ul class="endpoint-list">
          <li><a href="/api/warehouses">api/warehouses</a></li>
          <li><a href="/api/jobs">api/jobs</a></li>
          <li><a href="/api/pipelines">api/pipelines</a></li>
          <li><a href="/api/clusters">api/clusters</a></li>
          <li><a href="/health">health</a></li>
          <li><a href="/docs">docs — OpenAPI</a></li>
        </ul>
      </div>

      <div class="card">
        <div class="card-label">// skills installed</div>
        <h3>17 Active Skills</h3>
        <div class="cmd-block" style="font-size:0.72rem; line-height:1.9">
          <div>✦ medallion-architecture</div>
          <div>✦ delta-modeling</div>
          <div>✦ spark-sql-mastery</div>
          <div>✦ dlt-pipelines</div>
          <div>✦ security-governance</div>
          <div>✦ data-governance-traceability</div>
          <div>✦ cyber-security</div>
          <div style="color:var(--text-dim)">+ 10 more</div>
        </div>
      </div>

      <div class="card ares">
        <div class="card-label">// commands</div>
        <h3>CLI Reference</h3>
        <div class="cmd-block">
          <div><span class="prompt">$</span> <span class="cmd">dba sql query --sql "..."</span></div>
          <div><span class="prompt">$</span> <span class="cmd">dba jobs run --name etl</span></div>
          <div><span class="prompt">$</span> <span class="cmd">dba catalog audit prod</span></div>
          <div><span class="prompt">$</span> <span class="cmd">dba pipelines start --name p1</span></div>
        </div>
      </div>

    </div>

    <footer>
      <p>DATABRICKS-AGENT // v0.1.0 // MIT LICENSE</p>
      <p style="margin-top:0.4rem">Built by <span>SANTOSH KANTHETY</span> — Tron Ares staging interface</p>
    </footer>
  </div>
</body>
</html>
"""


@app.get("/api/warehouses")
async def api_warehouses():
    from databricks_agent.sql import list_warehouses
    return list_warehouses()


@app.get("/api/jobs")
async def api_jobs():
    from databricks_agent.jobs import list_jobs
    return list_jobs()


@app.get("/api/pipelines")
async def api_pipelines():
    from databricks_agent.pipelines import list_pipelines
    return list_pipelines()


@app.get("/api/clusters")
async def api_clusters():
    from databricks_agent.clusters import list_clusters
    return list_clusters()


@app.get("/health")
async def health():
    from databricks_agent.connect import test_connection
    ok, msg = test_connection()
    return {"status": "ok" if ok else "error", "message": msg}
