"""Live, dependency-free browser dashboard for the job scheduler."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json

DASHBOARD_HTML = r'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Distributed Job Scheduler | Live Dashboard</title>
<style>
:root{color-scheme:dark;--bg:#0b1220;--panel:#111c2f;--panel2:#16243a;--line:#263750;--muted:#9aaec7;--text:#edf4ff;--cyan:#67d8e8;--green:#56d6a2;--amber:#f6c66a;--red:#ff7d88;--blue:#8aaaff}
*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 10% 0%,#172b46 0,#0b1220 42%);color:var(--text);font:14px/1.5 Inter,Segoe UI,Arial,sans-serif}
.wrap{max-width:1500px;margin:auto;padding:28px 30px 48px}.top{display:flex;justify-content:space-between;align-items:center;gap:20px;margin-bottom:24px}.eyebrow{color:var(--cyan);font-size:11px;font-weight:800;letter-spacing:2px;text-transform:uppercase}.top h1{font-size:27px;letter-spacing:-.6px;margin:5px 0 2px}.sub{color:var(--muted)}.live{display:flex;align-items:center;gap:8px;border:1px solid #245b4b;background:#102d2a;color:#8ef0c5;border-radius:30px;padding:8px 13px;font-size:12px;font-weight:700;white-space:nowrap}.dot{width:8px;height:8px;background:var(--green);border-radius:50%;box-shadow:0 0 12px var(--green)}
.serverbar{display:flex;flex-wrap:wrap;gap:10px;margin-bottom:17px}.chip{background:rgba(22,36,58,.8);border:1px solid var(--line);padding:8px 12px;border-radius:9px;color:#c5d3e6;font-size:12px}.chip b{color:#fff;margin-left:5px}
.cards{display:grid;grid-template-columns:repeat(7,minmax(120px,1fr));gap:13px;margin-bottom:20px}.card,.panel{background:linear-gradient(145deg,rgba(22,36,58,.97),rgba(15,26,43,.97));border:1px solid var(--line);border-radius:14px;box-shadow:0 12px 30px rgba(0,0,0,.15)}.card{padding:15px 16px}.card .label{color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:1px;font-weight:700}.card .num{font-size:29px;font-weight:750;letter-spacing:-1px;margin:3px 0}.card .hint{font-size:11px;color:#8fa4be}.num.cyan{color:var(--cyan)}.num.green{color:var(--green)}.num.amber{color:var(--amber)}
.grid2{display:grid;grid-template-columns:1.1fr .9fr;gap:16px;margin-bottom:16px}.panel{padding:17px 18px;min-width:0}.panelhead{display:flex;justify-content:space-between;align-items:center;gap:12px;margin-bottom:13px}.panel h2{font-size:15px;margin:0;letter-spacing:.1px}.tag{font-size:10px;color:var(--cyan);border:1px solid #2b6070;background:#102c3b;padding:4px 8px;border-radius:20px;white-space:nowrap}.policies{display:grid;grid-template-columns:1fr 1fr;gap:10px}.policy{background:var(--panel2);border:1px solid var(--line);border-radius:10px;padding:12px}.policy .k{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:1px}.policy .v{font-size:13px;font-weight:700;margin-top:4px}.note{margin-top:12px;color:var(--muted);font-size:12px}
.tablewrap{overflow:auto;border:1px solid var(--line);border-radius:10px}table{width:100%;border-collapse:collapse;min-width:650px}th{background:#0e192a;color:#91a8c5;font-size:10px;letter-spacing:.8px;text-transform:uppercase;text-align:left;padding:11px 12px;position:sticky;top:0}td{padding:11px 12px;border-top:1px solid rgba(38,55,80,.7);font-size:12px;white-space:nowrap}tbody tr:hover{background:rgba(103,216,232,.045)}.muted{color:var(--muted)}.mono{font-family:Consolas,monospace}.status{display:inline-flex;align-items:center;padding:3px 8px;border-radius:20px;font-size:10px;font-weight:800;letter-spacing:.3px}.status.ALIVE,.status.COMPLETED{color:#8ef0c5;background:#12372f}.status.QUEUED{color:#f6d28a;background:#3b2d15}.status.RUNNING{color:#8edfff;background:#123546}.status.DEAD,.status.FAILED{color:#ff9da6;background:#421f2a}.wide{margin-bottom:16px}.tools{display:flex;gap:8px;align-items:center}.tools input,.tools select{background:#0d192a;border:1px solid var(--line);color:var(--text);padding:8px 10px;border-radius:8px;font:inherit;font-size:12px}.tools input{width:190px}.empty{text-align:center;color:var(--muted);padding:24px}.foot{color:#7087a4;font-size:11px;text-align:right;margin-top:10px}
@media(max-width:1050px){.cards{grid-template-columns:repeat(3,1fr)}.grid2{grid-template-columns:1fr}}@media(max-width:620px){.wrap{padding:18px 12px}.top{align-items:flex-start;flex-direction:column}.top h1{font-size:22px}.cards{grid-template-columns:repeat(2,1fr)}.policies{grid-template-columns:1fr}.tools input{width:135px}}
</style>
</head>
<body><div class="wrap">
<header class="top"><div><div class="eyebrow">Distributed Computing · Demo 2</div><h1>Job Allocation & Worker Monitor</h1><div class="sub">Live view of clients, server queue, worker assignment and job results</div></div><div class="live"><span class="dot"></span> LIVE MONITORING <span id="lastUpdated" class="muted">· connecting</span></div></header>
<div class="serverbar"><div class="chip">Server <b id="serverStatus">CONNECTING</b></div><div class="chip">Client port <b id="clientPort">—</b></div><div class="chip">Uptime <b id="uptime">—</b></div><div class="chip">Client connections <b id="connections">—</b></div><div class="chip">Known clients <b id="knownClients">—</b></div><div class="chip">Worker failures <b id="workerFailures">0</b></div></div>
<section class="cards">
<div class="card"><div class="label">Total jobs</div><div class="num cyan" id="submitted">0</div><div class="hint">All submitted jobs</div></div>
<div class="card"><div class="label">In queue</div><div class="num amber" id="queued">0</div><div class="hint">Waiting for a worker</div></div>
<div class="card"><div class="label">Running</div><div class="num cyan" id="running">0</div><div class="hint">Currently assigned</div></div>
<div class="card"><div class="label">Completed</div><div class="num green" id="completed">0</div><div class="hint">Successful results</div></div>
<div class="card"><div class="label">Failed jobs</div><div class="num" id="failed">0</div><div class="hint">Execution errors</div></div>
<div class="card"><div class="label">Active workers</div><div class="num green" id="aliveWorkers">0</div><div class="hint" id="workerHint">0 registered</div></div>
<div class="card"><div class="label">Avg. job time</div><div class="num" id="avgDuration">—</div><div class="hint">Seconds per completed job</div></div>
</section>
<section class="grid2">
<div class="panel"><div class="panelhead"><h2>Scheduling policy</h2><span class="tag">FAIR CLIENT ACCESS</span></div><div class="policies"><div class="policy"><div class="k">Client priority</div><div class="v" id="clientPolicy">Round-robin</div></div><div class="policy"><div class="k">Worker selection</div><div class="v" id="workerPolicy">Least-loaded worker</div></div></div><div class="note">Every active client gets one job turn before the queue gives that client another turn. No client is permanently preferred. Among available workers, the server chooses the worker with the fewest jobs in progress (then uses reported load as a tie-breaker).</div></div>
<div class="panel"><div class="panelhead"><h2>Worker fleet</h2><span class="tag">HEARTBEAT STATUS</span></div><div class="tablewrap"><table><thead><tr><th>Worker</th><th>Status</th><th>Assigned now</th><th>Load</th><th>Completed</th><th>Failed</th><th>Heartbeat age</th></tr></thead><tbody id="workerRows"><tr><td colspan="7" class="empty">Waiting for workers…</td></tr></tbody></table></div></div>
</section>
<section class="panel wide"><div class="panelhead"><h2>Client activity</h2><span class="tag">CLIENT-WISE JOB STATISTICS</span></div><div class="tablewrap"><table><thead><tr><th>Client ID</th><th>Connections</th><th>Total jobs</th><th>Queued</th><th>Running</th><th>Completed</th><th>Failed</th></tr></thead><tbody id="clientRows"><tr><td colspan="7" class="empty">No client activity yet</td></tr></tbody></table></div></section>
<section class="panel wide"><div class="panelhead"><div><h2>Complete job allocation table</h2><div class="muted" style="font-size:11px;margin-top:3px">See exactly which client submitted each job and which worker handled it.</div></div><div class="tools"><input id="jobSearch" placeholder="Search job / client / worker…"><select id="statusFilter"><option value="ALL">All statuses</option><option>QUEUED</option><option>RUNNING</option><option>COMPLETED</option><option>FAILED</option></select></div></div><div class="tablewrap"><table><thead><tr><th>Server Job ID</th><th>Client ID</th><th>Client Job ID</th><th>Job title</th><th>Worker assigned</th><th>Status</th><th>Submitted</th><th>Started</th><th>Completed</th><th>Duration</th><th>Attempts</th><th>Result</th></tr></thead><tbody id="jobRows"><tr><td colspan="12" class="empty">No jobs submitted yet</td></tr></tbody></table></div><div class="foot" id="jobCount">Showing latest jobs</div></section>
</div>
<script>
const $=id=>document.getElementById(id);
const esc=value=>String(value??'—').replace(/[&<>"']/g,ch=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
const stamp=value=>value?new Date(value*1000).toLocaleTimeString([], {hour:'2-digit',minute:'2-digit',second:'2-digit'}):'—';
const duration=value=>value===null||value===undefined?'—':Number(value).toFixed(2)+' s';
const badge=status=>`<span class="status ${esc(status)}">${esc(status)}</span>`;
function render(data){
 const s=data.server,st=data.stats,w=data.workers;
 $('serverStatus').textContent=s.status;$('clientPort').textContent=s.client_port;$('uptime').textContent=(s.uptime_seconds/60<1?Math.round(s.uptime_seconds)+' sec':(s.uptime_seconds/60).toFixed(1)+' min');
 $('connections').textContent=s.total_client_connections;$('knownClients').textContent=s.known_clients;$('workerFailures').textContent=w.total_worker_failures;
 $('submitted').textContent=st.submitted;$('queued').textContent=st.queued;$('running').textContent=st.running;$('completed').textContent=st.completed;$('failed').textContent=st.failed;
 $('aliveWorkers').textContent=w.alive;$('workerHint').textContent=`${w.total} registered · ${w.dead} unavailable`;$('avgDuration').textContent=st.average_duration_seconds?st.average_duration_seconds.toFixed(2)+' s':'—';
 $('clientPolicy').textContent=data.scheduling.client_policy;$('workerPolicy').textContent=data.scheduling.worker_policy;
 $('workerRows').innerHTML=w.workers.length?w.workers.map(x=>`<tr><td class="mono"><b>${esc(x.worker_id)}</b></td><td>${badge(x.status)}</td><td>${x.in_flight}</td><td>${x.reported_load}</td><td>${x.completed_jobs}</td><td>${x.failed_jobs}</td><td>${x.heartbeat_age_seconds.toFixed(1)} s ago</td></tr>`).join(''):'<tr><td colspan="7" class="empty">No workers registered. Start worker.py on each worker machine.</td></tr>';
 $('clientRows').innerHTML=data.clients.length?data.clients.map(x=>`<tr><td class="mono"><b>${esc(x.client_id)}</b></td><td>${x.connections}</td><td>${x.total}</td><td>${x.queued}</td><td>${x.running}</td><td>${x.completed}</td><td>${x.failed}</td></tr>`).join(''):'<tr><td colspan="7" class="empty">No client activity yet</td></tr>';
 const q=$('jobSearch').value.toLowerCase(),f=$('statusFilter').value;
 const jobs=data.jobs.filter(x=>(f==='ALL'||x.status===f)&&[x.job_id,x.client_id,x.client_job_id,x.worker_id,x.job_title,x.task].join(' ').toLowerCase().includes(q));
 $('jobRows').innerHTML=jobs.length?jobs.map(x=>`<tr><td class="mono"><b>${esc(x.job_id)}</b></td><td>${esc(x.client_id)}</td><td class="mono">${esc(x.client_job_id)}</td><td>${esc(x.job_title)}<div class="muted">${esc(x.task)}</div></td><td class="mono">${esc(x.worker_id)}</td><td>${badge(x.status)}</td><td>${stamp(x.submitted_at)}</td><td>${stamp(x.started_at)}</td><td>${stamp(x.completed_at)}</td><td>${duration(x.duration_seconds)}</td><td>${x.attempts}</td><td title="${esc(x.result)}">${esc(String(x.result).slice(0,55))}</td></tr>`).join(''):'<tr><td colspan="12" class="empty">No jobs match the selected filter</td></tr>';
 $('jobCount').textContent=`Showing ${jobs.length} of ${data.stats.submitted} jobs · ${data.stats.failed} failed · queue depth ${st.pending_in_queue}`;
 $('lastUpdated').textContent='· updated '+new Date(data.updated_at*1000).toLocaleTimeString();
}
async function refresh(){try{const response=await fetch('/api/status',{cache:'no-store'});if(!response.ok)throw new Error('HTTP '+response.status);render(await response.json())}catch(e){$('serverStatus').textContent='OFFLINE';$('lastUpdated').textContent='· reconnecting'}}
$('jobSearch').addEventListener('input',refresh);$('statusFilter').addEventListener('change',refresh);refresh();setInterval(refresh,2000);
</script></body></html>'''


def start_dashboard(snapshot_provider, host="0.0.0.0", port=8000):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path in ("/", "/index.html"):
                body = DASHBOARD_HTML.encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(body)
            elif self.path == "/api/status":
                body = json.dumps(snapshot_provider(), default=str).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_error(404)

        def log_message(self, fmt, *args):
            return

    httpd = ThreadingHTTPServer((host, port), Handler)
    print("[DASHBOARD] Open http://127.0.0.1:%d in your browser" % port, flush=True)
    httpd.serve_forever()
