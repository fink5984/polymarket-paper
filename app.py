import asyncio, json, math, os, sqlite3, statistics, time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

import httpx
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

DATA_DIR = Path(os.getenv("DATA_DIR", "."))
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB = DATA_DIR / "paper.db"
INTERVAL = float(os.getenv("SAMPLE_SECONDS", "2"))
EDGE_MIN = float(os.getenv("EDGE_MIN", "0.08"))
STAKE = float(os.getenv("PAPER_STAKE", "10"))
SLIPPAGE = float(os.getenv("SLIPPAGE", "0.01"))
assert os.getenv("PAPER_ONLY", "true").lower() == "true", "This service is paper-only"

state = {"started": time.time(), "status": "starting", "market": None, "btc": None, "last_error": None}

def conn():
    c = sqlite3.connect(DB)
    c.row_factory = sqlite3.Row
    return c

def init_db():
    with conn() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS samples(ts REAL PRIMARY KEY, market_slug TEXT, seconds_left REAL,
          btc REAL, open_btc REAL, p_up REAL, up_bid REAL, up_ask REAL, down_bid REAL, down_ask REAL);
        CREATE TABLE IF NOT EXISTS trades(id INTEGER PRIMARY KEY, ts REAL, market_slug TEXT, side TEXT,
          ask REAL, fill_price REAL, model_p REAL, edge REAL, stake REAL, status TEXT DEFAULT 'open',
          result INTEGER, pnl REAL);
        CREATE UNIQUE INDEX IF NOT EXISTS one_trade_side ON trades(market_slug, side);
        """)

async def get_json(client, url, params=None):
    r = await client.get(url, params=params, timeout=10)
    r.raise_for_status()
    return r.json()

async def discover(client, epoch):
    for e in (epoch, epoch-300, epoch+300):
        slug = f"btc-updown-5m-{e}"
        try:
            data = await get_json(client, f"https://gamma-api.polymarket.com/events/slug/{slug}")
            if data and data.get("markets"):
                m = data["markets"][0]
                tokens = json.loads(m.get("clobTokenIds", "[]"))
                outcomes = json.loads(m.get("outcomes", '["Up","Down"]'))
                if len(tokens) == 2:
                    return {"slug": slug, "epoch": e, "end": e+300, "condition": m.get("conditionId"),
                            "tokens": dict(zip(outcomes, tokens)), "closed": m.get("closed", False)}
        except Exception:
            pass
    return None

async def btc_price(client):
    try:
        d = await get_json(client, "https://api.binance.com/api/v3/ticker/price", {"symbol":"BTCUSDT"})
        return float(d["price"])
    except Exception:
        d = await get_json(client, "https://api.coinbase.com/v2/prices/BTC-USD/spot")
        return float(d["data"]["amount"])

async def book(client, token):
    d = await get_json(client, "https://clob.polymarket.com/book", {"token_id": token})
    bids = [float(x["price"]) for x in d.get("bids", [])]
    asks = [float(x["price"]) for x in d.get("asks", [])]
    return (max(bids) if bids else None, min(asks) if asks else None)

def probability(prices, current, opened, seconds_left):
    if len(prices) < 8 or not opened: return 0.5
    rets = [math.log(prices[i]/prices[i-1]) for i in range(1, len(prices))]
    sigma = max(statistics.pstdev(rets), 0.00002) * math.sqrt(max(seconds_left/INTERVAL, 1))
    z = math.log(current/opened) / sigma
    return min(.995, max(.005, .5 * (1 + math.erf(z/math.sqrt(2)))))

async def settle_old(client):
    with conn() as c:
        slugs = [r[0] for r in c.execute("SELECT DISTINCT market_slug FROM trades WHERE status='open'")]
    for slug in slugs:
        try:
            d = await get_json(client, f"https://gamma-api.polymarket.com/events/slug/{slug}")
            if not d.get("closed"): continue
            m = d["markets"][0]; outcomes=json.loads(m["outcomes"]); ps=list(map(float,json.loads(m["outcomePrices"])))
            winner = outcomes[ps.index(max(ps))].lower()
            with conn() as c:
                rows=c.execute("SELECT * FROM trades WHERE market_slug=? AND status='open'",(slug,)).fetchall()
                for r in rows:
                    won = r["side"].lower() == winner
                    pnl = r["stake"] * ((1/r["fill_price"])-1) if won else -r["stake"]
                    c.execute("UPDATE trades SET status='settled',result=?,pnl=? WHERE id=?",(int(won),pnl,r["id"]))
        except Exception: pass

async def worker():
    init_db(); prices=[]; current_market=None; open_btc=None; last_settle=0
    async with httpx.AsyncClient(headers={"User-Agent":"paper-research/1.0"}) as client:
      while True:
        try:
            now=time.time(); epoch=int(now//300)*300
            if not current_market or current_market["end"] <= now:
                current_market=await discover(client,epoch); prices=[]; open_btc=None
            px=await btc_price(client); prices=(prices+[px])[-150:]
            if open_btc is None: open_btc=px
            if current_market:
                token_map={k.lower():v for k,v in current_market["tokens"].items()}
                up_token=token_map.get("up") or list(token_map.values())[0]
                down_token=token_map.get("down") or list(token_map.values())[1]
                (ub,ua),(db,da)=await asyncio.gather(book(client,up_token),book(client,down_token))
                left=max(0,current_market["end"]-now); pup=probability(prices,px,open_btc,left)
                with conn() as c:
                    c.execute("INSERT OR REPLACE INTO samples VALUES(?,?,?,?,?,?,?,?,?,?)",
                              (now,current_market["slug"],left,px,open_btc,pup,ub,ua,db,da))
                    for side,ask,model in (("Up",ua,pup),("Down",da,1-pup)):
                        if ask and 0.03 < ask < .97 and model-ask >= EDGE_MIN:
                            fill=min(.99,ask+SLIPPAGE); edge=model-fill
                            c.execute("INSERT OR IGNORE INTO trades(ts,market_slug,side,ask,fill_price,model_p,edge,stake) VALUES(?,?,?,?,?,?,?,?)",
                                      (now,current_market["slug"],side,ask,fill,model,edge,STAKE))
                state.update(status="collecting",market=current_market["slug"],btc=px,last_error=None)
            else: state.update(status="waiting_for_market",btc=px)
            if now-last_settle>60: await settle_old(client); last_settle=now
        except Exception as e:
            state.update(status="error",last_error=str(e)[:300])
        await asyncio.sleep(INTERVAL)

def stats():
    with conn() as c:
        s=c.execute("SELECT COUNT(*) n, COUNT(DISTINCT market_slug) markets, MIN(ts) first, MAX(ts) last FROM samples").fetchone()
        t=c.execute("SELECT COUNT(*) n, SUM(status='settled') settled, SUM(COALESCE(result,0)) wins, ROUND(SUM(COALESCE(pnl,0)),2) pnl FROM trades").fetchone()
        recent=[dict(r) for r in c.execute("SELECT * FROM trades ORDER BY ts DESC LIMIT 20")]
    return {"samples":dict(s),"trades":dict(t),"recent":recent,"runtime":state}

@asynccontextmanager
async def lifespan(app):
    task=asyncio.create_task(worker()); yield; task.cancel()

app=FastAPI(title="Polymarket BTC 5m Paper Lab",lifespan=lifespan)
@app.get("/health")
def health(): return {"ok":True,"paper_only":True,"status":state["status"]}
@app.get("/api/stats")
def api_stats(): return stats()
@app.get("/",response_class=HTMLResponse)
def dashboard():
    return HTMLResponse('''<!doctype html><html dir="rtl" lang="he"><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>BTC 5m Paper Lab</title><style>body{font-family:system-ui;background:#07111f;color:#e8f1ff;margin:0;padding:24px}.wrap{max-width:1000px;margin:auto}.badge{background:#164e3c;color:#9ff7cf;padding:8px 14px;border-radius:30px}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:14px;margin:24px 0}.card{background:#101f33;border:1px solid #263b55;border-radius:16px;padding:18px}.v{font-size:28px;font-weight:700;margin-top:8px}table{width:100%;border-collapse:collapse;background:#101f33;border-radius:14px;overflow:hidden}th,td{padding:10px;border-bottom:1px solid #263b55;text-align:right}.up{color:#67e8a5}.down{color:#ff8b8b}</style><div class="wrap"><span class="badge">מחקר בלבד · אין מסחר אמיתי</span><h1>Polymarket BTC — מעבדת 5 דקות</h1><div id="app">טוען…</div></div><script>async function load(){let d=await(await fetch('/api/stats')).json(),s=d.samples,t=d.trades,r=d.runtime;document.getElementById('app').innerHTML=`<div class="grid"><div class="card">סטטוס<div class="v">${r.status}</div></div><div class="card">מחיר BTC<div class="v">${r.btc?'$'+r.btc.toLocaleString():'—'}</div></div><div class="card">חלונות שנאספו<div class="v">${s.markets||0}</div></div><div class="card">דגימות<div class="v">${s.n||0}</div></div><div class="card">עסקאות מדומות<div class="v">${t.n||0}</div></div><div class="card">רווח מדומה<div class="v ${t.pnl>=0?'up':'down'}">$${t.pnl||0}</div></div></div><h2>עסקאות אחרונות</h2><table><tr><th>שוק</th><th>צד</th><th>מחיר</th><th>Edge</th><th>מצב</th><th>P&L</th></tr>${d.recent.map(x=>`<tr><td>${x.market_slug}</td><td>${x.side}</td><td>${x.fill_price.toFixed(3)}</td><td>${(x.edge*100).toFixed(1)}%</td><td>${x.status}</td><td>${x.pnl==null?'—':'$'+x.pnl.toFixed(2)}</td></tr>`).join('')}</table>${r.last_error?'<p class="down">'+r.last_error+'</p>':''}`};load();setInterval(load,5000)</script></html>''')
