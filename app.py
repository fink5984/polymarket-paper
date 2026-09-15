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
BANKROLL = float(os.getenv("PAPER_BANKROLL", "100"))
RESET_TOKEN = os.getenv("RESET_TOKEN", "")
assert os.getenv("PAPER_ONLY", "true").lower() == "true", "This service is paper-only"

state = {"started": time.time(), "status": "starting", "market": None, "btc": None, "last_error": None}

def conn():
    c = sqlite3.connect(DB)
    c.row_factory = sqlite3.Row
    return c

def init_db():
    with conn() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE IF NOT EXISTS samples(ts REAL PRIMARY KEY, market_slug TEXT, seconds_left REAL,
          btc REAL, open_btc REAL, p_up REAL, up_bid REAL, up_ask REAL, down_bid REAL, down_ask REAL);
        CREATE TABLE IF NOT EXISTS trades(id INTEGER PRIMARY KEY, ts REAL, market_slug TEXT, side TEXT,
          ask REAL, fill_price REAL, model_p REAL, edge REAL, stake REAL, status TEXT DEFAULT 'open',
          result INTEGER, pnl REAL);
        CREATE UNIQUE INDEX IF NOT EXISTS one_trade_side ON trades(market_slug, side);
        """)
        existing={r[1] for r in c.execute("PRAGMA table_info(trades)")}
        for name,sql in {
            "managed_status":"ALTER TABLE trades ADD COLUMN managed_status TEXT",
            "exit_ts":"ALTER TABLE trades ADD COLUMN exit_ts REAL",
            "exit_price":"ALTER TABLE trades ADD COLUMN exit_price REAL",
            "exit_reason":"ALTER TABLE trades ADD COLUMN exit_reason TEXT",
            "managed_pnl":"ALTER TABLE trades ADD COLUMN managed_pnl REAL"}.items():
            if name not in existing: c.execute(sql)
        previous=c.execute("SELECT value FROM meta WHERE key='reset_token'").fetchone()
        if RESET_TOKEN and (not previous or previous[0] != RESET_TOKEN):
            c.execute("DELETE FROM samples")
            c.execute("DELETE FROM trades")
            c.execute("DELETE FROM sqlite_sequence WHERE name='trades'")
            c.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('reset_token',?)",(RESET_TOKEN,))
            print(f"RESET COMPLETE token={RESET_TOKEN}: samples=0 trades=0", flush=True)

def wallet_available(c):
    realized=c.execute("SELECT COALESCE(SUM(managed_pnl),0) FROM trades WHERE managed_pnl IS NOT NULL").fetchone()[0]
    locked=c.execute("SELECT COALESCE(SUM(stake),0) FROM trades WHERE managed_status='open'").fetchone()[0]
    return BANKROLL + realized - locked

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
                    c.execute("UPDATE trades SET managed_status='settled',managed_pnl=?,exit_reason='resolution',exit_ts=? WHERE id=? AND managed_status='open'",(pnl,time.time(),r["id"]))
        except Exception: pass

def manage_exits(c, market_slug, now, left, pup, up_bid, down_bid):
    rows=c.execute("SELECT * FROM trades WHERE market_slug=? AND managed_status='open'",(market_slug,)).fetchall()
    for r in rows:
        bid=up_bid if r["side"].lower()=="up" else down_bid
        side_p=pup if r["side"].lower()=="up" else 1-pup
        if bid is None: continue
        roi=bid/r["fill_price"]-1; age=now-r["ts"]; reason=None
        if roi >= .35: reason="take_profit_35%"
        elif roi <= -.30: reason="stop_loss_30%"
        elif age >= 10 and side_p < .35: reason="model_reversal"
        elif left <= 15 and bid < .97: reason="time_exit_15s"
        if reason:
            exit_price=max(.001,bid-SLIPPAGE)
            managed_pnl=r["stake"]*(exit_price/r["fill_price"]-1)
            c.execute("UPDATE trades SET managed_status='exited',exit_ts=?,exit_price=?,exit_reason=?,managed_pnl=? WHERE id=?",(now,exit_price,reason,managed_pnl,r["id"]))

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
                    manage_exits(c,current_market["slug"],now,left,pup,ub,db)
                    for side,ask,model in (("Up",ua,pup),("Down",da,1-pup)):
                        if ask and 0.03 < ask < .97 and model-ask >= EDGE_MIN and wallet_available(c) >= STAKE:
                            fill=min(.99,ask+SLIPPAGE); edge=model-fill
                            c.execute("INSERT OR IGNORE INTO trades(ts,market_slug,side,ask,fill_price,model_p,edge,stake,managed_status) VALUES(?,?,?,?,?,?,?,?,?)",
                                      (now,current_market["slug"],side,ask,fill,model,edge,STAKE,"open"))
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
        current=c.execute("SELECT * FROM samples ORDER BY ts DESC LIMIT 1").fetchone()
        series=[dict(r) for r in c.execute("SELECT ts,seconds_left,btc,open_btc,p_up,up_bid,up_ask,down_bid,down_ask FROM samples WHERE market_slug=(SELECT market_slug FROM samples ORDER BY ts DESC LIMIT 1) ORDER BY ts DESC LIMIT 180")]
        settled=[dict(r) for r in c.execute("SELECT * FROM trades WHERE status='settled' ORDER BY ts")]
        managed=[dict(r) for r in c.execute("SELECT * FROM trades WHERE managed_pnl IS NOT NULL ORDER BY ts")]
    mark_total=0.0
    with conn() as c:
        for trade in recent:
            q=c.execute("SELECT up_bid,down_bid FROM samples WHERE market_slug=? ORDER BY ts DESC LIMIT 1",(trade["market_slug"],)).fetchone()
            mark=(q["up_bid"] if trade["side"].lower()=="up" else q["down_bid"]) if q else None
            trade["mark_price"]=mark
            trade["unrealized_pnl"]=(trade["stake"]*(mark/trade["fill_price"]-1)) if trade["status"]=="open" and mark is not None else None
            trade["display_pnl"]=trade["pnl"] if trade["status"]=="settled" else trade["unrealized_pnl"]
            try:
                ended = int(trade["market_slug"].rsplit("-",1)[1]) + 300 <= time.time()
            except (ValueError, IndexError):
                ended = False
            trade["display_status"] = "נסגר" if trade["status"]=="settled" else ("ממתין להכרעה" if ended else "פתוח")
            mark_total += trade["display_pnl"] or 0
    td=dict(t); td["win_rate"] = round(100*(td["wins"] or 0)/(td["settled"] or 1),1); td["mark_pnl"]=round(mark_total,2)
    def group(rows, keyfn, value="pnl"):
        out={}
        for x in rows:
            k=keyfn(x); z=out.setdefault(k,{"trades":0,"wins":0,"pnl":0.0})
            amount=x[value] or 0
            z["trades"]+=1; z["wins"]+=int(amount>0); z["pnl"]+=amount
        for z in out.values(): z["pnl"]=round(z["pnl"],2); z["win_rate"]=round(100*z["wins"]/z["trades"],1)
        return out
    pnls=sorted([x["pnl"] for x in settled if x["pnl"] is not None])
    gross_profit=sum(x for x in pnls if x>0); gross_loss=-sum(x for x in pnls if x<0)
    winners=sorted((x for x in pnls if x>0),reverse=True)
    analysis={"gross_profit":round(gross_profit,2),"gross_loss":round(gross_loss,2),
      "profit_factor":round(gross_profit/gross_loss,2) if gross_loss else None,
      "avg_pnl":round(sum(pnls)/len(pnls),2) if pnls else 0,
      "median_pnl":round(statistics.median(pnls),2) if pnls else 0,
      "best_trade":round(max(pnls),2) if pnls else 0,"worst_trade":round(min(pnls),2) if pnls else 0,
      "top5_profit_share":round(100*sum(winners[:5])/gross_profit,1) if gross_profit else 0,
      "by_side":group(settled,lambda x:x["side"]),
      "by_price":group(settled,lambda x:"0-.20" if x["fill_price"]<.2 else ".20-.40" if x["fill_price"]<.4 else ".40-.60" if x["fill_price"]<.6 else ".60-.80" if x["fill_price"]<.8 else ".80-1"),
      "by_edge":group(settled,lambda x:"<10%" if x["edge"]<.1 else "10-15%" if x["edge"]<.15 else "15-20%" if x["edge"]<.2 else "20%+")}
    managed_pnls=[x["managed_pnl"] for x in managed]
    analysis["managed"]={"trades":len(managed),"pnl":round(sum(managed_pnls),2),
      "avg_pnl":round(sum(managed_pnls)/len(managed_pnls),2) if managed_pnls else 0,
      "wins":sum(1 for x in managed_pnls if x>0),
      "by_reason":group(managed,lambda x:x["exit_reason"] or "unknown","managed_pnl")}
    analysis["managed"]["win_rate"]=round(100*analysis["managed"]["wins"]/len(managed_pnls),1) if managed_pnls else 0
    with conn() as c:
        realized=c.execute("SELECT COALESCE(SUM(managed_pnl),0) FROM trades WHERE managed_pnl IS NOT NULL").fetchone()[0]
        open_rows=c.execute("SELECT side,fill_price,stake,market_slug FROM trades WHERE managed_status='open'").fetchall()
        unrealized=0.0
        for x in open_rows:
            q=c.execute("SELECT up_bid,down_bid FROM samples WHERE market_slug=? ORDER BY ts DESC LIMIT 1",(x["market_slug"],)).fetchone()
            mark=(q["up_bid"] if x["side"].lower()=="up" else q["down_bid"]) if q else None
            if mark is not None: unrealized += x["stake"]*(mark/x["fill_price"]-1)
        locked=sum(x["stake"] for x in open_rows)
    wallet={"initial":BANKROLL,"balance":round(BANKROLL+realized,2),"available":round(BANKROLL+realized-locked,2),
            "locked":round(locked,2),"unrealized":round(unrealized,2),"equity":round(BANKROLL+realized+unrealized,2)}
    return {"samples":dict(s),"trades":td,"recent":recent,"current":dict(current) if current else None,
            "series":list(reversed(series)),"analysis":analysis,"wallet":wallet,"runtime":state,"server_time":time.time()}

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
    return HTMLResponse('''<!doctype html><html dir="rtl" lang="he"><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>BTC 5m Live Paper Lab</title><style>
*{box-sizing:border-box}body{font-family:system-ui;background:#06101e;color:#e8f1ff;margin:0;padding:18px}.wrap{max-width:1200px;margin:auto}.top{display:flex;justify-content:space-between;align-items:center;gap:10px;flex-wrap:wrap}.badge{background:#164e3c;color:#9ff7cf;padding:8px 14px;border-radius:30px}.live{color:#67e8a5}.dot{display:inline-block;width:9px;height:9px;border-radius:50%;background:#45e69a;box-shadow:0 0 12px #45e69a;margin-left:7px}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(155px,1fr));gap:12px;margin:18px 0}.card,.panel{background:#0e1e32;border:1px solid #263b55;border-radius:16px;padding:16px}.label{color:#9db0c9;font-size:14px}.v{font-size:25px;font-weight:750;margin-top:6px;direction:ltr;text-align:right}.mini{font-size:13px;color:#8499b4;margin-top:5px}.up{color:#58e69d}.down{color:#ff7e8b}.amber{color:#ffd166}.charts{display:grid;grid-template-columns:2fr 1fr;gap:14px}.chartbox{height:310px;position:relative;direction:ltr}.range{display:flex;justify-content:space-between;direction:ltr;color:#d6e4f5;font-size:13px;background:#091728;border-radius:9px;padding:7px 10px;margin:7px 0}.range b{font-size:15px}canvas{width:100%;height:100%}.legend{display:flex;gap:15px;flex-wrap:wrap;font-size:13px;color:#b8c6d9;margin:8px 0}.sw{width:10px;height:10px;border-radius:2px;display:inline-block;margin-left:5px}table{width:100%;table-layout:fixed;border-collapse:collapse;background:#0e1e32;border-radius:14px;overflow:hidden;font-size:13px}th,td{padding:9px 4px;border-bottom:1px solid #263b55;text-align:center;white-space:normal;overflow-wrap:anywhere}.scroll{overflow:hidden}h1{margin:14px 0 5px}h2{font-size:20px}.market{direction:ltr;text-align:left;color:#8facd0;font-size:13px;overflow:hidden;text-overflow:ellipsis}.bar{height:12px;background:#ff6575;border-radius:8px;overflow:hidden;display:flex;direction:ltr}.bar span{background:#42dc91}.err{background:#4a1c27;padding:12px;border-radius:10px}@media(max-width:760px){.charts{grid-template-columns:1fr}.chartbox{height:250px}h1{font-size:25px}.v{font-size:21px}body{padding:12px}table{font-size:11px}th,td{padding:8px 2px}.status{font-size:10px}}
</style><div class="wrap"><div class="top"><span class="badge">מחקר בלבד · אין מסחר אמיתי</span><span id="live"><i class="dot"></i>מתחבר…</span></div><h1>Polymarket BTC — לייב 5 דקות</h1><div id="market" class="market">—</div><div id="app">טוען נתונים…</div></div><script>
const money=x=>x==null?'—':'$'+Number(x).toLocaleString(undefined,{maximumFractionDigits:2});
const usd=x=>x==null?'—':'$'+Number(x).toLocaleString(undefined,{maximumFractionDigits:2});
const cent=x=>x==null?'—':(x*100).toFixed(1)+'¢'; const pct=x=>x==null?'—':(x*100).toFixed(1)+'%';
function chart(canvas, rows, keys, colors, probability=false){let ctx=canvas.getContext('2d'),w=canvas.clientWidth,h=canvas.clientHeight,d=devicePixelRatio||1;canvas.width=w*d;canvas.height=h*d;ctx.scale(d,d);ctx.clearRect(0,0,w,h);let pad=60;ctx.direction='ltr';ctx.textAlign='left';if(rows.length<2){ctx.fillStyle='#8499b4';ctx.fillText('Waiting for data...',pad,h/2);return}let vals=rows.flatMap(r=>keys.map(k=>r[k]).filter(x=>x!=null)),mn=probability?0:Math.min(...vals),mx=probability?1:Math.max(...vals);if(mx==mn){mx+=1;mn-=1}ctx.strokeStyle='#213650';ctx.fillStyle='#c5d5e8';ctx.font='bold 12px Arial';for(let i=0;i<=4;i++){let y=20+(h-40)*i/4;ctx.beginPath();ctx.moveTo(pad,y);ctx.lineTo(w-8,y);ctx.stroke();let value=mx-(mx-mn)*i/4,label=probability?(value*100).toFixed(0)+'c':'$'+value.toLocaleString(undefined,{maximumFractionDigits:0});ctx.fillText(label,3,y+4)}keys.forEach((k,ki)=>{ctx.strokeStyle=colors[ki];ctx.lineWidth=2.5;ctx.beginPath();let started=false;rows.forEach((r,i)=>{if(r[k]==null)return;let x=pad+(w-pad-10)*i/(rows.length-1),y=20+(h-40)*(mx-r[k])/(mx-mn);started?ctx.lineTo(x,y):ctx.moveTo(x,y);started=true});ctx.stroke()})}
function render(d){let s=d.samples,t=d.trades,r=d.runtime,c=d.current||{},rows=d.series||[],up=c.up_ask,down=c.down_ask,model=c.p_up,delta=c.btc&&c.open_btc?(c.btc/c.open_btc-1):null,left=Math.max(0,Math.round(c.seconds_left||0));document.getElementById('market').textContent=r.market||'ממתין לשוק';document.getElementById('live').innerHTML=`<i class="dot"></i><span class="live">LIVE</span> · עודכן ${new Date((s.last||d.server_time)*1000).toLocaleTimeString('he-IL')}`;document.getElementById('app').innerHTML=`
<div class="grid"><div class="card"><div class="label">נותר בחלון</div><div class="v amber">${Math.floor(left/60)}:${String(left%60).padStart(2,'0')}</div><div class="mini">חלון של 5 דקות</div></div><div class="card"><div class="label">BTC עכשיו</div><div class="v">${money(c.btc||r.btc)}</div><div class="mini">פתיחה: ${money(c.open_btc)}</div></div><div class="card"><div class="label">שינוי מהפתיחה</div><div class="v ${delta>=0?'up':'down'}">${delta==null?'—':(delta*100).toFixed(3)+'%'}</div><div class="mini">מודל UP: ${pct(model)}</div></div><div class="card"><div class="label">Polymarket UP</div><div class="v up">${cent(up)}</div><div class="mini">Bid ${cent(c.up_bid)} · Ask ${cent(up)}</div></div><div class="card"><div class="label">Polymarket DOWN</div><div class="v down">${cent(down)}</div><div class="mini">Bid ${cent(c.down_bid)} · Ask ${cent(down)}</div></div><div class="card"><div class="label">Edge הטוב כרגע</div><div class="v">${Math.max(model-(up||1),(1-model)-(down||1),0)*100|0}%</div><div class="mini">סף כניסה: 8%</div></div></div>
<div class="charts"><div class="panel"><h2>מחיר BTC בתוך החלון</h2><div class="legend"><span><i class="sw" style="background:#4ea1ff"></i>BTC</span><span><i class="sw" style="background:#ffd166"></i>מחיר פתיחה</span></div><div class="range"><span>נמוך <b>${money(Math.min(...rows.map(x=>x.btc||Infinity)))}</b></span><span>עכשיו <b>${money(c.btc)}</b></span><span>גבוה <b>${money(Math.max(...rows.map(x=>x.btc||0)))}</b></span></div><div class="chartbox"><canvas id="btcChart"></canvas></div></div><div class="panel"><h2>Polymarket מול המודל</h2><div class="legend"><span><i class="sw" style="background:#45e69a"></i>UP Ask</span><span><i class="sw" style="background:#ff6575"></i>DOWN Ask</span><span><i class="sw" style="background:#b584ff"></i>Model UP</span></div><div class="range"><span>UP <b class="up">${cent(up)}</b></span><span>מודל <b>${cent(model)}</b></span><span>DOWN <b class="down">${cent(down)}</b></span></div><div class="bar"><span style="width:${(up||.5)*100}%"></span></div><div class="chartbox"><canvas id="polyChart"></canvas></div></div></div>
<h2>הארנק המדומה</h2><div class="grid"><div class="card"><div class="label">שווי הארנק</div><div class="v ${d.wallet.equity>=d.wallet.initial?'up':'down'}">${usd(d.wallet.equity)}</div></div><div class="card"><div class="label">יתרה ממומשת</div><div class="v">${usd(d.wallet.balance)}</div></div><div class="card"><div class="label">כסף פנוי</div><div class="v">${usd(d.wallet.available)}</div></div><div class="card"><div class="label">בעסקאות פתוחות</div><div class="v amber">${usd(d.wallet.locked)}</div></div><div class="card"><div class="label">רווח פתוח</div><div class="v ${d.wallet.unrealized>=0?'up':'down'}">${usd(d.wallet.unrealized)}</div></div><div class="card"><div class="label">הפקדה התחלתית</div><div class="v">${usd(d.wallet.initial)}</div></div></div>
<div class="grid"><div class="card"><div class="label">חלונות שנאספו</div><div class="v">${s.markets||0}</div></div><div class="card"><div class="label">דגימות שוק</div><div class="v">${s.n||0}</div><div class="mini">תצפיות, לא עסקאות</div></div><div class="card"><div class="label">עסקאות מדומות</div><div class="v">${t.n||0}</div></div><div class="card"><div class="label">נסגרו / הצליחו</div><div class="v">${t.settled||0} / ${t.wins||0}</div><div class="mini">Win rate: ${t.win_rate||0}%</div></div><div class="card"><div class="label">P&L סופי</div><div class="v ${(t.pnl||0)>=0?'up':'down'}">${usd(t.pnl||0)}</div><div class="mini">עסקאות שהוכרעו</div></div><div class="card"><div class="label">P&L נוכחי</div><div class="v ${(t.mark_pnl||0)>=0?'up':'down'}">${usd(t.mark_pnl||0)}</div><div class="mini">כולל עסקאות פתוחות</div></div></div>
<section id="analysis"><h2>ניתוח מצטבר</h2><div class="grid"><div class="card"><div class="label">רווחים גולמיים</div><div class="v up">${usd(d.analysis.gross_profit)}</div></div><div class="card"><div class="label">הפסדים גולמיים</div><div class="v down">${usd(-d.analysis.gross_loss)}</div></div><div class="card"><div class="label">Profit Factor</div><div class="v">${d.analysis.profit_factor||'—'}</div></div><div class="card"><div class="label">ממוצע לעסקה</div><div class="v">${usd(d.analysis.avg_pnl)}</div></div><div class="card"><div class="label">חציון לעסקה</div><div class="v">${usd(d.analysis.median_pnl)}</div></div><div class="card"><div class="label">5 הזכיות הגדולות</div><div class="v">${d.analysis.top5_profit_share}%</div><div class="mini">מסך הרווחים הגולמיים</div></div></div><h2>בדיקת יציאה חכמה — מהיום</h2><div class="grid"><div class="card"><div class="label">עסקאות שיצאו</div><div class="v">${d.analysis.managed.trades}</div></div><div class="card"><div class="label">P&L ביציאה חכמה</div><div class="v ${d.analysis.managed.pnl>=0?'up':'down'}">${usd(d.analysis.managed.pnl)}</div></div><div class="card"><div class="label">הצלחה ביציאות</div><div class="v">${d.analysis.managed.win_rate}%</div></div><div class="card"><div class="label">ממוצע ליציאה</div><div class="v">${usd(d.analysis.managed.avg_pnl)}</div></div></div><div class="mini">Take profit 35% · Stop loss 30% · היפוך מודל · יציאה 15 שניות לפני הסיום. ההשוואה מתחילה מהגרסה הנוכחית בלבד.</div><div class="scroll"><table><tr><th>קבוצה</th><th>עסקאות</th><th>הצלחות</th><th>Win rate</th><th>P&L</th></tr>${Object.entries({...d.analysis.by_side,...d.analysis.by_price,...d.analysis.by_edge}).map(([k,x])=>`<tr><td>${k}</td><td>${x.trades}</td><td>${x.wins}</td><td>${x.win_rate}%</td><td class="${x.pnl>=0?'up':'down'}">${usd(x.pnl)}</td></tr>`).join('')}</table></div></section>
<h2>עסקאות מדומות אחרונות</h2><div class="scroll"><table><tr><th>זמן</th><th>צד</th><th>כניסה</th><th>עכשיו</th><th>מודל</th><th>Edge</th><th>מצב</th><th>רווח/הפסד</th></tr>${d.recent.map(x=>`<tr><td>${new Date(x.ts*1000).toLocaleTimeString('he-IL',{hour:'2-digit',minute:'2-digit'})}</td><td class="${x.side==='Up'?'up':'down'}">${x.side}</td><td>${cent(x.fill_price)}</td><td>${cent(x.mark_price)}</td><td>${pct(x.model_p)}</td><td>${pct(x.edge)}</td><td class="status">${x.display_status}</td><td class="${(x.display_pnl||0)>=0?'up':'down'}">${x.display_pnl==null?'—':usd(x.display_pnl)}</td></tr>`).join('')}</table></div>${r.last_error?'<p class="err">'+r.last_error+'</p>':''}`;chart(document.getElementById('btcChart'),rows,['btc','open_btc'],['#4ea1ff','#ffd166']);chart(document.getElementById('polyChart'),rows,['up_ask','down_ask','p_up'],['#45e69a','#ff6575','#b584ff'],true)}
async function load(){try{let d=await(await fetch('/api/stats',{cache:'no-store'})).json();window.__lastData=d;render(d)}catch(e){document.getElementById('live').textContent='שגיאת חיבור'}}load();setInterval(load,2000);addEventListener('resize',load);
</script></html>''')
