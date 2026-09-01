from flask import Flask, jsonify, render_template_string
import requests, time, sqlite3, threading, os
from datetime import datetime, timezone

app=Flask(__name__)
DB="scanner_v2.db"; INTERVAL=5
SIZES=[500,1000,2500,5000]
MEXC_FEE=float(os.getenv("MEXC_FEE","0.0005"))
GATE_FEE=float(os.getenv("GATE_FEE","0.0010"))
SYMBOLS=["BTC","ETH","SOL","XRP","DOGE","SUI","ADA","AVAX","LINK","TRX","TON","DOT","LTC","BCH","NEAR","APT","ARB","OP","INJ","SEI","PEPE","SHIB","FET","RENDER","TAO","ONDO","HBAR","XLM","UNI","AAVE"]
MEXC_DEPTH="https://api.mexc.com/api/v3/depth"
GATE_DEPTH="https://api.gateio.ws/api/v4/spot/order_book"
latest={}; lock=threading.Lock()
conn=sqlite3.connect(DB,check_same_thread=False)
conn.execute("CREATE TABLE IF NOT EXISTS opportunities(ts INTEGER,symbol TEXT,direction TEXT,size_usdt REAL,buy_avg REAL,sell_avg REAL,gross_pct REAL,net_pct REAL,profit_usdt REAL)")
conn.commit()

def avg_buy(asks, quote):
    rem=quote; qty=spent=0.0
    for p,q in asks:
        p=float(p); q=float(q); level=p*q; take=min(rem,level)
        if take<=0: break
        qty += take/p; spent += take; rem -= take
        if rem<=1e-9: break
    return None if rem>1e-6 or qty<=0 else (spent/qty,qty)

def avg_sell(bids, qty):
    rem=qty; proceeds=sold=0.0
    for p,q in bids:
        p=float(p); q=float(q); take=min(rem,q)
        if take<=0: break
        proceeds += take*p; sold += take; rem -= take
        if rem<=1e-12: break
    return None if rem>1e-9 or sold<=0 else (proceeds/sold,proceeds)

def mexc_book(sym):
    j=requests.get(MEXC_DEPTH,params={"symbol":sym+"USDT","limit":100},timeout=5).json()
    return j["bids"],j["asks"]

def gate_book(sym):
    j=requests.get(GATE_DEPTH,params={"currency_pair":sym+"_USDT","limit":100},timeout=5).json()
    return j["bids"],j["asks"]

def evaluate(sym,mb,ma,gb,ga):
    out=[]
    for size in SIZES:
        for direction,asks,bids,bfee,sfee in [
            ("MEXC→Gate",ma,gb,MEXC_FEE,GATE_FEE),
            ("Gate→MEXC",ga,mb,GATE_FEE,MEXC_FEE)]:
            b=avg_buy(asks,size)
            if not b: continue
            buy,qty=b; s=avg_sell(bids,qty)
            if not s: continue
            sell,proceeds=s
            gross=(sell/buy-1)*100
            cost=size*(1+bfee); netpro=proceeds*(1-sfee)
            profit=netpro-cost; net=profit/cost*100
            out.append([direction,size,buy,sell,gross,net,profit])
    return out

def worker():
    while True:
        ts=int(time.time()); snap={}
        for sym in SYMBOLS:
            try:
                mb,ma=mexc_book(sym); gb,ga=gate_book(sym)
                rows=evaluate(sym,mb,ma,gb,ga); snap[sym]=rows
                conn.executemany("INSERT INTO opportunities VALUES(?,?,?,?,?,?,?,?,?)",[(ts,sym,*r) for r in rows]); conn.commit()
            except Exception as e:
                snap[sym]={"error":str(e)}
        with lock:
            latest.clear(); latest.update(snap)
        time.sleep(INTERVAL)

threading.Thread(target=worker,daemon=True).start()

HTML="""<!doctype html><html><head><meta name='viewport' content='width=device-width,initial-scale=1'><title>Scanner V2</title>
<style>body{font-family:Arial;background:#111;color:#eee;margin:16px}.card{background:#1b1b1b;padding:12px;margin:10px 0;border-radius:12px}table{width:100%;font-size:12px;border-collapse:collapse}td,th{padding:6px;border-bottom:1px solid #333;text-align:right}td:first-child,th:first-child{text-align:left}</style></head>
<body><h2>MEXC ↔ Gate Scanner V2</h2><div>30 cryptos • profondeur réelle • 500/1000/2500/5000 USDT</div><div id='r'></div>
<script>async function load(){let d=await (await fetch('/api/latest')).json(),h='';for(let [s,rows] of Object.entries(d)){if(!Array.isArray(rows)||!rows.length)continue;rows.sort((a,b)=>b[5]-a[5]);let t=rows[0];h+=`<div class="card"><b>${s}/USDT</b> — meilleur net ${t[5].toFixed(3)}% (${t[0]}, ${t[1]} USDT)<table><tr><th>Sens</th><th>Taille</th><th>Brut%</th><th>Net%</th><th>Profit</th></tr>`;for(let x of rows.slice(0,8))h+=`<tr><td>${x[0]}</td><td>${x[1]}</td><td>${x[4].toFixed(3)}</td><td>${x[5].toFixed(3)}</td><td>${x[6].toFixed(2)}</td></tr>`;h+='</table></div>'}document.getElementById('r').innerHTML=h}load();setInterval(load,5000)</script></body></html>"""

@app.route("/")
def index(): return render_template_string(HTML)
@app.route("/api/latest")
def api_latest():
    with lock: return jsonify(latest)
@app.route("/health")
def health(): return {"ok":True,"time":datetime.now(timezone.utc).isoformat()}
if __name__=="__main__": app.run(host="0.0.0.0",port=8080,threaded=True)
