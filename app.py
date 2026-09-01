import os, time, threading, sqlite3
from datetime import datetime, timezone
from flask import Flask, jsonify, render_template_string
import requests

PAIRS = [p.strip().upper() for p in os.getenv('PAIRS','SOLUSDT,SUIUSDT').split(',') if p.strip()]
POLL_SECONDS = float(os.getenv('POLL_SECONDS','1.0'))
MEXC_FEE = float(os.getenv('MEXC_TAKER_FEE','0.0005'))   # hypothesis: 0.05%, configure to your account
GATE_FEE = float(os.getenv('GATE_TAKER_FEE','0.0010'))   # VIP0 currently 0.10%
MIN_NET = float(os.getenv('MIN_NET_SPREAD','0.0015'))    # 0.15%
TRADE_SIZES = [float(x) for x in os.getenv('TRADE_SIZES','500,1000,2500,5000').split(',')]
DB_PATH = os.getenv('DB_PATH','scanner.db')

app = Flask(__name__)
lock = threading.Lock()
state = {}

HTML = r'''<!doctype html><html lang="fr"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>MEXC ↔ Gate Arbitrage</title><style>
body{font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif;margin:18px;background:#0b0d10;color:#f5f7fa}.wrap{max-width:980px;margin:auto}
.card{background:#15191f;border:1px solid #2a313b;border-radius:16px;padding:16px;margin:12px 0}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:12px}
.big{font-size:28px;font-weight:700}.muted{color:#aeb7c2}.good{color:#4ee28a}.bad{color:#ff7676}.neutral{color:#d8dee9} table{width:100%;border-collapse:collapse}th,td{padding:9px;text-align:right;border-bottom:1px solid #2a313b}th:first-child,td:first-child{text-align:left}.pill{padding:4px 8px;border-radius:999px;background:#252c35;font-size:12px}.small{font-size:12px}</style></head>
<body><div class="wrap"><h1>MEXC ↔ Gate</h1><div class="muted">Scanner lecture seule • SOL/USDT & SUI/USDT • actualisation automatique</div><div id="cards" class="grid"></div><div class="card"><b>Paramètres</b><div class="small muted" id="params"></div></div></div>
<script>
function pct(x){return (x*100).toFixed(3)+'%'} function n(x,d=4){return Number(x).toLocaleString('fr-FR',{maximumFractionDigits:d})}
async function refresh(){let r=await fetch('/api/state');let d=await r.json();let html='';
for(const [pair,x] of Object.entries(d.pairs)){
 let best=x.best; let cls=best.net_spread>=d.min_net?'good':(best.net_spread>0?'neutral':'bad');
 html+=`<div class="card"><div style="display:flex;justify-content:space-between"><b>${pair.replace('USDT','/USDT')}</b><span class="pill">${best.direction}</span></div>
 <div class="big ${cls}">${pct(best.net_spread)}</div><div class="muted">spread net estimé</div>
 <table><tr><th></th><th>MEXC</th><th>Gate</th></tr><tr><td>Bid</td><td>${n(x.mexc.bid)}</td><td>${n(x.gate.bid)}</td></tr><tr><td>Ask</td><td>${n(x.mexc.ask)}</td><td>${n(x.gate.ask)}</td></tr></table>
 <div class="small muted">Brut: ${pct(best.gross_spread)} • Profit net / 1 000 USDT: ${n(best.net_profit_1000,2)} USDT<br>Maj: ${x.updated||'-'}</div></div>`}
 document.getElementById('cards').innerHTML=html||'<div class="card">En attente des données…</div>';
 document.getElementById('params').innerText=`Frais taker MEXC: ${pct(d.mexc_fee)} | Gate: ${pct(d.gate_fee)} | Alerte à partir de ${pct(d.min_net)} | Poll ${d.poll_seconds}s`;
}
setInterval(refresh,1500);refresh();</script></body></html>'''

def db_init():
    with sqlite3.connect(DB_PATH) as c:
        c.execute('''CREATE TABLE IF NOT EXISTS samples(ts INTEGER,pair TEXT,mexc_bid REAL,mexc_ask REAL,gate_bid REAL,gate_ask REAL,dir TEXT,gross REAL,net REAL)''')
        c.execute('CREATE INDEX IF NOT EXISTS idx_samples ON samples(pair,ts)')

def mexc_book(pair):
    r=requests.get('https://api.mexc.com/api/v3/depth',params={'symbol':pair,'limit':20},timeout=4); r.raise_for_status(); j=r.json()
    return {'bid':float(j['bids'][0][0]),'ask':float(j['asks'][0][0]),'bids':j['bids'],'asks':j['asks']}

def gate_book(pair):
    gp=pair.replace('USDT','_USDT')
    r=requests.get('https://api.gateio.ws/api/v4/spot/order_book',params={'currency_pair':gp,'limit':20},timeout=4); r.raise_for_status(); j=r.json()
    return {'bid':float(j['bids'][0][0]),'ask':float(j['asks'][0][0]),'bids':j['bids'],'asks':j['asks']}

def direction(m,g):
    # Buy at ask on cheap exchange; sell at bid on expensive exchange.
    a_gross = g['bid']/m['ask']-1.0  # MEXC -> Gate
    b_gross = m['bid']/g['ask']-1.0  # Gate -> MEXC
    a_net = (g['bid']*(1-GATE_FEE))/(m['ask']*(1+MEXC_FEE))-1.0
    b_net = (m['bid']*(1-MEXC_FEE))/(g['ask']*(1+GATE_FEE))-1.0
    if a_net >= b_net: return {'direction':'MEXC → Gate','gross_spread':a_gross,'net_spread':a_net,'net_profit_1000':1000*a_net}
    return {'direction':'Gate → MEXC','gross_spread':b_gross,'net_spread':b_net,'net_profit_1000':1000*b_net}

def worker():
    while True:
        for p in PAIRS:
            try:
                m=mexc_book(p); g=gate_book(p); b=direction(m,g); now=int(time.time())
                rec={'mexc':m,'gate':g,'best':b,'updated':datetime.now().strftime('%H:%M:%S')}
                with lock: state[p]=rec
                with sqlite3.connect(DB_PATH) as c:
                    c.execute('INSERT INTO samples VALUES(?,?,?,?,?,?,?,?,?)',(now,p,m['bid'],m['ask'],g['bid'],g['ask'],b['direction'],b['gross_spread'],b['net_spread']))
            except Exception as e:
                with lock:
                    old=state.get(p,{})
                    old['error']=str(e); old['updated']=datetime.now().strftime('%H:%M:%S')
                    state[p]=old
        time.sleep(POLL_SECONDS)

@app.route('/')
def home(): return render_template_string(HTML)

@app.route('/api/state')
def api_state():
    with lock: s={k:v for k,v in state.items() if 'mexc' in v and 'gate' in v}
    return jsonify({'pairs':s,'mexc_fee':MEXC_FEE,'gate_fee':GATE_FEE,'min_net':MIN_NET,'poll_seconds':POLL_SECONDS})

@app.route('/api/stats/<pair>')
def stats(pair):
    pair=pair.upper(); since=int(time.time())-86400
    with sqlite3.connect(DB_PATH) as c:
        rows=c.execute('SELECT dir,COUNT(*),AVG(net),MAX(net) FROM samples WHERE pair=? AND ts>=? AND net>=? GROUP BY dir',(pair,since,MIN_NET)).fetchall()
    return jsonify({'pair':pair,'window':'24h','threshold':MIN_NET,'directions':[{'direction':r[0],'samples':r[1],'avg_net':r[2],'max_net':r[3]} for r in rows]})

if __name__=='__main__':
    db_init()
    threading.Thread(target=worker,daemon=True).start()
    app.run(host='0.0.0.0',port=int(os.getenv('PORT','8080')),threaded=True)
