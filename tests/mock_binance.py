#!/usr/bin/env python3
"""Локальный mock Binance Futures для проверки логгера без доступа к бирже.
Запуск: python3 tests/mock_binance.py   (HTTP :18080, WS :18081)
Имитирует: exchangeInfo, /fapi/v1/depth, ws /public/stream (depth), /market/stream (aggTrade),
плюс один пропуск update id (t=8s) и один обрыв public-соединения (t=14s)."""
import asyncio, json, random, time, threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
import websockets

random.seed(1)
SYMS = {"BTCUSDT": 100000.0, "ETHUSDT": 4000.0}
class Sim:
    def __init__(s):
        s.u = {k: 1000 for k in SYMS}
        s.mid = dict(SYMS)
        s.bids = {k: {} for k in SYMS}; s.asks = {k: {} for k in SYMS}
        s.agg = {k: 500 for k in SYMS}
        for k, m in SYMS.items():
            for i in range(1, 400):
                s.bids[k][round(m - 0.1 * i, 2)] = round(random.uniform(0.1, 5), 3)
                s.asks[k][round(m + 0.1 * i, 2)] = round(random.uniform(0.1, 5), 3)
sim = Sim(); lock = threading.Lock(); t0 = time.time()

class H(BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def do_GET(self):
        u = urlparse(self.path)
        if u.path == "/fapi/v1/exchangeInfo":
            body = {"symbols": [{"symbol": k, "pricePrecision": 2, "quantityPrecision": 3, "x": 1} for k in SYMS]}
        elif u.path == "/fapi/v1/depth":
            k = parse_qs(u.query)["symbol"][0]
            time.sleep(0.15)
            with lock:
                bs = sorted(sim.bids[k].items(), reverse=True)[:1000]; as_ = sorted(sim.asks[k].items())[:1000]
                body = {"lastUpdateId": sim.u[k], "E": 0, "T": 0, "bids": [[f"{p:.2f}", f"{q:.3f}"] for p, q in bs], "asks": [[f"{p:.2f}", f"{q:.3f}"] for p, q in as_]}
        else:
            self.send_response(404); self.end_headers(); return
        b = json.dumps(body).encode()
        self.send_response(200); self.send_header("Content-Type", "application/json"); self.end_headers(); self.wfile.write(b)

pub, mkt = set(), set()
async def handler(ws):
    path = ws.request.path
    (pub if "/public/" in path else mkt).add(ws)
    try:
        await ws.wait_closed()
    finally:
        pub.discard(ws); mkt.discard(ws)

async def loop():
    dropped_conn = False; gapped = False
    while True:
        await asyncio.sleep(0.1)
        now = time.time() - t0
        for k in SYMS:
            with lock:
                first = sim.u[k] + 1
                n = random.randint(3, 12)
                if not gapped and now > 8 and k == "BTCUSDT":
                    first += 7; gapped = True      # дыра в update id
                    sim.u[k] += 7
                pu = sim.u[k] - (7 if first - 1 != sim.u[k] else 0) if False else None
                b, a = [], []
                prev_u = sim.u[k]
                for _ in range(n):
                    side = random.random() < 0.5
                    i = random.randint(1, 60)
                    if side:
                        p = round(sim.mid[k] - 0.1 * i, 2); q = 0.0 if random.random() < 0.3 else round(random.uniform(0.1, 6), 3)
                        (sim.bids[k].pop(p, None) if q == 0 else sim.bids[k].__setitem__(p, q)); b.append([f"{p:.2f}", f"{q:.3f}"])
                    else:
                        p = round(sim.mid[k] + 0.1 * i, 2); q = 0.0 if random.random() < 0.3 else round(random.uniform(0.1, 6), 3)
                        (sim.asks[k].pop(p, None) if q == 0 else sim.asks[k].__setitem__(p, q)); a.append([f"{p:.2f}", f"{q:.3f}"])
                last = first + n - 1
                sim.u[k] = last
            ev = int(time.time() * 1000) - 5
            msg = {"stream": f"{k.lower()}@depth@100ms", "data": {"e": "depthUpdate", "E": ev, "T": ev - 1, "s": k, "U": first, "u": last, "pu": prev_u, "b": b, "a": a}}
            if first - 1 != prev_u:  # после "дыры" pu указывает на реальный пред. u
                pass
            s = json.dumps(msg)
            for w in list(pub):
                try: await w.send(s)
                except Exception: pass
            if random.random() < 0.5:
                sim.agg[k] += 1
                tm = {"stream": f"{k.lower()}@aggTrade", "data": {"e": "aggTrade", "E": ev, "s": k, "a": sim.agg[k], "p": f"{sim.mid[k] + random.choice([-0.1, 0.1]):.2f}", "q": f"{random.uniform(0.001, 2):.3f}", "f": 1, "l": 1, "T": ev - 2, "m": random.random() < 0.5}}
                for w in list(mkt):
                    try: await w.send(json.dumps(tm))
                    except Exception: pass
        if not dropped_conn and now > 14:
            dropped_conn = True
            for w in list(pub): await w.close()

async def main():
    threading.Thread(target=lambda: HTTPServer(("127.0.0.1", 18080), H).serve_forever(), daemon=True).start()
    async with websockets.serve(handler, "127.0.0.1", 18081):
        await loop()
asyncio.run(main())
