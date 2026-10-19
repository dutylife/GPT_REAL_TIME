"""即時報價循環器；直接取代 run_loop.py，搭配使用者原有的 bot.py。

策略一/三沿用原本的「相對開盤」門檻；顯示另列昨收漲跌。
策略二用實際觀測的成交價，每分鐘保留最後一筆，搭配 Yahoo 當日歷史補種。
這些是採樣價序列，不是逐筆成交；無法還原兩次採樣間的完整波動。
即時來源失敗只重試，不以延遲來源發即時訊號。預設每輪開始相隔30秒，
掃描超時不重疊啟動。均量及法人準備在背景進行，不阻塞報價請求。
只執行本檔；不要同時執行舊 bot.py/main 或另一份監控工作。
"""
import concurrent.futures as futures
import html
import json
import math
import os
import threading
import time
import traceback
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo

import bot

TZ = ZoneInfo('Asia/Taipei')
INTERVAL = max(30, int(os.getenv('REALTIME_INTERVAL_SEC', '30')))
MAX_AGE = 120  # 報價及最新成交都須在此秒數以內，冷門股可能因此不發報
BATCH_SIZE = 40
MAX_WORKERS = 2
OPEN = dtime(9, 0, 10)
CLOSE = dtime(13, 32)
HEADERS = {'User-Agent': 'Mozilla/5.0',
           'Referer': 'https://mis.twse.com.tw/stock/index.jsp'}
URL = 'https://mis.twse.com.tw/stock/api/getStockInfo.jsp'
LOCK = threading.Lock()
BASELINES = {}
HISTORY = {}  # code -> {aware minute timestamp: actual sampled close}
MARKETS = {}
CHIP_READY = threading.Event()
STOP = threading.Event()
STARTED_AT = None


def now():
    return datetime.now(TZ)


def positive(value):
    try:
        x = float(str(value).replace(',', ''))
        return x if math.isfinite(x) and x > 0 else None
    except (ValueError, TypeError):
        return None


def parse_quote(row, received_at):
    """僅採成交價，z缺值時取同日 trade.z；絕不使用委買/委賣價。"""
    code = str(row.get('c', ''))
    market = row.get('ex')
    if code not in bot.WATCH_STOCKS or market not in ('tse', 'otc'):
        return None
    try:
        date = datetime.strptime(str(row['d']), '%Y%m%d').date()
        if date != received_at.date():
            return None
        quote_at = datetime.strptime(f"{row['d']} {row['t']}",
                                     '%Y%m%d %H:%M:%S').replace(tzinfo=TZ)
        price = positive(row.get('z'))
        trade_at = quote_at
        if price is None:
            trade = row.get('trade') or {}
            price = positive(trade.get('z'))
            trade_at = datetime.strptime(f"{row['d']} {trade['t']}",
                                         '%Y%m%d %H:%M:%S').replace(tzinfo=TZ)
        opened, previous = positive(row.get('o')), positive(row.get('y'))
        lots = positive(row.get('v'))
        if None in (price, opened, previous, lots):
            return None
        ages = [(received_at - t).total_seconds() for t in (quote_at, trade_at)]
        if any(age < -5 or age > MAX_AGE for age in ages):
            return None
        if not dtime(9) <= trade_at.time() <= dtime(13, 30):
            return None
        return dict(code=code, suffix='.TW' if market == 'tse' else '.TWO',
                    price=price, opened=opened, previous=previous, lots=lots,
                    quote_at=quote_at, trade_at=trade_at)
    except (KeyError, ValueError, TypeError):
        return None


def fetch_batch(channels):
    query = urllib.parse.urlencode({'ex_ch': '|'.join(channels), 'json': '1',
                                   'delay': '0', '_': str(time.time_ns())})
    request = urllib.request.Request(URL + '?' + query, headers=HEADERS)
    with urllib.request.urlopen(request, timeout=8) as response:
        data = json.load(response)
    if str(data.get('rtcode', '0000')) != '0000':
        raise RuntimeError('MIS回應狀態異常')
    rows = data.get('msgArray')
    if not isinstance(rows, list):
        raise ValueError('MIS未回傳msgArray')
    return rows


def wave_metrics(points, opened):
    """與原版 zigzag 一致的反轉規則，輸入為觀測價序列。"""
    if len(points) < 3:
        return False, None, None, None
    # 復用原版函式，保留1pp反轉確認及3pp新波段回檔條件。
    series = bot.pd.Series({t: (p - opened) / opened * 100 for t, p in points})
    return bot.find_current_wave_trough(series, bot.WAVE_REVERSAL_PP)


def metrics(q):
    code, price, opened = q['code'], q['price'], q['opened']
    minute = q['trade_at'].replace(second=0, microsecond=0)
    with LOCK:
        points = HISTORY.setdefault(code, {})
        # 同分鐘若背景歷史已到達，後來即時觀測覆寫該分鐘。
        points[minute] = price
        ordered = sorted(points.items())
        avg_vol, avg_amount = BASELINES.get(code, (None, None))
    # 按報價時間校正金額與量比，不把停留的舊成交時間當現在時段。
    elapsed = max(1, (q['quote_at'].hour - 9) * 60 + q['quote_at'].minute + 1)
    ratio = min(elapsed / bot.TOTAL_SESSION_MINUTES, 1)
    pct_open = (price / opened - 1) * 100
    pct_prev = (price / q['previous'] - 1) * 100
    amount = price * q['lots'] * 1000 / 1e8  # 與原版一致的估算成交金額
    breakout, rebound, rel_trigger, min_amount, min_lots = bot.get_size_tier(avg_amount)
    up, trough, key, pullback = wave_metrics(ordered, opened)
    s1 = pct_open >= breakout and amount >= max(bot.BASE_TURNOVER_FULLDAY * ratio,
                                              bot.MIN_TURNOVER_FLOOR)
    s2 = bool(up and trough is not None and pct_open - trough >= rebound and
              amount >= max(bot.BASE_TURNOVER_FULLDAY_S2 * ratio, bot.MIN_TURNOVER_FLOOR))
    relative = q['lots'] * 1000 / (avg_vol * ratio) if avg_vol and avg_vol > 0 else 0
    s3 = bool(rel_trigger is not None and relative >= rel_trigger and
              bot.QUIET_PCT_LOW <= pct_open <= bot.QUIET_PCT_HIGH and
              amount >= min_amount and q['lots'] >= min_lots)
    return dict(pct_open=pct_open, pct_prev=pct_prev, amount=amount,
                relative=relative, s1=s1, s2=s2, s3=s3,
                trough_key=key.isoformat() if key is not None else None,
                pullback=pullback, baseline_ready=avg_amount is not None)


def choose_signal(m, existing):
    key = m['trough_key']
    wave_ok = m['s2'] and key is not None and key != (existing or {}).get('wave_trough_key')
    if wave_ok and existing is not None:
        wave_ok = m['pullback'] is not None and m['pullback'] >= bot.WAVE_REALERT_MIN_PULLBACK_PP
    if wave_ok:
        return 1, '🔄波段轉強', key
    severity, label = (2, '🔥強勢突破') if m['s1'] else (3, '⚠️異常量能')
    if not (m['s1'] or m['s3']) or (existing and severity >= existing['severity']):
        return None
    return severity, label, (existing or {}).get('wave_trough_key')


def send(q, m, selected, state):
    severity, label, key = selected
    # 背景資料準備完才啟用法人判斷；失敗時遵循原版「註明後放行」。
    if not CHIP_READY.is_set():
        return False
    chip_ok, net = bot.chip_check(q['code'], q['suffix'])
    if chip_ok is False:
        return False
    if any((now() - q[t]).total_seconds() > MAX_AGE for t in ('quote_at', 'trade_at')):
        return False
    code = q['code']
    name = html.escape(bot.WATCH_STOCKS[code])
    extra = ''
    if severity == 3:
        extra += f"\n ↳ 量能倍數：{m['relative']:.1f}x；量價無法判定大戶買賣方向"
    if net is not None:
        extra += f'\n ↳ 🏦 法人近{bot.CHIP_LOOKBACK_DAYS}日(外資+投信)：{net:+,.0f}張'
    if not m['baseline_ready']:
        extra += '\n ↳ 均量準備中：採原版保守預設門檻，暫停異常量能策略'
    if state.get(code) and severity == 1:
        extra += f"\n ↳ 新波段：先前回檔{m['pullback']:.1f}pp"
    note = html.escape(bot.chip_note())
    message = (
        f'🤖 <b>Claude Real Time｜MIS報價</b>\n{note}'
        f'<a href="https://www.cmoney.tw/finance/{code}/stockmainkline">{code} {name}</a> {label}\n'
        f" ↳ 現價：<code>{q['price']:.2f}元</code>｜昨收漲跌：<code>{m['pct_prev']:+.2f}%</code>\n"
        f" ↳ 較開盤：<code>{m['pct_open']:+.2f}%</code>（原策略判斷基準）\n"
        f" ↳ 盤中量：{q['lots']:,.0f}張｜估算金額：{m['amount']:.2f}億\n"
        f" ↳ 報價：{q['quote_at']:%H:%M:%S}｜成交：{q['trade_at']:%H:%M:%S}\n"
        f" ↳ 發送：{now():%H:%M:%S}{extra}"
    )
    if bot.send_telegram(message):
        state[code] = {'severity': severity, 'wave_trough_key': key}
        bot.save_alert_state(bot.ALERT_STATE_PATH, state)
        return True
    return False


def seed_history(mapping):
    """Yahoo只用來補今天早先的波段歷史，不用作目前價/目前量。"""
    items = list(mapping.items())
    for start in range(0, len(items), 40):
        if STOP.is_set():
            return
        chunk = items[start:start + 40]
        try:
            result = bot.chunked_download([c + s for c, s in chunk],
                chunk_size=40, max_retries=0, inter_chunk_delay=0,
                period='1d', interval='1m', auto_adjust=False, threads=8, timeout=8)
            for code, suffix in chunk:
                df = result.get(code + suffix)
                if df is None or df.empty:
                    continue
                closes = df['Close']
                # 某些yfinance版本對單檔仍回傳多層欄位。
                if getattr(closes, 'ndim', 1) == 2:
                    closes = closes.iloc[:, 0]
                with LOCK:
                    points = HISTORY.setdefault(code, {})
                    for ts, value in closes.dropna().items():
                        if ts.tzinfo is None:
                            continue
                        stamp = ts.tz_convert(TZ)
                        price = positive(value)
                        # 啟動之後只信任即時觀測；不得以後到的延遲K棒覆蓋。
                        if (stamp.date() == now().date() and stamp < STARTED_AT.replace(second=0, microsecond=0)
                                and dtime(9) <= stamp.time() < dtime(13, 30) and price):
                            points.setdefault(stamp.replace(second=0, microsecond=0), price)
        except Exception as exc:
            print(f'⚠️ 波段歷史補種失敗：{type(exc).__name__}；繼續累積即時觀測', flush=True)
    print('✅ 波段歷史補種完成；中途缺少的逐筆波動無法還原', flush=True)


def prepare(mapping):
    try:
        bot.ensure_chip_data()
    except Exception:
        traceback.print_exc()
    finally:
        CHIP_READY.set()
    # 均量先排除今天，避免盤中尚未收完的日K污染20日基準。
    original_download = bot.chunked_download

    def daily_download(tickers, **kwargs):
        kwargs.update(period='2mo', end=now().date().isoformat(),
                      auto_adjust=False, threads=8, timeout=8)
        result = original_download(tickers, **kwargs)
        cleaned = {}
        for ticker, frame in result.items():
            if getattr(frame.columns, 'nlevels', 1) > 1:
                for level in range(frame.columns.nlevels):
                    if ticker in frame.columns.get_level_values(level):
                        frame = frame.xs(ticker, axis=1, level=level)
                        break
            cleaned[ticker] = frame.tail(20)
        return cleaned

    # 只在本背景階段短暫換掉均量下載；此時尚未啟動歷史補種。
    bot.chunked_download = daily_download
    try:
        baseline = bot.fetch_avg_volumes(mapping, bot.CACHE_PATH)
        with LOCK:
            BASELINES.update(baseline)
        print(f'✅ 均量基準 {len(baseline)} 檔準備完成', flush=True)
    except Exception:
        traceback.print_exc()
    finally:
        bot.chunked_download = original_download
    seed_history(mapping)


def main():
    global STARTED_AT
    n = now()
    if n.weekday() >= 5:
        print('週末，結束。')
        return
    close_at = datetime.combine(n.date(), CLOSE, tzinfo=TZ)
    open_at = datetime.combine(n.date(), OPEN, tzinfo=TZ)
    if n >= close_at:
        print('已過收盤，結束。')
        return
    while now() < open_at:
        time.sleep(min(30, max(0, (open_at - now()).total_seconds())))
    STARTED_AT = now()
    # 新檔名防止沿用舊版可能含今天未完成日K的均量快取。
    bot.CACHE_PATH = 'avg_volume_prior20_cache.csv'
    BASELINES.update(bot.load_avg_volume_cache(bot.CACHE_PATH))
    state = bot.load_alert_state(bot.ALERT_STATE_PATH)
    background_started = False
    print(f'✅ MIS報價模式；目標每輪{INTERVAL}秒，超時則完成後接下一輪', flush=True)
    print('📌 等待法人初始化後才發送；未有歷史時波段策略需累積觀測', flush=True)
    try:
        with futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            while now() < close_at:
                started = time.monotonic()
                channels = []
                for code in bot.WATCH_STOCKS:
                    known = MARKETS.get(code)
                    prefixes = [known] if known else ['tse', 'otc']
                    channels.extend(f'{prefix}_{code}.tw' for prefix in prefixes)
                batches = [channels[i:i+BATCH_SIZE] for i in range(0, len(channels), BATCH_SIZE)]
                # 限制待執行批次，收盤後不留下整個池子的請求排隊。
                pending, cursor = set(), 0
                valid, emitted, errors = 0, 0, 0
                seen = set()
                while (cursor < len(batches) or pending) and now() < close_at:
                    while cursor < len(batches) and len(pending) < MAX_WORKERS:
                        pending.add(pool.submit(fetch_batch, batches[cursor]))
                        cursor += 1
                    done, pending = futures.wait(pending, return_when=futures.FIRST_COMPLETED)
                    for task in done:
                        try:
                            rows = task.result()
                        except Exception as exc:
                            errors += 1
                            print(f'⚠️ MIS批次失敗：{type(exc).__name__}；下一輪重試', flush=True)
                            continue
                        for row in rows:
                            code = str(row.get('c', ''))
                            if code in bot.WATCH_STOCKS and row.get('ex') in ('tse', 'otc'):
                                MARKETS[code] = row['ex']
                            q = parse_quote(row, now())
                            if not q or code in seen or now() >= close_at:
                                continue
                            seen.add(code)
                            valid += 1
                            try:
                                m = metrics(q)
                                selected = choose_signal(m, state.get(code))
                                if selected and send(q, m, selected, state):
                                    emitted += 1
                            except Exception:
                                print(f'⚠️ {code}處理失敗，繼續其他股票', flush=True)
                                traceback.print_exc()
                for task in pending:
                    task.cancel()
                if not background_started and MARKETS:
                    mapping = {c: '.TW' if ex == 'tse' else '.TWO' for c, ex in MARKETS.items()}
                    threading.Thread(target=prepare, args=(mapping,), daemon=True).start()
                    background_started = True
                elapsed = time.monotonic() - started
                print(f'📊 {now():%H:%M:%S} 有效成交報價{valid}/{len(bot.WATCH_STOCKS)} '
                      f'發送{emitted} 批次失敗{errors} 耗時{elapsed:.1f}秒｜'
                      f'法人初始化={CHIP_READY.is_set()}', flush=True)
                # 休市時不誤發舊資料，仍重試到截止，避免把來源故障誤判休市。
                pause = max(2, INTERVAL - elapsed) if errors else max(0, INTERVAL - elapsed)
                wait = min(pause, max(0, (close_at - now()).total_seconds()))
                if wait:
                    STOP.wait(wait)
    finally:
        STOP.set()
    print('收盤，結束。', flush=True)


if __name__ == '__main__':
    main()
