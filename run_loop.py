"""
盤中循環執行器：搭配目前使用 Yahoo 1 分 K 的 bot.py。

- GitHub Actions 啟動一次，交易時段內反覆呼叫 bot.main()。
- 開盤前啟動：等待至 09:00:10。
- 每輪從開始時間算，間隔 5 分鐘。
- 掃描超過 5 分鐘：完成後直接進行下一輪。
- 單輪掃描發生例外：印出錯誤，繼續下一輪。
- 09:45 後檢查休市；查詢失敗不直接當作休市。
- 13:32 後不再啟動新一輪，正在執行的掃描會完成後結束。
"""

import time
import traceback
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo

import yfinance as yf
import bot

TZ = ZoneInfo("Asia/Taipei")

OPEN_T = dtime(9, 0)
CLOSE_T = dtime(13, 32)
HOLIDAY_CHECK_T = dtime(9, 45)

YAHOO_INTERVAL_SEC = 300


def now_tw():
    return datetime.now(TZ)


def at_today(t):
    return datetime.combine(now_tw().date(), t, tzinfo=TZ)


def sleep_until(target):
    while True:
        remain = (target - now_tw()).total_seconds()
        if remain <= 0:
            return
        time.sleep(min(remain, 30))


def has_today_data():
    """
    用台積電 1 分 K 檢查資料日期。

    True：有今天資料。
    False：最新資料早於今天。
    None：查詢失敗、空資料或日期異常，無法判斷。
    """
    try:
        df = yf.download(
            "2330.TW",
            period="1d",
            interval="1m",
            progress=False,
        )

        if df is None or df.empty:
            print("⚠️ 休市檢查未取得資料，本次無法判斷。")
            return None

        last_ts = df.index[-1]
        if last_ts.tzinfo is None:
            print("⚠️ 休市檢查資料缺少時區，本次無法判斷。")
            return None

        latest_date = last_ts.tz_convert(TZ).date()
        today = now_tw().date()

        print(f"📅 休市檢查：最新K棒日期 {latest_date}")

        if latest_date == today:
            return True
        if latest_date < today:
            return False
        return None

    except Exception:
        print("⚠️ 休市檢查失敗，繼續循環。")
        traceback.print_exc()
        return None


def main():
    n = now_tw()

    if n.weekday() >= 5:
        print("週末，結束。")
        return

    close_at = at_today(CLOSE_T)

    if n >= close_at:
        print("已過收盤時間，結束。")
        return

    open_at = at_today(OPEN_T) + timedelta(seconds=10)

    if n < open_at:
        print(f"⏳ 等待開盤，預計 {open_at:%H:%M:%S} 開始掃描。")
        sleep_until(open_at)

    print(f"✅ 開始循環掃描，台北時間 {now_tw():%H:%M:%S}")
    print("📡 Yahoo 1分K模式，每輪開始時間間隔5分鐘。")

    holiday_checked = False

    while now_tw() < close_at:
        if not holiday_checked and now_tw().time() >= HOLIDAY_CHECK_T:
            today_status = has_today_data()

            if today_status is False:
                print("09:45後最新資料仍早於今天，判斷為休市，結束。")
                return

            # 無法判斷時，留待下一輪再次檢查。
            holiday_checked = today_status is True

        if now_tw() >= close_at:
            break

        started = now_tw()
        started_monotonic = time.monotonic()

        print(f"\n=== 掃描 {started:%H:%M:%S} ===")

        try:
            bot.main()
        except Exception:
            print("⚠️ 本輪掃描失敗，保留循環，下一輪再試。")
            traceback.print_exc()

        elapsed = time.monotonic() - started_monotonic
        print(f"⏱ 本輪耗時 {elapsed:.0f} 秒")

        if now_tw() >= close_at:
            break

        remaining = max(YAHOO_INTERVAL_SEC - elapsed, 0)
        target = now_tw() + timedelta(seconds=remaining)

        if target >= close_at:
            break

        if remaining > 0:
            print(f"⏳ 下一輪預計 {target:%H:%M:%S}")
            sleep_until(target)
        else:
            print("↪️ 本輪已超過5分鐘，直接進行下一輪。")

    print("收盤，結束。")


if __name__ == "__main__":
    main()
