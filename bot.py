import os
import csv
import json
import time
import logging
import requests
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo
import warnings
warnings.filterwarnings("ignore")  # 隱藏 yfinance 的煩人警告

# 關掉yfinance自己印的錯誤訊息(例如 "$XXXX.TW: No data found, symbol may be delisted")。
# 這類訊息在我們的流程裡是「正常會出現的中間過程」——因為我們會先用.TW試一輪，
# 上櫃股票試.TW失敗是預期中的事，失敗的會在階段二改用.TWO重試，不是真的資料有問題。
# 關掉這些雜訊，改成流程最後印一行清楚的總結，才不會誤導人以為股票真的下市或壞掉了。
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

TOKEN = os.getenv('TELEGRAM_TOKEN')
CHAT_IDS = (os.getenv('TELEGRAM_CHAT_ID') or '').split(',')

# ============ 參數設定 ============
TOTAL_SESSION_MINUTES = 270          # 台股一天交易時間 9:00-13:30 = 270 分鐘
BASE_TURNOVER_FULLDAY = 2.0          # 策略一：全天基準金額門檻(億)
BASE_TURNOVER_FULLDAY_S2 = 1.5       # 策略二：全天基準金額門檻(億)
MIN_TURNOVER_FLOOR = 0.15            # 開盤極早期的最低門檻，避免噪音
QUIET_PCT_LOW = -1.5                 # 策略三：價格變動區間(避免跟策略一重疊)
QUIET_PCT_HIGH = 2.5
AVG_VOLUME_LOOKBACK_DAYS = "20d"

# ---- 大戶(法人)買進條件 ----
CHIP_FILTER_ENABLED = True       # 設成 False 就關閉這個條件
CHIP_LOOKBACK_DAYS = 3           # 看前幾個交易日(不含今天：證交所/櫃買的法人數字收盤後才公布，盤中拿不到今天的)
CHIP_MIN_NET_LOTS = 0            # 外資+投信 近N日合計淨買超「大於」幾張，才算大戶有買進
CHIP_CACHE_PATH = "chip_cache.json"
CHIP_RETRY_SECONDS = 900         # 法人資料取得失敗時，最短間隔多久才重試
CHIP = {'date': None, 'net': {'TWSE': {}, 'TPEX': {}}, 'ok': {'TWSE': False, 'TPEX': False},
        'dates': [], 'last_try': 0.0}
CACHE_PATH = "avg_volume_cache.csv"
ALERT_STATE_PATH = "alert_state.csv"     # 跨執行的「已發送」紀錄：同一檔股票今天發過一次就不再發

# ---- 交易時間判斷 ----
TAIWAN_TZ = ZoneInfo("Asia/Taipei")
MARKET_OPEN_TIME = dtime(9, 0)
MARKET_CLOSE_TIME = dtime(13, 35)
SUFFIX_CACHE = {}  # {股票代號: '.TW' 或 '.TWO'}，循環執行時第一輪找出後就不用再重試


def is_market_hours():
    """判斷現在是否在台股交易時間內(週一~週五 9:00-13:30，台北時區)。不含國定假日判斷。"""
    now_tw = datetime.now(TAIWAN_TZ)
    if now_tw.weekday() >= 5:  # 5=Sat, 6=Sun
        return False
    return MARKET_OPEN_TIME <= now_tw.time() <= MARKET_CLOSE_TIME

# ---- 全策略分級表：用「近20日平均每日成交金額(億)」當流動性/股本大小的替代指標 ----
# 大型權值股不需要噴到4%才算異常，噴2.5%配合明顯放量就已經是很大的訊號，門檻放寬讓警報更早出現
# 小型冷門股維持嚴格門檻(甚至排除策略三)，避免雜訊亂觸發
#
# 策略二(波段轉強)鎖定的是「trend up」這個現象本身，不管前面有沒有跌、跌多深：
# 只要zigzag確認出一個新的起漲低點(不管那個低點是正是負，甚至可能只是回檔而已)、
# 且從那個低點反彈的幅度夠了，就算一個新的上升波段，發送一次；不管後面漲到哪裡都算同一波，
# 直到真的又拉回、確認新的一波，才會再發下一次。
#
# 每一列: (該級距最低平均日成交金額門檻,
#          策略一漲幅門檻%, 策略二反彈門檻%,
#          策略三相對量能倍數門檻, 策略三現在金額最低門檻(億), 策略三現在最低張數)
SIZE_TIERS = [
    (5.0, 2.5, 2.5, 1.8, 0.5, 50),   # 大型活躍股：門檻最寬鬆，噴出來就早點通知
    (0.5, 3.5, 3.0, 2.5, 0.3, 80),   # 中型股：中等門檻
    # 平均日成交金額 < 0.5億 的小型冷門股 -> 用最後的預設值(見 get_size_tier)，且策略三直接排除
]
DEFAULT_BREAKOUT_PCT = 4.0     # 找不到分級資料(如新股)或小型股：維持原本嚴格門檻
DEFAULT_REBOUND_PCT = 3.5
# 今天已經發過訊號的股票，要「先前拉回至少這麼多百分點」再重新轉強，才算值得再發第二次的新一波；
# 上升途中只是小小往下(例如1~2個百分點)就又上去，還是同一段行情，不重複發報。
WAVE_REALERT_MIN_PULLBACK_PP = 3.0
WAVE_REVERSAL_PP = 1.0  # zigzag轉折確認門檻：只要出現至少這麼多「百分點」的真實反轉，就算轉折點成立

# 台股市值前500監控池
WATCH_STOCKS = {
    '1101': '台泥', '1102': '亞泥', '1103': '嘉泥', '1104': '環泥', '1108': '幸福',
    '1109': '信大', '1110': '東泥', '1201': '味全', '1203': '味王', '1210': '大成',
    '1215': '卜蜂', '1216': '統一', '1217': '愛之味', '1218': '泰山', '1219': '福壽',
    '1220': '台榮', '1225': '福懋油', '1227': '佳格', '1229': '聯華', '1231': '聯華食',
    '1232': '大統益', '1234': '黑松', '1235': '興泰', '1236': '宏亞', '1301': '台塑',
    '1303': '南亞', '1304': '台聚', '1305': '華夏', '1307': '三芳', '1308': '亞聚',
    '1309': '台達化', '1310': '台苯', '1312': '國喬', '1313': '聯成', '1314': '中石化',
    '1315': '達新', '1316': '上曜', '1319': '東陽', '1321': '大洋', '1323': '永裕',
    '1324': '地球', '1325': '恆大', '1326': '台化', '1337': '再生-KY', '1338': '廣華',
    '1339': '昭輝', '1340': '勝悅-KY', '1402': '遠東新', '1409': '新纖', '1413': '宏洲',
    '1414': '東和', '1417': '嘉裕', '1434': '福懋', '1436': '華友聯', '1437': '勤益控',
    '1438': '三地開發', '1440': '南紡', '1441': '大東', '1442': '名軒', '1443': '立益',
    '1444': '力麗', '1445': '大宇', '1447': '力鵬', '1449': '佳和', '1451': '年興',
    '1452': '宏益', '1455': '集盛', '1456': '怡華', '1457': '宜進', '1460': '宏遠',
    '1463': '強盛', '1464': '得力', '1465': '偉全', '1466': '聚隆', '1467': '南緯',
    '1468': '昶和', '1472': '三洋實業', '1473': '台南', '1474': '弘裕', '1475': '業旺',
    '1476': '儒鴻', '1477': '聚陽', '1503': '士電', '1504': '東元', '1506': '正道',
    '1507': '永大', '1513': '中興電', '1514': '亞力', '1515': '力山', '1517': '利奇',
    '1519': '華城', '1521': '大億', '1522': '堤維西', '1524': '耿鼎', '1525': '江申',
    '1526': '日馳', '1527': '鑽全', '1528': '恩德', '1530': '亞崴', '1531': '高林股',
    '1532': '勤美', '1533': '車王電', '1535': '中宇', '1536': '和大', '1537': '廣隆',
    '1538': '正峰', '1540': '喬福', '1541': '錩泰', '1558': '伸興', '1560': '中砂',
    '1568': '倉佑', '1582': '信錦', '1584': '精剛', '1590': '亞德客-KY', '1597': '直得',
    '1603': '華電', '1605': '華新', '1608': '華榮', '1609': '大亞', '1611': '中電',
    '1612': '宏泰', '1614': '三洋電', '1615': '大山', '1616': '億泰', '1617': '榮星',
    '1618': '合機', '1702': '南僑', '1707': '葡萄王', '1708': '東鹼', '1710': '東聯',
    '1711': '永光', '1712': '興農', '1714': '和桐', '1717': '長興', '1718': '中纖',
    '1720': '生達', '1721': '三晃', '1722': '台肥', '1723': '中碳', '1725': '元禎',
    '1727': '中華化', '1730': '花仙子', '1731': '美吾華', '1732': '毛寶', '1734': '杏輝',
    '1735': '日勝化', '1736': '喬山', '1752': '南光', '1760': '寶齡富錦', '1773': '勝一',
    '1783': '和康生', '1785': '光洋科', '1786': '科妍', '1789': '神隆', '1795': '美時',
    '1802': '台玻', '1805': '寶徠', '1808': '潤隆', '1810': '和成', '1815': '富喬',
    '1904': '正隆', '1905': '華紙', '1907': '永豐餘', '1909': '榮成', '2002': '中鋼',
    '2006': '東和鋼鐵', '2007': '燁興', '2009': '第一銅', '2014': '中鴻', '2015': '豐興',
    '2017': '官田鋼', '2020': '美亞', '2023': '燁輝', '2027': '大成鋼', '2028': '威致',
    '2031': '新光鋼', '2033': '佳大', '2034': '允強', '2038': '海光', '2049': '上銀',
    '2059': '川湖', '2062': '橋椿', '2101': '南港', '2103': '台橡', '2105': '正新',
    '2106': '建大', '2107': '厚生', '2201': '裕隆', '2204': '中華', '2206': '三陽工業',
    '2207': '和泰車', '2208': '台船', '2227': '裕日車', '2231': '為升', '2233': '宇隆',
    '2236': '百達-KY', '2239': '英利-KY', '2241': '艾姆勒', '2243': '宏旭-KY', '2247': '汎德永業',
    '2250': 'IKKA-KY', '2301': '光寶科', '2302': '麗正', '2303': '聯電', '2305': '全友',
    '2308': '台達電', '2312': '金寶', '2313': '華通', '2316': '楠梓電', '2317': '鴻海',
    '2323': '中環', '2324': '仁寶', '2327': '國巨', '2328': '廣宇', '2329': '華泰',
    '2330': '台積電', '2331': '精英', '2332': '友訊', '2337': '旺宏', '2338': '光罩',
    '2340': '台亞', '2342': '茂矽', '2344': '華邦電', '2345': '智邦', '2347': '聯強',
    '2348': '海悅', '2349': '錸德', '2351': '順天', '2352': '佳世達', '2353': '宏碁',
    '2354': '鴻準', '2355': '敬鵬', '2356': '英業達', '2357': '華碩', '2359': '所羅門',
    '2360': '致茂', '2362': '藍天', '2363': '矽統', '2365': '昆盈', '2367': '燿華',
    '2368': '金像電', '2376': '技嘉', '2377': '微星', '2379': '瑞昱', '2380': '虹光',
    '2382': '廣達', '2383': '台光電', '2385': '群光', '2388': '威盛', '2392': '正崴',
    '2393': '億光', '2395': '研華', '2401': '凌陽', '2402': '毅嘉', '2404': '漢唐',
    '2405': '浩鑫', '2406': '國碩', '2408': '南亞科', '2409': '友達', '2412': '中華電',
    '2413': '環科', '2414': '精技', '2417': '圓剛', '2420': '新巨', '2421': '建準',
    '2423': '固緯', '2426': '鼎元', '2427': '三商電', '2428': '興勤', '2429': '銘旺科',
    '2433': '互盛電', '2436': '偉詮電', '2439': '美律', '2442': '新美齊', '2449': '京元電子',
    '2450': '神腦', '2451': '創見', '2453': '凌群', '2454': '聯發科', '2455': '全新',
    '2457': '飛宏', '2458': '義隆', '2464': '盟立', '2465': '麗臺', '2467': '志聖',
    '2471': '資通', '2472': '立隆電', '2474': '可成', '2476': '鉅祥', '2477': '美隆電',
    '2478': '大毅', '2481': '強茂', '2482': '連宇', '2485': '兆赫', '2486': '一詮',
    '2488': '漢平', '2489': '瑞軒', '2492': '華新科', '2495': '普安', '2498': '宏達電',
    '2501': '國建', '2504': '國產', '2505': '國揚', '2506': '太設', '2511': '太子',
    '2515': '中工', '2516': '新建', '2520': '冠德', '2524': '京城', '2528': '皇普',
    '2534': '宏盛', '2535': '達欣工', '2536': '宏普', '2537': '聯上發', '2542': '興富發',
    '2545': '皇翔', '2547': '日勝生', '2548': '華固', '2597': '潤弘', '2601': '益航',
    '2603': '長榮', '2605': '新興', '2606': '裕民', '2607': '榮運', '2608': '嘉里大榮',
    '2609': '陽明', '2610': '華航', '2611': '志信', '2612': '中航', '2615': '萬海',
    '2618': '長榮航', '2630': '亞航', '2633': '台灣高鐵', '2634': '漢翔', '2636': '台驊控股',
    '2637': '慧洋-KY', '2642': '宅配通', '2645': '長榮航太', '2702': '華園', '2704': '國賓',
    '2705': '六福', '2707': '晶華', '2723': '美食-KY', '2727': '王品', '2731': '雄獅',
    '2739': '寒舍', '2748': '雲品', '2801': '彰銀', '2809': '京城銀', '2812': '台中銀',
    '2820': '華票', '2823': '中壽', '2834': '臺企銀', '2838': '聯邦銀', '2845': '遠東銀',
    '2849': '安泰銀', '2850': '新產', '2851': '中再保', '2852': '第一保', '2855': '統一證',
    '2867': '三商壽', '2880': '華南金', '2881': '富邦金', '2882': '國泰金', '2883': '開發金',
    '2884': '玉山金', '2885': '元大金', '2886': '兆豐金', '2887': '台新新光金', '2888': '新光金',
    '2889': '國票金', '2890': '永豐金', '2891': '中信金', '2892': '第一金', '2897': '王道銀行',
    '2903': '遠百', '2905': '三商', '2906': '高林', '2908': '特力', '2912': '統一超',
    '2913': '農林', '2915': '潤泰全', '2923': '鼎固-KY', '3002': '歐格', '3003': '健和興',
    '3004': '豐達科', '3005': '神基', '3006': '晶豪科', '3008': '大立光', '3010': '華立',
    '3013': '晟銘電', '3014': '聯陽', '3015': '全漢', '3016': '嘉晶', '3017': '奇鋐',
    '3018': '隆銘綠能', '3019': '亞光', '3022': '威強電', '3023': '信邦', '3024': '憶聲',
    '3025': '星通', '3026': '禾伸堂', '3027': '盛達', '3028': '增你強', '3030': '德律',
    '3032': '偉訓', '3034': '聯詠', '3035': '智原', '3036': '文曄', '3037': '欣興',
    '3038': '全台', '3042': '晶技', '3044': '健鼎', '3045': '台灣大', '3048': '益登',
    '3049': '精金', '3055': '蔚華科', '3056': '總太', '3058': '立德', '3060': '銘異',
    '3062': '建漢', '3090': '日電貿', '3092': '鴻碩', '3105': '穩懋', '3131': '弘塑',
    '3163': '波若威', '3169': '亞信', '3189': '景碩', '3211': '順達', '3227': '原相',
    '3228': '金麗科', '3231': '緯創', '3234': '光環', '3260': '威剛', '3264': '欣銓',
    '3265': '台星科', '3272': '東碩', '3289': '宜特', '3311': '閎暉', '3312': '弘憶股',
    '3317': '尼克森', '3321': '同泰', '3324': '雙鴻', '3338': '泰碩', '3357': '臺慶科',
    '3362': '先進光', '3363': '上詮', '3374': '精材', '3380': '明泰', '3406': '玉晶光',
    '3443': '創意', '3450': '聯鈞', '3481': '群創', '3483': '力致', '3491': '昇達科',
    '3504': '揚明光', '3508': '位速', '3515': '華擎', '3529': '力旺', '3532': '台勝科',
    '3533': '嘉澤', '3535': '晶彩科', '3537': '堡達', '3550': '聯穎', '3563': '牧德',
    '3576': '聯合再生', '3583': '辛耘', '3588': '通嘉', '3596': '智易', '3605': '宏致',
    '3615': '安可', '3624': '光頡', '3625': '西勝', '3661': '世芯-KY', '3665': '貿聯-KY',
    '3673': 'TPK-KY', '3680': '家登', '3702': '大聯大', '3706': '神達', '3707': '漢磊',
    '3711': '日月光投控', '3715': '定穎投控', '4105': '東洋', '4108': '懷特', '4128': '中天',
    '4132': '國鼎', '4142': '國光生', '4147': '中裕', '4164': '承業醫', '4190': '佐登-KY',
    '4746': '台耀', '4743': '合一', '4763': '材料-KY', '4766': '南寶', '4772': '台特化',
    '4904': '遠傳', '4908': '前鼎', '4912': '聯德控股-KY', '4915': '致伸', '4916': '事欣科',
    '4919': '新唐', '4931': '新盛力', '4938': '和碩', '4939': '亞電', '4949': '有成精密',
    '4952': '凌通', '4958': '臻鼎-KY', '4966': '譜瑞-KY', '4967': '十銓', '4971': 'IET-KY',
    '4973': '廣穎', '4977': '眾達-KY', '4979': '華星光', '4989': '榮科', '4991': '環宇-KY',
    '5009': '榮剛', '5203': '訊連', '5222': '全訊', '5236': '凌陽創新', '5269': '祥碩',
    '5274': '信驊', '5289': '宜鼎', '5299': '杰力', '5309': '系統電', '5314': '世紀*',
    '5340': '建榮', '5347': '世界', '5351': '鈺創', '5371': '中光電', '5388': '中磊',
    '5425': '台半', '5434': '崇越', '5443': '均豪', '5471': '松翰', '5474': '聰泰',
    '5475': '德宏', '5483': '中美晶', '5484': '慧友', '5519': '隆大', '5521': '工信',
    '5522': '遠雄', '5536': '聖暉*', '5607': '遠雄港', '5608': '四維航', '5871': '中租-KY',
    '5876': '上海商銀', '5904': '寶雅', '6005': '群益證', '6015': '宏遠證', '6023': '元大期',
    '6108': '競國', '6121': '新普', '6125': '廣運', '6129': '普誠', '6138': '茂達',
    '6139': '亞翔', '6141': '柏承', '6166': '凌華', '6173': '信昌電', '6182': '合晶',
    '6187': '萬潤', '6196': '帆宣', '6197': '佳必琪', '6202': '盛群', '6207': '雷科',
    '6213': '聯茂', '6217': '中探針', '6223': '旺矽', '6230': '尼得科超眾', '6233': '旺玖',
    '6239': '力成', '6245': '立端', '6257': '矽格', '6269': '台郡', '6271': '同欣電',
    '6274': '台燿', '6275': '元山', '6282': '康舒', '6285': '啟碁', '6290': '良維',
    '6415': '矽力-KY', '6423': '億而得', '6435': '大中', '6438': '迅得', '6442': '光聖',
    '6451': '訊芯-KY', '6462': '神盾', '6488': '環球晶', '6510': '精測', '6515': '穎崴',
    '6526': '達發', '6530': '創威', '6531': '愛普*', '6533': '晶心科', '6548': '長科*',
    '6558': '興能高', '6560': '欣普羅', '6579': '研揚', '6584': '南俊國際', '6640': '均華',
    '6643': 'M31', '6664': '群翊', '6667': '信紘科', '6669': '緯穎', '6679': '鈺太',
    '6691': '洋基工程', '6695': '芯鼎', '6706': '惠特', '6715': '嘉基', '6719': '力智',
    '6739': '竹陞科技', '6742': '澤米', '6753': '龍德造船', '6770': '力積電', '6781': 'AES-KY',
    '6805': '富世達', '6806': '森崴能源', '6829': '千附精密', '6862': '三集瑞-KY', '6922': '宸曜',
    '6944': '兆聯實業', '6983': '華洋精機', '7402': '邑錡', '7709': '榮田', '7717': '萊德光電-KY',
    '7734': '印能科技', '7750': '新代', '7769': '鴻勁', '7772': '耀穎', '7786': '東方風能',
    '7788': '松川精密', '7795': '長廣', '7822': '倍利科', '7828': '創新服務', '7856': '漢測',
    '7861': '貝爾威勒', '8027': '鈦昇', '8028': '昇陽半導體', '8033': '雷虎', '8039': '台虹',
    '8040': '九暘', '8046': '南電', '8054': '安國', '8058': '耐特', '8064': '東捷',
    '8069': '元太', '8081': '致新', '8103': '瀚荃', '8119': '公信', '8155': '博智',
    '8210': '勤誠', '8222': '寶一', '8227': '巨有科技', '8234': '新漢', '8255': '朋程',
    '8261': '富鼎', '8271': '宇瞻', '8291': '尚茂', '8299': '群聯', '8358': '金居',
    '8374': '羅昇', '8996': '高力', '9103': '美德醫療-DR', '9904': '寶成', '9907': '統一實',
    '9910': '豐泰', '9914': '美利達', '9917': '中保科', '9921': '巨大', '9938': '百和',
    '9958': '世紀鋼'
}


def send_telegram(message):
    """回傳 True 代表所有 chat_id 都送成功。"""
    url = f'https://api.telegram.org/bot{TOKEN}/sendMessage'
    all_ok = True
    sent_any_target = False
    for chat_id in CHAT_IDS:
        chat_id = chat_id.strip()
        if not chat_id:
            continue
        sent_any_target = True
        payload = {'chat_id': chat_id, 'text': message, 'parse_mode': 'HTML'}
        try:
            resp = requests.post(url, data=payload, timeout=10)
            if resp.status_code != 200:
                print(f"❌ Telegram發送失敗 (chat_id={chat_id})："
                      f"狀態碼={resp.status_code}，回應內容={resp.text}")
                all_ok = False
            else:
                print(f"✅ Telegram發送成功 (chat_id={chat_id})")
        except Exception as e:
            print(f"❌ Telegram發送時發生例外 (chat_id={chat_id})：{e}")
            all_ok = False
    if not sent_any_target:
        print("❌ 沒有設定任何有效的 TELEGRAM_CHAT_ID")
        return False
    return all_ok


def send_alerts_chunked(header, alerts, limit=3500):
    """Telegram單則上限4096字，超過會整則被拒絕，所以把警報切成多則(每則不超過limit字)。
    只有全部送成功才回傳True。"""
    chunks = []
    cur = header
    for a in alerts:
        if cur != header and len(cur) + 2 + len(a) > limit:
            chunks.append(cur)
            cur = header
        cur += ("" if cur == header else "\n\n") + a
    chunks.append(cur)
    return all([send_telegram(c) for c in chunks])


def load_alert_state(path):
    """讀取跨執行的發送紀錄：{股票代號: {'severity': 已發過的最重要等級, 'wave_trough_key': 最近一次發送的波段起漲低點識別碼}}。
    隔天自動失效。等級數字越小代表訊號越重要：1=波段轉強, 2=強勢突破, 3=異常量能。
    wave_trough_key 用來辨識「這是不是同一個波段」：只要zigzag找到的起漲低點時間戳跟上次不一樣，
    就代表是全新的一波，可以再發一次，不受發過幾次限制。"""
    today = datetime.now(TAIWAN_TZ).strftime("%Y-%m-%d")
    state = {}
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                for row in reader:
                    if len(row) != 4:
                        continue
                    date_str, code, severity, wave_trough_key = row
                    if date_str == today:
                        state[code] = {
                            'severity': int(severity),
                            'wave_trough_key': wave_trough_key if wave_trough_key else None,
                        }
        except Exception:
            pass
    return state


def save_alert_state(path, state):
    today = datetime.now(TAIWAN_TZ).strftime("%Y-%m-%d")
    try:
        with open(path, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            for code, info in state.items():
                key = info.get('wave_trough_key') or ''
                writer.writerow([today, code, info['severity'], key])
    except Exception:
        pass


def load_avg_volume_cache(path):
    """讀取當日快取，若不是今天的就視為過期。每筆存: 日均量(股), 日均成交金額(億)"""
    today = datetime.now(TAIWAN_TZ).strftime("%Y-%m-%d")
    cache = {}
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                for row in reader:
                    if len(row) != 4:
                        continue
                    date_str, code, avg_vol, avg_turnover = row
                    if date_str == today:
                        cache[code] = (float(avg_vol), float(avg_turnover))
        except Exception:
            pass
    return cache


def save_avg_volume_cache(path, cache):
    today = datetime.now(TAIWAN_TZ).strftime("%Y-%m-%d")
    try:
        with open(path, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            for code, (avg_vol, avg_turnover) in cache.items():
                writer.writerow([today, code, avg_vol, avg_turnover])
    except Exception:
        pass


def fetch_avg_volumes(ticker_suffix_map, cache_path):
    """
    取得每檔股票近20日「日均成交量(股)」與「日均成交金額(億)」。
    後者用來當作股本/市值大小的替代指標，避免冷門小型股用相對量能亂觸發。
    有當日快取就不重打 API，避免每次掃描都重抓 500 檔的日線。
    """
    cache = load_avg_volume_cache(cache_path)
    missing = {code: suf for code, suf in ticker_suffix_map.items() if code not in cache}

    if missing:
        tickers = [f"{code}{suf}" for code, suf in missing.items()]
        hist = chunked_download(tickers, period=AVG_VOLUME_LOOKBACK_DAYS, interval="1d")

        for code, suf in missing.items():
            ticker = f"{code}{suf}"
            try:
                if ticker not in hist:
                    continue
                data = hist[ticker]
                vol_series = data['Volume']
                close_series = data['Close']
                daily = pd.DataFrame({'vol': vol_series, 'close': close_series}).dropna()
                daily = daily[daily['vol'] > 0]
                if len(daily) > 0:
                    avg_vol = float(daily['vol'].mean())
                    avg_turnover_billion = float((daily['vol'] * daily['close']).mean()) / 100000000
                    cache[code] = (avg_vol, avg_turnover_billion)
            except Exception:
                continue

        save_avg_volume_cache(cache_path, cache)

    return cache


def find_current_wave_trough(pct_series, reversal_pp):
    """
    簡化版zigzag：在「相對開盤漲跌幅%」序列上，用reversal_pp(百分點)當轉折確認門檻，
    找出「目前正在進行的上升波段」是從哪個低點(trough)開始起漲的。

    邏輯：價格從某個高點回落超過reversal_pp個百分點，才確認那個高點是真正的波段高點、
    現在進入下降波段；同理，從某個低點反彈超過reversal_pp個百分點，才確認那是真正的
    波段低點、現在進入上升波段。持續更新極值，直到反向超過門檻才翻轉。

    回傳 (目前是否處於上升波段, 該波段起漲低點的漲跌幅%, 該低點在序列中的索引(用來跨執行辨識是否為新波段),
          起漲前的回檔幅度pp)。
    回檔幅度 = 「這個低點之前的波段高點」減「這個低點」；如果這一波是從開盤後的第一個低點直接往上
    (前面根本沒有高點)，回檔幅度是None。
    如果現在不是處於上升波段(還在下跌或盤整)，回傳 (False, None, None, None)。
    """
    if len(pct_series) < 3:
        return False, None, None, None

    trend = None  # None(尚未確認方向) / 'up' / 'down'
    running_high = pct_series.iloc[0]
    running_high_idx = pct_series.index[0]
    running_low = pct_series.iloc[0]
    running_low_idx = pct_series.index[0]
    extreme_val = pct_series.iloc[0]
    extreme_idx = pct_series.index[0]
    trough_val = None
    trough_idx = None
    peak_val = None       # 目前這段下降波段「之前」的波段高點
    pullback_pp = None    # 目前這個起漲低點之前，從波段高點拉回了幾個百分點

    for idx, val in pct_series.items():
        if trend is None:
            # 尚未確認方向前，同時追蹤目前為止的最高點與最低點，
            # 一旦兩者差距超過門檻，用「誰比較晚出現」判斷方向剛確認成上升還是下降。
            if val > running_high:
                running_high, running_high_idx = val, idx
            if val < running_low:
                running_low, running_low_idx = val, idx

            if running_high - running_low >= reversal_pp:
                if running_high_idx > running_low_idx:
                    trend = 'up'
                    trough_val, trough_idx = running_low, running_low_idx
                    extreme_val, extreme_idx = running_high, running_high_idx
                    pullback_pp = None
                else:
                    trend = 'down'
                    peak_val = running_high
                    extreme_val, extreme_idx = running_low, running_low_idx
            continue

        if trend == 'up':
            if val >= extreme_val:
                extreme_val, extreme_idx = val, idx
            elif extreme_val - val >= reversal_pp:
                trend = 'down'
                peak_val = extreme_val   # 剛結束的上升波段高點，先記下來
                extreme_val, extreme_idx = val, idx
        else:  # trend == 'down'
            if val <= extreme_val:
                extreme_val, extreme_idx = val, idx
            elif val - extreme_val >= reversal_pp:
                trend = 'up'
                trough_val, trough_idx = extreme_val, extreme_idx
                pullback_pp = (peak_val - extreme_val) if peak_val is not None else None
                extreme_val, extreme_idx = val, idx

    if trend == 'up' and trough_idx is not None:
        return True, trough_val, trough_idx, pullback_pp
    return False, None, None, None


def get_size_tier(avg_turnover_billion):
    """
    依「近20日平均每日成交金額(億)」判斷分級。
    回傳 (漲幅門檻%, 反彈門檻%, 相對量能倍數門檻或None, 金額最低門檻或None, 最低張數或None)。
    後三項為None代表這檔股票太冷門，不適用策略三。
    找不到資料(avg_turnover_billion is None，例如新股)時，回傳最嚴格的預設值。
    """
    if avg_turnover_billion is None:
        return (DEFAULT_BREAKOUT_PCT, DEFAULT_REBOUND_PCT, None, None, None)

    for min_avg_turnover, breakout_pct, rebound_pct, relative_trigger, min_turnover_now, min_lots in SIZE_TIERS:
        if avg_turnover_billion >= min_avg_turnover:
            return (breakout_pct, rebound_pct, relative_trigger, min_turnover_now, min_lots)

    # 小型冷門股：策略一維持預設嚴格門檻，策略三直接排除
    return (DEFAULT_BREAKOUT_PCT, DEFAULT_REBOUND_PCT, None, None, None)


def compute_metrics(df, open_price, avg_daily_volume, avg_turnover_billion):
    """算出單一時間點(某個切片長度)的所有指標，past/now共用同一套邏輯"""
    current_price = float(df['Close'].iloc[-1])
    volume_lots = int(df['Volume'].sum() / 1000)
    # 用最後一根K棒的實際時間算已開盤分鐘數，而不是K棒數量：
    # 冷門股沒成交就沒有1分K，K棒數會偏少，導致時間比例被低估、金額門檻變太低。
    elapsed_minutes = max(len(df), 1)
    try:
        last_ts = df.index[-1].tz_convert(TAIWAN_TZ)
        open_dt = last_ts.replace(hour=9, minute=0, second=0, microsecond=0)
        elapsed_minutes = max(int((last_ts - open_dt).total_seconds() // 60) + 1, 1)
    except Exception:
        pass

    percent_change = ((current_price - open_price) / open_price) * 100
    turnover_billion = (current_price * volume_lots * 1000) / 100000000

    # 時間校正後的金額門檻：早盤門檻自動放低，但設一個下限避免噪音
    time_ratio = min(elapsed_minutes / TOTAL_SESSION_MINUTES, 1.0)
    dyn_threshold_1 = max(BASE_TURNOVER_FULLDAY * time_ratio, MIN_TURNOVER_FLOOR)
    dyn_threshold_2 = max(BASE_TURNOVER_FULLDAY_S2 * time_ratio, MIN_TURNOVER_FLOOR)

    # 依股本/流動性大小分級：大型股門檻放寬，早點抓到訊號；小型股維持嚴格避免噪音
    breakout_pct, rebound_pct, relative_trigger, min_turnover_now, min_lots = get_size_tier(avg_turnover_billion)

    strategy_1 = (percent_change >= breakout_pct and turnover_billion >= dyn_threshold_1)

    # 策略二：波段轉強，用zigzag找出「目前這一波上漲」是從哪個低點起漲的。
    # 這裡只算「型態符不符合」(有沒有真的在漲、反彈幅度夠不夠、金額夠不夠)——
    # 重點是抓「反轉斜率本身」，不管現在價位漲回多高(平盤、翻正、甚至衝到漲停都算同一波)。
    # 至於這個起漲低點是不是「新的」(還沒發過的波段)、以及第一波是否夠深(接近跌停)，
    # 交給main()裡的alert_state去判斷，因為那需要跨執行的歷史紀錄才能比對。
    pct_series = (df['Close'] - open_price) / open_price * 100
    wave_in_uptrend, wave_trough_val, wave_trough_idx, wave_pullback_pp = find_current_wave_trough(pct_series, WAVE_REVERSAL_PP)
    wave_rebound_pp = (percent_change - wave_trough_val) if (wave_in_uptrend and wave_trough_val is not None) else 0
    strategy_2 = bool(
        wave_in_uptrend and wave_trough_val is not None and
        wave_rebound_pp >= rebound_pct and
        turnover_billion >= dyn_threshold_2
    )

    # 策略三：相對量能異常放大 —— 依股票大小分級套用不同門檻，小型冷門股直接排除
    # 注意：只能偵測「量比平常同時段多很多」，無法判斷買賣方向(吸籌或出貨)，需自行搭配籌碼面確認
    strategy_3 = False
    relative_volume = 0.0
    if avg_daily_volume and avg_daily_volume > 0 and relative_trigger is not None:
        expected_volume_now = avg_daily_volume * time_ratio
        actual_volume_shares = volume_lots * 1000
        relative_volume = actual_volume_shares / expected_volume_now if expected_volume_now > 0 else 0
        strategy_3 = (relative_volume >= relative_trigger and
                      QUIET_PCT_LOW <= percent_change <= QUIET_PCT_HIGH and
                      turnover_billion >= min_turnover_now and
                      volume_lots >= min_lots)

    return {
        'current_price': current_price,
        'percent_change': percent_change,
        'turnover_billion': turnover_billion,
        'volume_lots': volume_lots,
        'relative_volume': relative_volume,
        'strategy_1': strategy_1,
        'strategy_2': strategy_2,
        'strategy_3': strategy_3,
        'wave_trough_val': wave_trough_val,
        'wave_trough_idx': wave_trough_idx,
        'wave_pullback_pp': wave_pullback_pp,
    }


def chunked_download(tickers, chunk_size=50, max_retries=3, inter_chunk_delay=1.5,
                      empty_ratio_retry_threshold=0.5, **download_kwargs):
    """
    把一大串tickers拆成小批分開下載，原因有二：
    1. 避免一次抓太多檔(如500檔)在單一請求裡，因為批次過大、網路瞬斷導致整批被誤判成「查無資料」。
    2. Yahoo Finance對GitHub Actions這類共用IP的CI環境很容易觸發限流/封鎖，
       限流時yf.download常常「不會丟例外」，只是回傳裡一堆股票資料是空的——
       所以重試判斷不能只看有沒有丟例外，還要看「這批裡面有多少比例是空的」，
       空太多就當作這批其實是失敗的，重試(用越來越長的等待時間，降低再次被限流的機率)。
    回傳合併後的結果字典 {ticker: dataframe}。
    """
    merged = {}
    total_chunks = (len(tickers) + chunk_size - 1) // chunk_size

    for chunk_num, i in enumerate(range(0, len(tickers), chunk_size), start=1):
        chunk = tickers[i:i + chunk_size]
        attempt = 0
        chunk_data = None
        empty_count = len(chunk)

        while attempt <= max_retries:
            try:
                result = yf.download(chunk, group_by='ticker', progress=False, **download_kwargs)
                if len(chunk) == 1:
                    is_empty = result is None or result.empty
                    empty_count = 1 if is_empty else 0
                else:
                    empty_count = sum(
                        1 for t in chunk
                        if t not in result or result[t].dropna(how='all').empty
                    )
                chunk_data = result
                empty_ratio = empty_count / len(chunk)
                if empty_ratio <= empty_ratio_retry_threshold:
                    break  # 大部分都有抓到資料，這批算成功，不用重試
            except Exception:
                chunk_data = None

            attempt += 1
            if attempt <= max_retries:
                wait_seconds = 2 * attempt  # 遞增等待：2秒、4秒、6秒...降低連續被限流的機率
                time.sleep(wait_seconds)

        if chunk_data is not None:
            if len(chunk) == 1:
                if not (chunk_data is None or chunk_data.empty):
                    merged[chunk[0]] = chunk_data
            else:
                for ticker in chunk:
                    if ticker in chunk_data and not chunk_data[ticker].dropna(how='all').empty:
                        merged[ticker] = chunk_data[ticker]

        print(f"  批次 {chunk_num}/{total_chunks}：{len(chunk) - empty_count}/{len(chunk)} 檔成功"
              + (f"（重試{attempt}次後）" if attempt > 0 else ""))

        if chunk_num < total_chunks:
            time.sleep(inter_chunk_delay)  # 每批之間都稍微間隔，降低整體請求頻率避免觸發限流

    return merged


# ================== 大戶(法人)買進：資料抓取與判斷 ==================
# 資料來源：證交所 T86(上市)、櫃買中心(上櫃)的「三大法人買賣超日報」，全市場一次一個請求，免費免 token。
# 這裡的「大戶」定義為：外資(外陸資，不含外資自營商) + 投信，近 CHIP_LOOKBACK_DAYS 個交易日合計淨買超。
_HTTP_HEADERS = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36'}


def _to_int(s):
    s = str(s).replace(',', '').strip()
    if s in ('', '--', '-', 'X'):
        return 0
    try:
        return int(float(s))
    except ValueError:
        return 0


def _first_index(fields, cond):
    for i, h in enumerate(fields):
        if cond(h):
            return i
    return None


def parse_inst_table(fields, rows, market):
    """依「欄位名稱」找欄位位置(不寫死第幾欄，官方改欄位順序也不怕)，回傳 {代號: (外資買賣超股數, 投信買賣超股數)}。"""
    fields = [str(f).replace(' ', '').replace('　', '').replace('（', '(').replace('）', ')') for f in fields]
    if market == 'TWSE':
        i_code = _first_index(fields, lambda h: '證券代號' in h or h == '代號')
        i_for = _first_index(fields, lambda h: h.startswith('外陸資買賣超') or h.startswith('外資買賣超'))
        i_tru = _first_index(fields, lambda h: h.startswith('投信買賣超'))
    else:
        i_code = _first_index(fields, lambda h: h == '代號' or '證券代號' in h)
        i_for = _first_index(fields, lambda h: '外資及陸資(不含外資自營商)' in h and '買賣超' in h)
        i_tru = _first_index(fields, lambda h: h.startswith('投信') and '買賣超' in h)
    if None in (i_code, i_for, i_tru):
        raise ValueError(f"{market} 找不到需要的欄位，實際欄位：{fields}")
    out = {}
    for r in rows:
        code = str(r[i_code]).strip()
        if code:
            out[code] = (_to_int(r[i_for]), _to_int(r[i_tru]))
    return out


def _http_json(url, params):
    resp = requests.get(url, params=params, headers=_HTTP_HEADERS, timeout=20)
    resp.raise_for_status()
    return resp.json()


def fetch_twse_day(d):
    """某一天上市股票的法人買賣超。休市日回傳 {}；網路/格式錯誤會丟例外。"""
    j = _http_json("https://www.twse.com.tw/rwd/zh/fund/T86",
                   {'date': d.strftime('%Y%m%d'), 'selectType': 'ALLBUT0999', 'response': 'json'})
    stat = str(j.get('stat', ''))
    fields, data = j.get('fields'), j.get('data')
    if not data and j.get('tables'):
        fields, data = j['tables'][0].get('fields'), j['tables'][0].get('data')
    if stat.upper() == 'OK' and data:
        return parse_inst_table(fields, data, 'TWSE')
    if '沒有符合' in stat or '無符合' in stat or (stat.upper() == 'OK' and not data):
        return {}
    raise RuntimeError(f"證交所回應異常：{stat}")


def fetch_tpex_day(d):
    """某一天上櫃股票的法人買賣超。沒資料回傳 {}；網路/格式錯誤會丟例外。"""
    j = _http_json("https://www.tpex.org.tw/www/zh-tw/insti/dailyTrade",
                   {'type': 'Daily', 'sect': 'EW', 'date': d.strftime('%Y/%m/%d'), 'id': '', 'response': 'json'})
    tables = j.get('tables') or []
    if not tables or not tables[0].get('data'):
        return {}
    return parse_inst_table(tables[0]['fields'], tables[0]['data'], 'TPEX')


def load_chip_cache():
    try:
        with open(CHIP_CACHE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_chip_cache(cache):
    try:
        keep = sorted(cache.keys())[-15:]  # 過去的日期資料不會再變，留近15天就夠
        with open(CHIP_CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump({k: cache[k] for k in keep}, f, ensure_ascii=False)
    except Exception:
        pass


def _cache_get(cache, key, market):
    v = cache.get(key, {}).get(market)
    return None if v is None else {c: (int(a), int(b)) for c, (a, b) in v.items()}


def _cache_put(cache, key, market, data):
    cache.setdefault(key, {})[market] = {c: [a, b] for c, (a, b) in data.items()}


def ensure_chip_data():
    """每天只在第一輪(或失敗後每15分鐘)抓一次近N個交易日的法人資料，其餘輪次直接用記憶體裡的結果。"""
    if not CHIP_FILTER_ENABLED:
        return
    today = datetime.now(TAIWAN_TZ).date()
    all_ok = CHIP['ok']['TWSE'] and CHIP['ok']['TPEX']
    if CHIP['date'] == today and (all_ok or time.time() - CHIP['last_try'] < CHIP_RETRY_SECONDS):
        return
    CHIP['date'] = today
    CHIP['last_try'] = time.time()

    cache = load_chip_cache()
    ok = {'TWSE': True, 'TPEX': True}
    dates, twse_days, tpex_days = [], {}, {}

    # 上市：往前找出最近 N 個「有開盤」的交易日(休市日官方回傳空資料，自動跳過)
    try:
        d, scanned = today, 0
        while len(dates) < CHIP_LOOKBACK_DAYS and scanned < 20:
            d -= timedelta(days=1)
            scanned += 1
            if d.weekday() >= 5:
                continue
            key = d.isoformat()
            data = _cache_get(cache, key, 'TWSE')
            if data is None:
                data = fetch_twse_day(d)
                _cache_put(cache, key, 'TWSE', data)
                time.sleep(1.5)  # 證交所對連續請求很敏感，放慢一點
            if data:
                dates.append(d)
                twse_days[d] = data
        if not dates:
            raise RuntimeError("找不到任何一個交易日的上市法人資料")
    except Exception as e:
        ok['TWSE'] = False
        print(f"⚠️ 上市法人資料取得失敗：{e}")

    # 上櫃：用同樣的交易日
    if dates:
        try:
            for d in dates:
                key = d.isoformat()
                data = _cache_get(cache, key, 'TPEX')
                if not data:
                    data = fetch_tpex_day(d)
                    if not data:
                        raise RuntimeError(f"{key} 櫃買中心沒有回傳資料")
                    _cache_put(cache, key, 'TPEX', data)
                    time.sleep(1.5)
                tpex_days[d] = data
        except Exception as e:
            ok['TPEX'] = False
            print(f"⚠️ 上櫃法人資料取得失敗：{e}")
    else:
        ok['TPEX'] = False

    def total_lots(days):
        net = {}
        for data in days.values():
            for code, (f, t) in data.items():
                net[code] = net.get(code, 0) + f + t
        return {c: v / 1000 for c, v in net.items()}  # 股 -> 張

    CHIP['net'] = {'TWSE': total_lots(twse_days) if ok['TWSE'] else {},
                   'TPEX': total_lots(tpex_days) if ok['TPEX'] else {}}
    CHIP['ok'] = ok
    CHIP['dates'] = dates
    save_chip_cache(cache)
    print(f"🏦 法人資料：交易日 {', '.join(x.isoformat() for x in sorted(dates)) or '無'}｜"
          f"上市 {len(CHIP['net']['TWSE'])} 檔{'✅' if ok['TWSE'] else '❌'}、"
          f"上櫃 {len(CHIP['net']['TPEX'])} 檔{'✅' if ok['TPEX'] else '❌'}")


def chip_check(code, suffix):
    """回傳 (是否通過, 近N日外資+投信淨買超張數)。
    True=大戶有買、False=沒買、None=無法判斷(條件關閉或資料抓失敗)。無法判斷時不擋訊號，避免資料源故障整天都沒訊號。"""
    if not CHIP_FILTER_ENABLED or suffix is None:
        return None, None
    market = 'TWSE' if suffix == '.TW' else 'TPEX'
    if not CHIP['ok'][market]:
        return None, None
    net_lots = CHIP['net'][market].get(code, 0.0)  # 法人完全沒交易的股票 = 0 = 不算買進
    return net_lots > CHIP_MIN_NET_LOTS, net_lots


def chip_note():
    if not CHIP_FILTER_ENABLED:
        return ""
    miss = [name for m, name in (('TWSE', '上市'), ('TPEX', '上櫃')) if not CHIP['ok'][m]]
    return f"⚠️ {'、'.join(miss)}法人資料取得失敗，該市場本次未套用大戶條件\n" if miss else ""


def main():
    if not is_market_hours():
        print("目前非台股交易時間 (平日 9:00-13:30)，跳過本次掃描。")
        return

    print("🚀 啟動雙核掃描 (自動辨識上市/上櫃通道，含時間校正 + 相對量能)...")

    # --- 階段一：已知是上櫃的直接走 .TWO，其餘(含第一輪還沒有對照表時)先試 .TW ---
    tw_codes = [c for c in WATCH_STOCKS if SUFFIX_CACHE.get(c, '.TW') == '.TW']
    known_two = [c for c in WATCH_STOCKS if SUFFIX_CACHE.get(c) == '.TWO']

    data_tw = chunked_download([f"{c}.TW" for c in tw_codes], period="1d", interval="1m")

    valid_data = {}
    ticker_suffix_map = {}
    missing_codes = []

    for code in tw_codes:
        ticker_tw = f"{code}.TW"
        if ticker_tw in data_tw and not data_tw[ticker_tw].empty:
            valid_data[code] = data_tw[ticker_tw].dropna()
            ticker_suffix_map[code] = ".TW"
        else:
            missing_codes.append(code)

    # --- 階段二：.TW 找不到的 + 已知上櫃的，抓上櫃 (.TWO) ---
    codes_two = missing_codes + known_two
    if codes_two:
        data_two = chunked_download([f"{c}.TWO" for c in codes_two], period="1d", interval="1m")

        for code in codes_two:
            ticker_two = f"{code}.TWO"
            if ticker_two in data_two and not data_two[ticker_two].empty:
                valid_data[code] = data_two[ticker_two].dropna()
                ticker_suffix_map[code] = ".TWO"

    SUFFIX_CACHE.update(ticker_suffix_map)

    # 清楚的總結：真正找不到資料的股票才需要關注，不用理會過程中.TW試錯的雜訊
    still_missing = [code for code in WATCH_STOCKS if code not in valid_data]
    print(f"📊 資料取得狀況：成功 {len(valid_data)}/{len(WATCH_STOCKS)} 檔"
          f"（上市 {sum(1 for s in ticker_suffix_map.values() if s == '.TW')} 檔、"
          f"上櫃 {sum(1 for s in ticker_suffix_map.values() if s == '.TWO')} 檔）")
    if still_missing:
        print(f"⚠️ 兩種通道都查無資料的代號({len(still_missing)}檔，可能是真的下市/興櫃/代碼有誤)："
              f"{', '.join(still_missing)}")

    # --- 休市日檢查：休市時Yahoo會回傳上一個交易日的資料，不能把舊資料當今天掃 ---
    latest_dates = []
    for _df in valid_data.values():
        if _df.empty:
            continue
        try:
            latest_dates.append(_df.index[-1].tz_convert(TAIWAN_TZ).date())
        except Exception:
            pass
    if latest_dates and max(latest_dates) < datetime.now(TAIWAN_TZ).date():
        print(f"⏸ 最新K棒日期是 {max(latest_dates)}，不是今天(可能是休市日或Yahoo尚未更新)，跳過本次掃描。")
        return

    # --- 階段三：取得日均量與日均金額基準(有快取就不重抓) ---
    baseline_dict = fetch_avg_volumes(ticker_suffix_map, CACHE_PATH)

    alert_state = load_alert_state(ALERT_STATE_PATH)
    ensure_chip_data()
    filtered_by_chip = []
    shallow_wave_skipped = []
    alerts = []
    diagnostic_records = []  # 用來印出「漲幅前幾名」的完整計算數據，方便事後對答案，不影響發送邏輯

    # 訊號等級：數字越小越重要。只有比「今天已經發過的等級」更重要的訊號才會再發一次，
    # 避免一開始的小訊號(如異常量能)把後面更關鍵的訊號(如波段轉強)吃掉。
    # 波段轉強(zigzag確認的上升波段)優先度最高；直接沖漲停的強勢突破次之；異常量能最弱。
    SEVERITY_VTURN = 1      # 🔄波段轉強
    SEVERITY_BREAKOUT = 2   # 🔥強勢突破
    SEVERITY_VOLUME = 3     # ⚠️異常量能

    for code, df in valid_data.items():
        name = WATCH_STOCKS[code]
        if df.empty:
            continue

        open_price = float(df['Open'].iloc[0])
        if open_price == 0:
            continue

        baseline = baseline_dict.get(code)
        avg_daily_volume, avg_turnover_billion = baseline if baseline else (None, None)
        now_metrics = compute_metrics(df, open_price, avg_daily_volume, avg_turnover_billion)
        existing = alert_state.get(code)

        chip_status, chip_net_lots = chip_check(code, ticker_suffix_map.get(code))
        chip_line = ""
        if chip_net_lots is not None:
            chip_line = f"\n ↳ 🏦 法人近{CHIP_LOOKBACK_DAYS}日(外資+投信)：<code>{chip_net_lots:+,.0f}張</code>"

        diagnostic_records.append({
            'code': code, 'name': name,
            'percent_change': now_metrics['percent_change'],
            'turnover_billion': now_metrics['turnover_billion'],
            'strategy_1': now_metrics['strategy_1'],
            'strategy_2': now_metrics['strategy_2'],
            'strategy_3': now_metrics['strategy_3'],
            'avg_turnover_billion': avg_turnover_billion,
            'existing_severity': existing['severity'] if existing else None,
            'chip_net_lots': chip_net_lots,
        })

        # --- 策略二(波段轉強)：抓的是「trend up」這個現象本身，不管前面有沒有跌深。
        # 不是「發過一次就不能再發」，而是「每一個新的上升波段都可以各發一次」---
        wave_alert_ok = False
        if now_metrics['strategy_2']:
            trough_key = now_metrics['wave_trough_idx'].isoformat() if now_metrics['wave_trough_idx'] is not None else None
            prev_trough_key = existing['wave_trough_key'] if existing else None

            # 只要zigzag確認出一個「新的」起漲低點(跟上次發過的不是同一個)，就算一個新波段，
            # 不分是不是今天第一次、也不管那個低點跌得深不深，規則完全一致。
            wave_alert_ok = trough_key is not None and trough_key != prev_trough_key

            # 今天已經發過訊號(不論哪一種)的股票，要「先前真的拉回夠深」才值得再發第二次。
            # 上升途中只是小小往下就又彈上去，還是同一段行情，不重複發報(例如台星科)。
            if wave_alert_ok and existing is not None:
                pb = now_metrics['wave_pullback_pp']
                if pb is None or pb < WAVE_REALERT_MIN_PULLBACK_PP:
                    wave_alert_ok = False
                    pb_txt = f"回檔{pb:.1f}pp" if pb is not None else "中途沒有回檔"
                    shallow_wave_skipped.append(f"{code}{name}({pb_txt})")

        # --- 大戶(法人)買進條件：外資+投信近N個交易日合計淨買超才放行，三種訊號都要過這一關 ---
        if chip_status is False and (wave_alert_ok or now_metrics['strategy_1'] or now_metrics['strategy_3']):
            filtered_by_chip.append(f"{code}{name}({now_metrics['percent_change']:+.1f}%)")
            continue

        if wave_alert_ok:
            signal_type = "🔄波段轉強"
            severity = SEVERITY_VTURN
            extra_line = ""
            if existing is not None:
                pb = now_metrics['wave_pullback_pp']
                pb_txt = f"{pb:.1f}pp" if pb is not None else "一段"
                extra_line = f"\n ↳ 🔁 新一波上升：先前回檔{pb_txt}後再度轉強向上"
            extra_line += chip_line

            alerts.append(
                f"<b>{code} {name}</b> {signal_type}\n"
                f" ↳ 現價：<code>{now_metrics['current_price']:.2f}元</code> ❘ 漲幅：<code>{now_metrics['percent_change']:+.2f}%</code>\n"
                f" ↳ 盤中量：<code>{now_metrics['volume_lots']:,}張</code> ❘ 金額：<code>{now_metrics['turnover_billion']:.1f}億</code>"
                f"{extra_line}"
            )
            alert_state[code] = {
                'severity': SEVERITY_VTURN,
                'wave_trough_key': now_metrics['wave_trough_idx'].isoformat(),
            }
            continue  # 這檔這一輪已經處理完，不用再看策略一/三

        # --- 策略一(強勢突破)、策略三(異常量能)：維持原本「只有更重要的訊號才能再發」的規則 ---
        if now_metrics['strategy_1'] or now_metrics['strategy_3']:
            if now_metrics['strategy_1']:
                signal_type = "🔥強勢突破"
                severity = SEVERITY_BREAKOUT
            else:
                signal_type = "⚠️異常量能"
                severity = SEVERITY_VOLUME

            if existing is not None and severity >= existing['severity']:
                continue  # 沒有比之前發過的更重要，跳過

            extra_line = ""
            if now_metrics['strategy_3']:
                extra_line = (
                    f"\n ↳ 量能倍數：<code>{now_metrics['relative_volume']:.1f}x</code>（相對該時段應有量）"
                    f"\n ↳ ⚠️ 僅能偵測「量能異常放大」，無法判斷是買方還是賣方主導，請自行確認方向"
                )
            extra_line += chip_line

            alerts.append(
                f"<b>{code} {name}</b> {signal_type}\n"
                f" ↳ 現價：<code>{now_metrics['current_price']:.2f}元</code> ❘ 漲幅：<code>{now_metrics['percent_change']:+.2f}%</code>\n"
                f" ↳ 盤中量：<code>{now_metrics['volume_lots']:,}張</code> ❘ 金額：<code>{now_metrics['turnover_billion']:.1f}億</code>"
                f"{extra_line}"
            )
            # 保留原本已經記錄的wave_trough_key(如果有的話)，避免蓋掉波段轉強的紀錄
            alert_state[code] = {
                'severity': severity,
                'wave_trough_key': existing['wave_trough_key'] if existing else None,
            }

    # 診斷區：印出漲幅前10名的完整計算數據，不管有沒有觸發都印，
    # 之後如果又出現「別人抓到、我們沒發」的情況，直接對這段log就能查出是門檻沒到、
    # 還是資料/邏輯真的有問題，不用再用猜的。
    top_movers = sorted(diagnostic_records, key=lambda r: r['percent_change'], reverse=True)[:10]
    SEVERITY_LABELS = {1: '波段轉強', 2: '強勢突破', 3: '異常量能', None: '今天尚未發過'}
    print("🔎 診斷：今日漲幅前10名(不論是否觸發訊號)")
    for r in top_movers:
        avg_to_str = f"{r['avg_turnover_billion']:.2f}億" if r['avg_turnover_billion'] is not None else "無資料"
        already = SEVERITY_LABELS.get(r['existing_severity'], '未知')
        chip_str = f"{r['chip_net_lots']:+,.0f}張" if r.get('chip_net_lots') is not None else "未套用"
        print(f"   {r['code']} {r['name']}：漲幅{r['percent_change']:+.2f}% "
              f"❘ 今日金額{r['turnover_billion']:.2f}億 ❘ 日均金額{avg_to_str} "
              f"❘ 策略1={r['strategy_1']} 策略2={r['strategy_2']} 策略3={r['strategy_3']} "
              f"❘ 今天已發過：{already} ❘ 法人近{CHIP_LOOKBACK_DAYS}日：{chip_str}")

    if shallow_wave_skipped:
        print(f"↪️ 回檔不夠深(<{WAVE_REALERT_MIN_PULLBACK_PP}pp)，不重複發第二次波段轉強："
              + "、".join(shallow_wave_skipped))
    if filtered_by_chip:
        print(f"🏦 被法人條件擋下 {len(filtered_by_chip)} 檔(近{CHIP_LOOKBACK_DAYS}日外資+投信沒有淨買超)："
              + "、".join(filtered_by_chip))

    if alerts:
        cond = f"｜法人近{CHIP_LOOKBACK_DAYS}日買超" if CHIP_FILTER_ENABLED else ""
        header = f"🤖 <b>Claude Real Time</b>\n🚨 盤中大戶監控 (新發動{cond})\n{chip_note()}━━━━━━━━━━━━━━\n"
        if send_alerts_chunked(header, alerts):
            save_alert_state(ALERT_STATE_PATH, alert_state)
        else:
            print("⚠️ 訊息未全部送出成功，本輪不更新「已發送」紀錄，下一輪會再試。")
    else:
        save_alert_state(ALERT_STATE_PATH, alert_state)
        print("目前無新發動、波段轉強或異常量能的股票。")


if __name__ == '__main__':
    main()
