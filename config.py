"""全部阈值集中在这里。

这些只是起点，不是策略。先用影子模式跑一两周，看 report.py 的校准结果再调。
"""

# ---- 运行 ----
CYCLE_SECONDS = 15 * 60
DB_PATH = "desk.db"
MOCK_DB_PATH = "desk-mock.db"  # --mock 的数据单独存，别混进真实校准
SHADOW_BANK = 1_000.0          # 影子模式的虚拟本金（USDC）
JEV_MODEL = "jev-latest"       # 跑稳以后建议固定版本，比如 "jev-1.13.0"
JEV_PRICE_PER_MTOK = 0.042

# ---- 宇宙：Gamma ----
UNIVERSE_PAGES = 5             # 每页 100 个市场，按 24h 成交额从高到低
MAX_DAYS_TO_END = 30

# ---- 硬过滤：纯代码，不发请求 ----
# 不碰的市场类别（Gamma feeType 的前缀）：
# - sports：历史回测里 507 个体育盘，Jev 不比市场准，加上它也不比只用市场价好（research/jev_backtest/README.md）
# - crypto / finance_prices：比特币、以太坊、股指的价格盘。纯数字，Jev 不管什么情况都给 0.4–0.55；
#   实盘前 8 个已结算的加密货币盘，市场 Brier 0.103，Jev 0.238
EXCLUDE_FEE_TYPES = ("sports", "crypto", "finance_prices")
MIN_LIQUIDITY = 10_000
MIN_VOLUME_24H = 20_000
MIN_HOURS_TO_END = 12
PRICE_BAND = (0.15, 0.85)      # 中间价太靠近 0/1：赔率差、尾部风险大；Jev 的概率往 0.5 压缩，冷门那边会被系统性高估
MAX_SPREAD = 0.03
MAX_MOVE_1H = 0.08             # 1 小时内价格变动太大，说明市场正在消化新闻，我们的信息大概率已经过时

# ---- 盘口过滤：CLOB + Data API ----
DEPTH_BAND = 0.03              # 只统计离最优价 3¢ 以内的挂单
MIN_DEPTH_USD = 1_500          # 两个结果的买卖盘各自至少这么多
MIN_TRADES_24H = 40
MAX_DOSSIERS = 15              # 每轮最多建多少份档案（每份 = 2 个请求 + 2 次新闻搜索 + 2 次 Jev）

# ---- 新闻：Bing News + Google News 的 RSS 搜索（news.py）----
NEWS_MAX_ITEMS = 8             # 每份档案最多几条。state 越长 Jev 越贵、也越容易被无关内容带偏
NEWS_MAX_AGE_DAYS = 7

# ---- 软过滤：Jev 的回答 ----
MIN_RULES_CLEAR = 0.70
MAX_DISPUTE_RISK = 0.50        # 实测区分度差：规则很清楚的盘也常给 0.4–0.5，0.30 会拦下大部分市场
MIN_INFO_SCORE = 1.0           # 0~3 分；低于 1 = 除了题目本身几乎没有信息

# ---- 定价 ----
SHRINK = 0.25                  # 没有 FAIR_COEF 时：公允价 = 市场价 + SHRINK × (Jev - 市场价)。
                               # 历史回测里 Jev 没带来市场价以外的信息，只给它四分之一的权重

# 可选：公允概率 = sigmoid(c0 + c1·logit(市场价) + c2·logit(Jev))，系数取自 research/jev_backtest 的逻辑回归。
# 现在不用：回测里这个组合（交叉验证）不比只用市场价好，Jev 的系数也不显著
FAIR_COEF: tuple[float, float, float] | None = None
MIN_EDGE = 0.04                # 扣掉手续费后，每股至少 4¢ 的优势
SHORTLIST = 5                  # 交给 Jev 做最终选择的候选数
MIN_PICK_CONFIDENCE = 0.40

# ---- 仓位 ----
MAX_POSITIONS = 3
KELLY_FRACTION = 0.25
MAX_TICKET_PCT_BANK = 0.05
MAX_TICKET_PCT_DEPTH = 0.20    # 一次最多吃掉近端卖盘的 20%
MIN_TICKET_USD = 5.0
LOW_INFO_FACTOR = 0.5          # info 分低于 2 时仓位打折

# ---- 冷却（bench）----
BENCH_BOOK_HOURS = 1           # 盘口不合格
BENCH_REJECT_HOURS = 6         # Jev 软过滤没过
BENCH_PASS_HOURS = 2           # 问过 Jev 但没优势 / 没被选中

# ---- 退出 ----
EXIT_VOLUME_DECAY = 0.20       # 24h 成交额 < 0.2 × 近 7 日日均 → 平仓
