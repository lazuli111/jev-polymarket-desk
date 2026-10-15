# Jev × Polymarket 影子交易台

**在线看板：https://lazuli111.github.io/jev-polymarket-desk/** （每 15 分钟随 GitHub Actions 更新一次）

借鉴 savip 那篇 “Megabrain (Jev) & Six Grok Bots” 的架构：代码负责所有能算的东西，Jev 只回答“规则清不清楚”“结果更可能是哪个”这类判断题，每个回答都是带概率的数字。数据源从 meme 币换成了 Polymarket。

**目前只有影子模式**：每一笔都按当时的真实盘口模拟成交、记账，不会下单。

## 和原帖的对应关系

| 原帖（meme 币） | 这里（Polymarket） |
|---|---|
| GeckoTerminal 新池子 | Gamma API：30 天内到期、按 24h 成交额排序的前 500 个市场 |
| 免费过滤：币龄 / 流动性 / 成交量 / 市值 | 硬过滤：可交易、剩余时间 12h–30d、流动性、成交额、中间价在 0.15–0.85、价差、1 小时波动 |
| DexScreener 买卖笔数 | CLOB 盘口深度（两侧各 ≥ $1,500）+ Data API 24h 成交笔数 |
| 档案：项目信息、链上数据、X 账号 | 档案：题目、结算规则、结算来源、剩余时间、近 7 天的新闻标题（Bing / Google News RSS） |
| Jev 给每个币打分 | Jev 分两次回答：看完整档案答结果概率 `p_a` 和信息量；只看规则答规则清晰度和争议风险 |
| Jev 从候选里挑一个 | 同样，最多挑一个，可以回答“都不选” |
| 6% 本金 / 2% 池子流动性 | ¼ Kelly，上限 5% 本金、近端卖盘的 20%，信息不足时再打五折 |
| 手续费下限 | Polymarket taker 费：股数 × rate × p × (1−p)，按市场类别取 rate |
| 6h 成交量 < 0.2× 均值就平仓 | 结算、优势兑现（买价 ≥ 公允价）、或 24h 成交 < 0.2× 周均就平仓 |

## 快速开始

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
cp .env.example .env        # 然后在 .env 里填上自己的 TYPESAFE_API_KEY
.venv/bin/python main.py --once --mock   # 不调用 Jev，先确认流程能跑通（数据单独存到 desk-mock.db）
.venv/bin/python main.py                 # 真跑：每 15 分钟一轮
.venv/bin/python report.py               # 看成绩单
.venv/bin/python ui.py                   # 本地看板：http://localhost:8765
```

没有 key 的话，可以用 `.venv/bin/python set_key.py` 在终端里输入，它会写进 `.env`（权限 600，输入不回显）。

## 文件

- `config.py`：全部阈值
- `polymarket.py`：Gamma / CLOB / Data API 只读客户端，加上盘口模拟成交
- `collect.py`：宇宙 → 硬过滤 → 盘口过滤 → 档案。`news_query()` 把题目变成搜索词，`fetch_news()` 把新闻放进档案
- `news.py`：Bing News + Google News 的 RSS 搜索（免费、不要 key），本轮缓存、限速、出错返回空
- `filters.py`：三道过滤
- `questions.py`：所有给 Jev 的问题
- `judge.py`：唯一使用 Jev key 的地方，另有 `MockJudge`
- `sizing.py`：公允价、手续费、Kelly 下单量
- `pick.py`：让 Jev 从候选里最多挑一个
- `book.py`：SQLite 账本，记录每次判断、影子仓位和冷却名单
- `main.py`：主循环
- `report.py`：校准报告和影子盈亏
- `ui.py` + `ui/`：本地看板（标准库 HTTP 服务，只读，只监听 127.0.0.1）
- `set_key.py`：在终端里输入 Jev key，写进 `.env`
- `.github/workflows/desk.yml`：GitHub Actions 定时任务，每 15 分钟跑一轮
- `research/short_cycle/`：已放弃的 5 / 15 分钟涨跌盘研究（归档）

## 看板

- **在线：https://lazuli111.github.io/jev-polymarket-desk/** 。GitHub Actions 每跑完一轮，就用最新的账本生成静态页面，发布到 GitHub Pages。页眉会显示数据更新时间。
- **本地：http://localhost:8765** 。先运行 `.venv/bin/python ui.py`，它读本地的 `desk.db`。

两边是同一套页面：虚拟现金、持仓、Jev 判断的结论分布、最近的判断和校准，每 30 秒刷新。只读，不会改账本。

## 在 GitHub 上自动跑

`.github/workflows/desk.yml` 每 15 分钟在 GitHub Actions 上跑一轮 `main.py --once`：

- **Jev key** 放在仓库的 Actions secret `TYPESAFE_API_KEY` 里，不在代码里：
  ```bash
  gh secret set TYPESAFE_API_KEY   # 会提示粘贴，输入不显示
  ```
- **账本**（`desk.db`）存在 `data` 分支上。每轮开始时取出来，跑完推回去，`data` 分支只保留最新的一个提交。取账本失败时这一轮直接失败，不会用空账本覆盖。
- **看板**：每轮最后执行 `ui.py --export site`，把页面和 `desk.json` 发布到 GitHub Pages（仓库设置里 Pages 的来源是 GitHub Actions）。
- **手动跑一轮**：仓库的 Actions 页面里点 “Run workflow”，或者 `gh workflow run desk.yml`。
- **在本地看最新数据**：把 `data` 分支上的账本取下来，再开看板：
  ```bash
  git fetch origin data && git show origin/data:desk.db > desk.db
  ```
- GitHub 的定时任务经常晚几分钟，高峰期偶尔会跳过一轮；公开仓库连续 60 天没有活动会自动停用定时任务（每轮推 `data` 分支算活动）。
- 本地不要同时再跑 `main.py`，两边的账本会分叉。

## 已放弃的方向：5 / 15 分钟涨跌盘

半小时内结算的市场几乎全是加密货币 5 / 15 分钟涨跌盘。分钟级回测加秒级前向记录的结论是：盘口本身很准，用现货价算的 TWAP 模型在同一秒比不过盘口（Brier 0.169 vs 0.123，模型打法扣费后约 −50%），Coinbase 现货也不领先盘口，Jev 在这类纯数字、秒级反应的市场上派不上用场。代码、方法和数字都归档在 [research/short_cycle/](research/short_cycle/README.md)。

## 几个关键设计

1. **给 Jev 的 state 里不放价格。** 要的是它的独立判断，放了价格它会锚定市场价。
2. **公允价 = 市场价 + 0.5 × (Jev − 市场价)。** 校准结果出来之前，不能完全相信 Jev。
3. **每一次判断都会入库，市场结算后自动补上结果。** `report.py` 会对比 Jev 和市场价的 Brier 分数。**这才是影子模式真正要回答的问题：Jev 的判断比市场价更准吗？** 如果比不过市场价，阈值调得再细也没用。
4. 日期运算、计数和所有数字都由代码完成。第三方测试显示 Jev 不擅长这些。
5. **规则质量单独问，state 里只放规则。** 实测放在同一次调用里时，Jev 会把新闻和“结果难猜”混进规则清晰度和争议风险。拆开后规则清晰度明显更合理，但争议风险的区分度还是差（规则很清楚的盘也常给 0.4–0.5），`MAX_DISPUTE_RISK` 要看数据再定。
6. **价格区间收窄到 0.15–0.85。** Jev 的概率会往 0.5 压缩，在冷门结果上系统性高于市场价，不收窄的话影子仓位会集中在买冷门。
7. **新闻标题是不可信的第三方文本。** 只留纯文本（去 HTML、链接、控制字符）、截短，链接一律不打开。提到 Polymarket / Kalshi 等预测市场盘口的条目直接丢掉，理由同第 1 条。

## 还没做的

- **更好的新闻源**：现在每份档案最多 8 条近 7 天的“日期 - 标题 - 摘要”，来自 Bing News 和 Google News 的 RSS 搜索，搜索词由代码从题目里提取。局限：
  - 只有标题和一句摘要（Google 那边只有来源名），不读正文；冷门市场（小众电竞、低级别网球）经常搜不到或只有赛前预测文。
  - 相关性只做了关键词级别的过滤，偶尔会混进擦边的新闻。
  - 两家 RSS 的条款都只允许个人、非商业使用，量大或商用要换正式的新闻 API。
  - 体育新闻里常带博彩赔率，这其实也是一种“价格”，目前没过滤。要不要过滤，等校准报告出来再看。
  - 接上新闻后 `info` 分应该会上来，`too_little_info` 拦下的会变少。要先看几轮真实的 Jev 回答，再决定 `MIN_INFO_SCORE` 要不要调。
- **实盘下单**：需要 `py-clob-client` 和 Polymarket 钱包私钥，等影子数据证明有优势之后再接。
- **地区限制**：Polymarket 国际站对部分国家和地区（包括美国）有访问限制。先确认你所在的地区可以合法使用。

## 风险

预测市场的大盘口（选举、美联储）定价通常很有效率，单靠一个模型判断很难长期赢过市场。阈值只是起点。不是投资建议。
