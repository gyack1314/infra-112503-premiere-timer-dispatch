# 跨区域节目编排的延时消息中间件

一个只依赖 Python 3.11 标准库的小型中间件原型。它把节目编排转换为带依赖、窗口、
改期闸门、合作方回执和观众更正名单的可审计状态，而不是简单地 `sleep` 后发 HTTP。

## 核心边界

1. **本地时间只是输入表示**：最终保存 IANA 时区、明确 `fold` 和 UTC 整数秒点。
2. **领取/请求不是执行事实**：`pull` 只生成 `leased` 派发单；只有合作端
   `callback.state=sent` 且给出/可推断 `executed_at` 后，才记录实际执行。
3. **未知不是完成**：`unknown`、超时、重启都不会把旧单据盖章为完成。
4. **改期先处理旧持有项**：未派发项直接按新 revision 重算；合作方已领取的项进入
   `withdraw_requested`，在确认 `withdrawn` 或 `sent` 前阻塞同一通知含义的新派发。
5. **已实际发出的旧公告不能被新公告抹掉**：旧 revision 的 `sent` 回执会生成更正单，
   并把回执 `recipients` 标识为需要更正的观众。
6. **互动入口另有地区开放闸门**：即使计划秒点已到、依赖已满足，只要没有地区正式开放
   事实，名为 `join/enter/interact/entry/...` 或显式声明 `requires_region_open`
   的动作仍不可领取。
7. **多地区隔离**：状态键为 `(show, region)`，一个地区的改期、阻塞或校验错误不影响其他地区。
8. **重启不丢状态**：传入 `state_path` 后，状态以 JSON 原子替换落盘；`restart`
   会重新加载并按当前时钟、`miss` 策略和人群事实重算。

## 快速运行

```bash
# 原有资料格式检查
python3 scenario_reader.py examples

# 全部单元/接口测试
python3 -m unittest discover -v

# 值班台文本报告：运行两个原始演练
python3 main.py drill

# 运行单个 JSONL 演练，并打印 pull/withdraw 响应
python3 main.py scenario examples/moved-before-callback.jsonl --show-pulls

# 纯 JSON 的 inspect 结果
python3 main.py scenario examples/fall-back-and-missed-window.jsonl --json

# 从 HTTP 服务状态文件直接查询值班视图
python3 main.py query --show D --region s --state delayed-middleware-state.json

# 启动 HTTP 服务和值班页面
python3 app.py
# 浏览器打开：http://127.0.0.1:8080/?show=D&region=s
```

也可以使用模块入口：

```bash
python3 -m delayed_middleware scenario drills/open-depend-and-interact.jsonl --show-pulls
python3 -m delayed_middleware server --host 127.0.0.1 --port 8080 --state state.json
```

## 随仓库提供的演练

原始契约中的两个示例位于 `examples/`；`drills/` 放使用可选 `publish` 扩展的完整顺序演练。

- `examples/fall-back-and-missed-window.jsonl`
  - `remind`：纽约回拨第一次 `01:30 fold=0` → `05:30Z`，窗口 60s，`miss=skip`；
    重启后时钟到 `06:33Z`，显示 `skipped_missed`，不会再投。
  - `room`：同一本地时刻第二次 `01:30 fold=1` → `06:30Z`，窗口 120s，
    `miss=catch_up`；`06:33Z`  pull 仍可迟到领取。
- `examples/moved-before-callback.jsonl`
  - 第一次 pull 领取 `announce@r1`。
  - r2 改期后，旧派发单进入撤回请求；`unknown` 回执不解除阻塞。
  - 第二次 pull 只拿到 `withdrawals`。
  - 最后的 `sent` 回执证明旧公告在 `09:00:20Z` 实际发给 `a,c`；查询中生成
    观众更正名单 `["a","c"]`。
  - `join` 对 `announce` 的依赖可由旧 revision 的实际回执满足，但该场景没有地区开放
    事实，因此互动入口仍有 `region_open` 未满足项。
- `drills/open-depend-and-interact.jsonl`
  - 演示 `room → remind → join`、地区 `publish` 事实和合作方实际回执的正常顺序。

## JSONL 输入契约

原始六类消息保持不变：`plan`、`clock`、`pull`、`callback`、`restart`、`inspect`。
每行仍是 `{"at": 整数逻辑秒, "kind": ..., "record": {...}}`，同一 `at` 按行序处理。

`plan.record.actions[]` 支持：

| 字段 | 说明 |
| --- | --- |
| `action` | 地区内稳定动作名；跨 revision 的同名动作表示同一通知含义 |
| `local` | 不带偏移的完整地区本地时间 |
| `fold` | 回拨歧义时刻必须显式给 `0`（第一次）或 `1`（第二次） |
| `window` | 窗口长度，整数秒；截止点为 `scheduled_utc + window` |
| `miss` | `skip`：严格窗口，过后不投；`catch_up`：过后进入补发决策 |
| `needs` | 依赖的同地区其他动作；必须有 `sent` 实际回执，或同名外部公开事实 |
| `requires_region_open` | 显式声明是否要求地区正式开放；`join/interact/entry` 等名称默认要求 |
| `publishes_region_open` | 该动作收到 `sent` 回执后同时形成地区开放事实；名为 `room`/`room_open`/`region_open` 等的动作默认具备此效果 |
| `audience` | 可选的该动作人群，覆盖 show/region 或本地默认仿真人群 |
| `allow_replacement_after_send` | 旧项已确认 `sent` 后，是否仍允许显式编排的替代通知；默认 `false`，先走更正 |

可选扩展消息：

- `publish` / `publication` / `region_fact`：地区发布/开放事实，例如
  `{"show":"S","region":"R","status":"open","published_at":"...Z"}`。
- `fact`：任意上游已公开事实，`record.fact` 可被 `needs` 按名依赖。
- `audience`：更新地区实际人群。

无法解析的 IANA 名、本地时间、春令时不存在时刻、歧义但缺少 `fold`、依赖环或未知依赖，
都会使整份 plan 返回 `needs_editor_change`；当前有效编排不会被半份坏预约覆盖，
同时查询中保留“退回编排修改”的票据。

## HTTP/JSON 接口

| 方法与路径 | 用途 |
| --- | --- |
| `POST /v1/plans` | 首次预约或提交更高 revision 改期 |
| `POST /v1/reservations` | 预约别名 |
| `POST /v1/reschedules` | 显式改期；原地区必须存在且 revision 更大 |
| `POST /v1/pull` | 合作端领取可执行动作、更正单和待响应撤回 |
| `POST /v1/callbacks` | 合作端回报 `sent` / `withdrawn` / `unknown` |
| `POST /v1/publications` | 接收地区正式发布/开放事实 |
| `POST /v1/facts` | 接收任意上游已公开事实 |
| `POST /v1/audience` | 更新地区实际人群 |
| `GET /v1/regions` | 所有地区实时摘要 |
| `GET /v1/regions?show=...&region=...` | 单地区值班查询 |
| `GET /v1/regions/{show}/{region}` | 路径形式的单地区查询 |
| `GET /v1/shows/{show}/regions/{region}` | 等价路径 |
| `GET /?show=...&region=...` | 值班人可读 HTML 页面 |
| `POST /v1/clock` | 演练/仿真时钟 |
| `POST /v1/restart` | 重新加载状态并执行恢复重算 |

### 调用示例

```bash
curl -sS -X POST http://127.0.0.1:8080/v1/plans \
  -H 'Content-Type: application/json' \
  -d '{
    "show":"premiere","region":"east","revision":1,
    "tz":"America/New_York",
    "actions":[
      {"action":"remind","local":"2026-11-01T01:30:00","fold":0,
       "window":60,"miss":"skip","needs":["room"]},
      {"action":"join","local":"2026-11-01T01:33:00","fold":0,
       "window":600,"miss":"catch_up","needs":["room","remind"],
       "requires_region_open":true}
    ]
  }'

curl -sS -X POST http://127.0.0.1:8080/v1/publications \
  -H 'Content-Type: application/json' \
  -d '{"show":"premiere","region":"east","status":"open"}'

curl -sS -X POST http://127.0.0.1:8080/v1/pull \
  -H 'Content-Type: application/json' \
  -d '{"provider":"partner-a","show":"premiere","region":"east"}'

curl -sS -X POST http://127.0.0.1:8080/v1/callbacks \
  -H 'Content-Type: application/json' \
  -d '{
    "provider":"partner-a","show":"premiere","region":"east",
    "action":"remind","revision":1,"state":"sent",
    "recipients":["a","c"],"executed_at":"2026-11-01T05:30:09Z"
  }'

curl -sS 'http://127.0.0.1:8080/v1/regions/premiere/east'
```

## Python API

```python
from delayed_middleware import Middleware

middleware = Middleware("state.json", wall_clock_fallback=True)
middleware.schedule_plan(plan_record)
middleware.receive_publication({
    "show": "S", "region": "R", "status": "open",
    "published_at": "2026-12-01T10:00:00Z",
})
delivery = middleware.provider_pull("partner", "S", "R")
middleware.provider_callback(callback_record)
report = middleware.inspect("S", "R")
```

常用别名也已提供：`schedule`、`pull`、`callback`、`get_region`、`query_region`。

## 值班查询中的关键信息

`GET /v1/regions/{show}/{region}` 返回：

- 当前 revision、IANA 时区、当前查询 UTC、地区是否正式开放；
- 每个动作的本地时刻、fold、UTC 应执行秒点、窗口截止点和错过策略；
- 当前状态：`scheduled`、`ready`、`held`、`catch_up_ready`、
  `waiting_precondition`、`blocked_old_disposition`、`withdraw_requested`、
  `expired_unconfirmed`、`skipped_missed`、`sent`、`withdrawn` 等；
- 实际 `sent` 回执、真实 `executed_at`、接收者批次和 on-time/late 判定；
- 未满足的前置事件及已由哪个 revision 的公开/回执事实满足；
- 旧 revision 尝试、撤回单据和重排闸门；
- 哪些观众已被合作方回执确认收到旧公告、哪些人需要更正；
- 过期无结论、撤回无结论、前置仍缺失等 `unresolved` 项；
- 无法解析、需退回编排修改的 `planning_issues`。

## 本地仿真接收者

`delayed_middleware/receivers.py` 内置四个少量接收者：

- `a`：在线立即送达；
- `b`：弱网延迟后送达；
- `c`：首次延迟/重试后送达；
- `d`：当前渠道不可达。

这些状态只用于演示“实际人群/到达状态”和补发名单重算。合作方 `sent.recipients`
是更权威的已发送/暴露事实，不会被仿真状态反向改写成“未执行”。补发名单会排除同一
动作已有实际回执的接收者，并给出目标人群、已确认到达和仿真到达状态。

## 主要目录

```text
delayed_middleware/
  timeutil.py      # IANA、fold、跳空、UTC 秒点
  receivers.py     # 少量仿真接收者
  engine.py        # 状态机、预约/改期、pull/callback、更正、查询、持久化
  reporting.py     # 值班文本视图
  scenario.py      # JSONL 演练驱动器
  server.py        # 标准库 HTTP/JSON 与 HTML 值班页
examples/          # 原始输入契约和两个演练资料
drills/            # 使用可选发布事实扩展的完整演练
tests/             # 引擎不变量、示例场景和 HTTP 接口测试
```
