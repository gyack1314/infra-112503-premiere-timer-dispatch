# 节目编排的延时消息与错过窗口处置

时间输入分逻辑接收顺序和 UTC 实际秒点。地区 tz 采用 IANA 名，local 为完整本地时间。歧义的 fold 取 0 或 1。

## 输入消息

每个 JSONL 文件是一段独立输入记录。每行包括整数秒点 at、消息名称 kind 与对象 record。at 代表中间件收到消息的时刻，同一秒内按行序提交。业务时间和对应回执引用写在 record 中。

- **plan**　show、region、revision、tz 和动作 local、fold、window、miss。miss 为 skip 或 catch_up，needs 指向该地区其他 action 的实际完成。
- **clock**　utc 是当前 UTC ISO 时间。
- **pull**　provider 领取 region 的可执行动作。
- **callback**　provider 按 show、region、action、revision 回报 state 与 recipients，状态 sent、withdrawn 或 unknown。 sent 的 executed_at 表示真正送出时间。若仿真记录省略该字段，以最后一次已知时钟值为准。
- **restart**　中间件重启。
- **inspect**　按 show/region 查询。

## 示例

- fall-back-and-missed-window.jsonl
- moved-before-callback.jsonl

## 读取演练资料

运行环境为 Python 3.11，只使用标准库。在根目录运行以下命令可核对文件格式及读取接口。

    python3 scenario_reader.py examples
    python3 -m unittest discover -v

资料读取器会按文件统计各类参与方消息的数量。演练记录只描述各参与方交给系统的输入。
