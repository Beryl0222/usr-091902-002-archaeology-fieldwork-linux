# 考古现场关系管理

保存跨年度勘探、发掘、研究判断与样本流转之间的可追溯关系。

`fixtures/domain.json` 保存首批领域名词和状态样例，便于接口联调时保持一致语义。运行 `python3 service.py --check` 可检查基础配置；执行 `npm test` 可运行全部契约与领域测试（25 项）。

## 模型概览

所有业务事实都是**不可变事件**（`domain.py`），服务本身不保存"当前值"，当前状态由事件折叠得出：

- **事件信封**携带 `device_id`（采集设备）、`clock`（该设备维护的 Lamport 逻辑时钟）、`happened_at`（UTC）与 `event_id`。
- **离线归队合并**：`POST /merge` 接收成批事件，按 `(clock, device_id, event_id)` 得到全序；同一 `event_id` 幂等去重，同 id 内容冲突记 `conflicting` 而不覆盖。乱序送达（含撤回先于测年、照片先于器物）经多轮延后折叠得到同一结果，始终无法满足前置的事件记异常留痕。
- **空间与层位**：探方（坐标）→ 地层（深度、叠压）→ 遗迹（主墓/兆沟/陪葬墓/祭祀坑…）→ 器物组合；器物可挂铭文、照片（带坐标）、测年样本。器物组合在状态中按遗迹与地层两种口径给出。
- **校正即新版本**：任何 `resource_corrected` 都要求原因，只允许白名单字段；原值与历次版本完整保留。重复编号且内容不同的登记不覆盖早期记录，器物目录号冲突双方都保留并进入 `catalog_conflicts`，须以校正闭环。
- **竞争性观点**：年代、制度等级、墓主等观点并存（`competing_claims`），每条必须引用具体证据、声明 `low/medium/high` 置信度；含"可能/疑似"等措辞不得标 high。观点接受独立同行评议（提议人自评不计支持），并可撤回。
- **发布门槛**：`POST /publications` 固化快照，只有"活跃 + 证据可解析 + 独立同行支持 + 依据测年未撤回"的观点进入 `conclusions`，其余带阻断原因进入 `excluded_claims`，推断始终带 `epistemic: "inference"` 与置信度，不发布为既定事实。
- **测年撤回**：`dating_withdrawn` 要求原因；撤回后引用该测年的观点在当前视图失去可发布性。
- **保管链**：样本/文物支持临时出库（checkout，须填去向与目的）、归还（return，闭环到原出库）、跨库移交（transfer，须由当前保管方发起；出库期间禁止移交）。样本消耗超额拒绝，余额由事件折叠，可稳定复算。
- **时点还原**：发布快照内嵌所依据的事件 id 集合与事件集合哈希；`GET /publications/<id>/reconstruction` 用该子集独立重放并核对哈希与结论，可还原任一发布日期当时成立的结论（后续撤回不影响历史发布）。`GET /state?as_of=...` 支持任意时点视图。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/health` | 健康检查（稳定身份契约） |
| POST | `/commands` | 在线命令，服务端校验并补逻辑时钟 |
| POST | `/merge` | 离线批次归队合并（简写事件或完整信封） |
| GET | `/state[?as_of=UTC]` | 当前状态 / 时点状态 |
| POST | `/publications` | 认证发布不可变快照 |
| GET | `/publications/<id>` | 读取发布快照 |
| GET | `/publications/<id>/reconstruction` | 独立重放并核对发布当时结论 |

命令类型见 `domain.py` 的 `COMMAND_TO_EVENT`（如 `record_feature`、`correct_record`、`propose_claim`、`review_claim`、`withdraw_dating`、`custody_event`、`consume_sample`）。事件存储可选 JSONL 追加日志（`--log path.jsonl` 或 `FIELDWORK_LOG`），重启后自动重放。

## 运行

```bash
python3 service.py --check            # 基础冒烟检查
python3 service.py --port 8000        # 启动服务
python3 service.py --log events.jsonl # 带持久化
npm test                              # 全部测试
```
