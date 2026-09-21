"""考古现场关系管理：事件日志、离线合并、状态投影与发布时点重建。

设计要点
========

* 一切业务事实都是不可变事件，信封携带 ``device_id``（采集设备）与
  ``clock``（该设备维护的 Lamport 逻辑时钟）。现场离线录入的事件成批
  "归队" 合并，按 ``(clock, device_id, event_id)`` 得到全序，同一
  ``event_id`` 幂等去重——因此同一批事件无论以什么顺序、分几批送达，
  整理结果都相同。
* 投影只依赖事件集合而不依赖送达顺序：前置对象尚未出现的事件进入延后
  队列，多轮折叠直至不动点；始终无法满足的事件记为异常并留痕，绝不静默
  覆盖。
* 任何校正都产生新版本，原值保留在版本链中；测年可撤回，引用撤回测年的
  观点在当前视图失去支撑，但历史发布快照不受影响。
* 年代、制度等级、墓主等竞争性观点并存，各自引用具体证据、声明置信度、
  接受独立同行评议；发布时把不满足条件的观点排除在"结论"之外，任何结论
  都携带置信度标签，推断不得冒充既定事实。
* 样本消耗、文物临时出库与跨库移交进入保管链；样本余额由事件折叠得出，
  超额消耗与链条断裂被拒绝并记异常，故余额稳定可复算。
* 发布是带水印的快照事件，内嵌发布时成立的结论与所依据的事件集合，
  研究者可按事件集合重放，还原任一发布日期当时成立的结论。
"""

from __future__ import annotations

import hashlib
import json
import re
import threading
import uuid
from collections import defaultdict
from dataclasses import dataclass, field

SCHEMA = "archaeology-fieldwork/event/1"

# 命令类型 -> 事件类型（在线命令经服务端校验后由服务端补信封）
COMMAND_TO_EVENT = {
    "register_unit": "unit_opened",
    "record_stratum": "stratum_recorded",
    "record_feature": "feature_recorded",
    "register_find": "find_registered",
    "correct_record": "resource_corrected",
    "link_inscription": "inscription_linked",
    "link_photo": "photo_linked",
    "register_sample": "sample_registered",
    "record_dating": "dating_measured",
    "withdraw_dating": "dating_withdrawn",
    "consume_sample": "sample_consumed",
    "custody_event": "custody_recorded",
    "propose_claim": "claim_proposed",
    "review_claim": "claim_reviewed",
    "withdraw_claim": "claim_withdrawn",
    "certify_publication": "publication_certified",
}

CONFIDENCE_ALIASES = {"高": "high", "中": "medium", "低": "low"}
REVIEW_ALIASES = {"支持": "supported", "赞同": "supported", "需修改": "changes_requested",
                  "反对": "rejected", "驳回": "rejected"}
VALID_CONFIDENCE = {"low", "medium", "high"}
VALID_REVIEW = {"supported", "changes_requested", "rejected"}
UNCERTAIN_PATTERN = re.compile(r"可能|疑似|或为|推测|或许|大概|不能排除|有待")
ISO_UTC = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2})?(\.\d+)?Z$")

# 各类资源允许被校正的字段
CORRECTABLE = {
    "unit": {"grid_e", "grid_n"},
    "stratum": {"unit_id", "depth_top", "depth_bottom", "matrix_above"},
    "feature": {"unit_id", "stratum_id", "kind", "geometry"},
    "find": {"find_type", "feature_id", "stratum_id", "coordinates", "catalog_no"},
    "sample": {"material"},
}

FEATURE_KINDS = {"主墓", "兆沟", "陪葬墓", "祭祀坑", "灰坑", "房址", "其他"}
CUSTODY_ACTIONS = {"transfer", "checkout", "return"}


class ValidationError(ValueError):
    """在线命令未通过校验（映射为 HTTP 400）。"""


def envelope(device_id, event_type, payload, clock, happened_at, event_id=None):
    """构造事件信封。离线设备可自带 event_id/clock；在线由服务端补齐。"""
    if not isinstance(device_id, str) or not device_id:
        raise ValidationError("device_id 不能为空")
    if not isinstance(event_type, str) or not event_type:
        raise ValidationError("事件类型不能为空")
    if not isinstance(payload, dict):
        raise ValidationError("payload 必须是对象")
    if not isinstance(clock, int) or clock < 0:
        raise ValidationError("clock 必须是非负整数")
    if not ISO_UTC.match(happened_at or ""):
        raise ValidationError("happened_at 必须是 UTC ISO8601 字符串（以 Z 结尾）")
    return {
        "event_id": event_id or str(uuid.uuid4()),
        "schema": SCHEMA,
        "device_id": device_id,
        "clock": clock,
        "happened_at": happened_at,
        "type": event_type,
        "payload": payload,
    }


def order_key(event):
    """Lamport 全序：(逻辑时钟, 设备 id, 事件 id)。"""
    return (event["clock"], event["device_id"], event["event_id"])


def canonical_json(obj):
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def event_hash(events):
    """对事件集合做稳定哈希（先按全序排序，与送达顺序无关）。"""
    digest = hashlib.sha256()
    for event in sorted(events, key=order_key):
        digest.update(canonical_json(event).encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


# ---------------------------------------------------------------------------
# 事件日志：追加存储 + 归队合并
# ---------------------------------------------------------------------------

class EventLog:
    """内存事件集合，可选 JSONL 追加持久化。

    合并语义：
      * 同 ``event_id`` 视为同一事件，重复送达幂等忽略；
      * 相同 id 但内容冲突（设备伪造/串号）记 ``event_id_conflict``；
      * 返回接受/去重/冲突计数，供归队批次核对。
    """

    def __init__(self, path=None):
        self._events = {}          # event_id -> event
        self.clock = 0             # 服务端在线逻辑时钟
        self._lock = threading.RLock()
        self.path = path
        if path:
            self._load(path)

    def _load(self, path):
        try:
            with open(path, "r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    event = json.loads(line)
                    self._events[event["event_id"]] = event
                    self.clock = max(self.clock, event["clock"])
        except FileNotFoundError:
            pass

    def _append(self, event):
        if not self.path:
            return
        with open(self.path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, ensure_ascii=False) + "\n")

    def merge(self, events):
        """合并一批事件（离线归队），返回汇总报告。"""
        report = {"accepted": [], "duplicated": [], "conflicting": []}
        with self._lock:
            for event in events:
                eid = event.get("event_id")
                if not eid:
                    raise ValidationError("合并事件缺少 event_id")
                existing = self._events.get(eid)
                if existing is not None:
                    if canonical_json(existing) == canonical_json(event):
                        report["duplicated"].append(eid)
                    else:
                        report["conflicting"].append(eid)
                    continue
                self._events[eid] = event
                self.clock = max(self.clock, event["clock"])
                self._append(event)
                report["accepted"].append(eid)
        return report

    def tick(self):
        """在线命令取一个服务端逻辑时钟。"""
        with self._lock:
            self.clock += 1
            return self.clock

    def add_online(self, event):
        """在线命令事件直接入日志（event_id 必然唯一）。"""
        with self._lock:
            self._events[event["event_id"]] = event
            self._append(event)
        return event

    def all(self):
        with self._lock:
            return list(self._events.values())

    def by_ids(self, ids):
        with self._lock:
            return [self._events[i] for i in ids if i in self._events]


# ---------------------------------------------------------------------------
# 投影
# ---------------------------------------------------------------------------

@dataclass
class _Resource:
    kind: str
    ref: str
    current: dict
    versions: list = field(default_factory=list)


class Projector:
    """把事件集合折叠为当前状态。

    折叠按 Lamport 全序进行，但对前置缺失的事件延后重试直至不动点，
    因此最终状态只取决于事件集合，与乱序送达无关。
    """

    def __init__(self):
        self.resources = {}          # ref -> _Resource（探方/地层/遗迹/器物/样本）
        self.inscriptions = {}       # inscription_id -> dict
        self.photos = {}             # photo_id -> dict
        self.datings = {}            # dating_id -> dict
        self.claims = {}             # claim_id -> dict
        self.publications = {}       # publication_id -> dict(snapshot)
        self.custody = {}            # ref -> {"holder":..., "open":..., "chain":[...]}
        self.catalog_index = defaultdict(set)   # catalog_no -> {find_ref}
        self.anomalies = []
        self.event_ids = set()

    # -- 对外入口 ---------------------------------------------------------

    def fold(self, events, as_of=None):
        ordered = sorted(events, key=order_key)
        pending = []
        for event in ordered:
            if as_of is not None and event["happened_at"] > as_of:
                continue
            pending.append(event)
        progressed = True
        while progressed:
            progressed = False
            remaining = []
            for event in pending:
                status = self._apply(event)
                if status == "applied":
                    progressed = True
                elif status == "defer":
                    remaining.append(event)
                # rejected 已在 _apply 内记异常
            pending = remaining
        for event in pending:
            self._anomaly(event, "unresolved_prerequisite",
                          f"事件 {event['type']} 的前置对象在事件集合中始终不存在")
        self.anomalies.sort(key=lambda a: (a["clock"], a["device_id"], a["event_id"], a["code"]))
        return self.state(as_of=as_of)

    # -- 内部 -------------------------------------------------------------

    def _anomaly(self, event, code, message):
        self.anomalies.append({
            "code": code,
            "event_id": event["event_id"],
            "type": event["type"],
            "clock": event["clock"],
            "device_id": event["device_id"],
            "happened_at": event["happened_at"],
            "message": message,
        })

    def _reject(self, event, code, message):
        self._anomaly(event, code, message)
        return "rejected"

    def _apply(self, event):
        if event["event_id"] in self.event_ids:
            return "applied"  # 幂等

        payload = event.get("payload")
        if not isinstance(payload, dict):
            return self._reject(event, "bad_payload", "payload 必须是对象")

        handler = getattr(self, f"_on_{event['type']}", None)
        if handler is None:
            return self._reject(event, "unknown_event_type", f"未知事件类型 {event['type']}")

        status = handler(event, payload)
        if isinstance(status, tuple):  # ("applied", None) / ("rejected", reason) 简写
            status = status[0]
        if status == "applied":
            self.event_ids.add(event["event_id"])
        return status

    def _require_refs(self, *refs):
        return all(ref in self.resources or ref in self.datings
                   or ref in self.inscriptions or ref in self.photos for ref in refs)

    def _register(self, event, kind, ref, data, identity_fields):
        existing = self.resources.get(ref)
        if existing is not None:
            same = all(existing.current.get(key) == data.get(key) for key in identity_fields)
            if same and existing.kind == kind:
                return "applied", None  # 离线重发，幂等
            return self._reject(event, "duplicate_identity_conflict",
                                f"{kind} 编号 {ref} 被不同现场信息重复登记，早期记录保留"), "rejected"
        resource = _Resource(kind=kind, ref=ref, current=dict(data))
        resource.versions.append({
            "version": 1,
            "event_id": event["event_id"],
            "device_id": event["device_id"],
            "happened_at": event["happened_at"],
            "reason": "初次登记",
            "data": dict(data),
        })
        self.resources[ref] = resource
        if kind == "find" and data.get("catalog_no"):
            self.catalog_index[data["catalog_no"]].add(ref)
            if len(self.catalog_index[data["catalog_no"]]) > 1:
                others = sorted(self.catalog_index[data["catalog_no"]] - {ref})
                self._anomaly(event, "duplicate_catalog_number",
                              f"器物编号 {data['catalog_no']} 同时指向 {ref} 与 {others}，"
                              "两方原始记录均保留，需出具编号校正")
        return "applied", None

    # -- 资源登记 ---------------------------------------------------------

    def _on_unit_opened(self, event, p):
        for key in ("unit_id", "grid_e", "grid_n"):
            if key not in p:
                return self._reject(event, "missing_field", f"缺少字段 {key}"), "rejected"
        data = {"grid_e": p["grid_e"], "grid_n": p["grid_n"],
                "opened_at": p.get("opened_at", event["happened_at"])}
        return self._register(event, "unit", p["unit_id"], data, ("grid_e", "grid_n"))

    def _on_stratum_recorded(self, event, p):
        for key in ("stratum_id", "unit_id", "depth_top", "depth_bottom"):
            if key not in p:
                return self._reject(event, "missing_field", f"缺少字段 {key}"), "rejected"
        if p["unit_id"] not in self.resources:
            return "defer", None
        if self.resources[p["unit_id"]].kind != "unit":
            return self._reject(event, "relation_type_error",
                                f"{p['unit_id']} 不是探方"), "rejected"
        data = {"unit_id": p["unit_id"], "depth_top": p["depth_top"],
                "depth_bottom": p["depth_bottom"], "matrix_above": p.get("matrix_above")}
        return self._register(event, "stratum", p["stratum_id"], data,
                              ("unit_id", "depth_top", "depth_bottom"))

    def _on_feature_recorded(self, event, p):
        for key in ("feature_id", "kind", "unit_id"):
            if key not in p:
                return self._reject(event, "missing_field", f"缺少字段 {key}"), "rejected"
        if p["kind"] not in FEATURE_KINDS:
            return self._reject(event, "bad_enum", f"遗迹类别 {p['kind']} 不受支持"), "rejected"
        needed = [p["unit_id"]]
        if p.get("stratum_id"):
            needed.append(p["stratum_id"])
        if not all(ref in self.resources for ref in needed):
            return "defer", None
        data = {"kind": p["kind"], "unit_id": p["unit_id"],
                "stratum_id": p.get("stratum_id"), "geometry": p.get("geometry")}
        return self._register(event, "feature", p["feature_id"], data,
                              ("kind", "unit_id", "stratum_id"))

    def _on_find_registered(self, event, p):
        for key in ("find_id", "find_type"):
            if key not in p:
                return self._reject(event, "missing_field", f"缺少字段 {key}"), "rejected"
        needed = [ref for ref in (p.get("feature_id"), p.get("stratum_id")) if ref]
        if not all(ref in self.resources for ref in needed):
            return "defer", None
        data = {"find_type": p["find_type"], "feature_id": p.get("feature_id"),
                "stratum_id": p.get("stratum_id"), "coordinates": p.get("coordinates"),
                "catalog_no": p.get("catalog_no")}
        return self._register(event, "find", p["find_id"], data,
                              ("find_type", "feature_id", "stratum_id"))

    def _on_resource_corrected(self, event, p):
        ref = p.get("ref")
        changes = p.get("changes")
        if not ref or not isinstance(changes, dict) or not changes:
            return self._reject(event, "missing_field", "校正需要 ref 与非空 changes"), "rejected"
        if ref not in self.resources:
            return "defer", None  # 被校正对象可能在乱序流后面
        resource = self.resources[ref]
        allowed = CORRECTABLE.get(resource.kind, set())
        illegal = set(changes) - allowed
        if illegal:
            return self._reject(event, "illegal_correction",
                                f"{resource.kind} 不允许校正字段 {sorted(illegal)}"), "rejected"
        if not p.get("reason"):
            return self._reject(event, "correction_without_reason", "校正必须注明原因"), "rejected"
        # 关系字段仍需指向现存对象
        relation_fields = {"unit_id", "feature_id", "stratum_id"}
        for key, value in changes.items():
            if key in relation_fields and value is not None and value not in self.resources:
                return "defer", None
        old_catalog = resource.current.get("catalog_no")
        new_data = dict(resource.current)
        new_data.update(changes)
        resource.current = new_data
        resource.versions.append({
            "version": len(resource.versions) + 1,
            "event_id": event["event_id"],
            "device_id": event["device_id"],
            "happened_at": event["happened_at"],
            "reason": p["reason"],
            "changes": dict(changes),
            "data": dict(new_data),
        })
        new_catalog = new_data.get("catalog_no")
        if resource.kind == "find" and new_catalog and new_catalog != old_catalog:
            self.catalog_index[new_catalog].add(ref)
            if len(self.catalog_index[new_catalog]) > 1:
                self._anomaly(event, "duplicate_catalog_number",
                              f"校正后器物编号 {new_catalog} 与 {sorted(self.catalog_index[new_catalog] - {ref})} 冲突")
        return "applied", None

    # -- 铭文 / 照片 ------------------------------------------------------

    def _on_inscription_linked(self, event, p):
        for key in ("inscription_id", "find_id", "text"):
            if key not in p:
                return self._reject(event, "missing_field", f"缺少字段 {key}"), "rejected"
        if p["find_id"] not in self.resources:
            return "defer", None
        if p["inscription_id"] in self.inscriptions:
            existing = self.inscriptions[p["inscription_id"]]
            if existing["find_id"] == p["find_id"] and existing["text"] == p["text"]:
                return "applied"  # 离线重发，幂等
            return self._reject(event, "duplicate_identity_conflict",
                                "铭文 id 重复但文字/所属器物不同，早期记录保留")
        self.inscriptions[p["inscription_id"]] = {
            "inscription_id": p["inscription_id"], "find_id": p["find_id"],
            "text": p["text"], "event_id": event["event_id"]}
        return "applied", None

    def _on_photo_linked(self, event, p):
        for key in ("photo_id", "ref", "coordinates"):
            if key not in p:
                return self._reject(event, "missing_field", f"缺少字段 {key}"), "rejected"
        if p["ref"] not in self.resources:
            return "defer", None
        if p["photo_id"] in self.photos:
            return "applied", None
        self.photos[p["photo_id"]] = {
            "photo_id": p["photo_id"], "ref": p["ref"],
            "coordinates": p["coordinates"], "event_id": event["event_id"]}
        return "applied", None

    # -- 样本、测年、消耗 -------------------------------------------------

    def _on_sample_registered(self, event, p):
        for key in ("sample_id", "material", "mass_initial"):
            if key not in p:
                return self._reject(event, "missing_field", f"缺少字段 {key}"), "rejected"
        if not isinstance(p["mass_initial"], (int, float)) or p["mass_initial"] <= 0:
            return self._reject(event, "bad_quantity", "mass_initial 必须为正数"), "rejected"
        if p.get("find_id") and p["find_id"] not in self.resources:
            return "defer", None
        data = {"material": p["material"], "mass_initial": p["mass_initial"],
                "unit": p.get("unit", "g"), "find_id": p.get("find_id"),
                "balance": p["mass_initial"], "consumed": 0}
        status, _ = self._register(event, "sample", p["sample_id"], data,
                                   ("material", "mass_initial"))
        if status == "applied":
            holder = p.get("holder", "现场临时库房")
            self.custody[p["sample_id"]] = {"holder": holder, "open_checkout": None, "chain": [{
                "action": "register", "counterparty": holder, "at": event["happened_at"],
                "event_id": event["event_id"], "purpose": "样本登记入库"}]}
        return status, None

    def _on_dating_measured(self, event, p):
        for key in ("dating_id", "sample_id", "method", "result"):
            if key not in p:
                return self._reject(event, "missing_field", f"缺少字段 {key}"), "rejected"
        if p["sample_id"] not in self.resources:
            return "defer", None
        if p["dating_id"] in self.datings:
            existing = self.datings[p["dating_id"]]
            if all(existing.get(k) == p.get(k) for k in ("sample_id", "method", "result")):
                return "applied", None
            return self._reject(event, "duplicate_identity_conflict",
                                "测年编号重复但结果不同"), "rejected"
        self.datings[p["dating_id"]] = {
            "dating_id": p["dating_id"], "sample_id": p["sample_id"],
            "method": p["method"], "result": p["result"],
            "lab": p.get("lab"), "measured_at": p.get("measured_at", event["happened_at"]),
            "withdrawn": False, "withdraw_reason": None, "event_id": event["event_id"]}
        return "applied", None

    def _on_dating_withdrawn(self, event, p):
        dating_id = p.get("dating_id")
        if not dating_id:
            return self._reject(event, "missing_field", "缺少 dating_id"), "rejected"
        if dating_id not in self.datings:
            return "defer", None
        if not p.get("reason"):
            return self._reject(event, "withdrawal_without_reason", "测年撤回必须注明原因"), "rejected"
        dating = self.datings[dating_id]
        if dating["withdrawn"]:
            return "applied", None  # 撤回幂等
        dating["withdrawn"] = True
        dating["withdraw_reason"] = p["reason"]
        dating["withdrawn_at"] = event["happened_at"]
        dating["withdraw_event_id"] = event["event_id"]
        return "applied", None

    def _on_sample_consumed(self, event, p):
        sample_id = p.get("sample_id")
        amount = p.get("amount")
        if not sample_id or not isinstance(amount, (int, float)):
            return self._reject(event, "missing_field", "需要 sample_id 与数值 amount"), "rejected"
        if sample_id not in self.resources:
            return "defer", None
        resource = self.resources[sample_id]
        if amount <= 0:
            return self._reject(event, "bad_quantity", "消耗量必须为正数"), "rejected"
        if amount > resource.current["balance"] + 1e-9:
            return self._reject(event, "insufficient_sample",
                                f"申请消耗 {amount}{resource.current['unit']} 超过余额 "
                                f"{resource.current['balance']}{resource.current['unit']}，事件被拒绝"), "rejected"
        resource.current["balance"] = round(resource.current["balance"] - amount, 9)
        resource.current["consumed"] = round(resource.current["consumed"] + amount, 9)
        resource.versions.append({
            "version": len(resource.versions) + 1,
            "event_id": event["event_id"],
            "happened_at": event["happened_at"],
            "reason": p.get("purpose", "样本消耗"),
            "changes": {"consumed_amount": amount},
            "data": dict(resource.current)})
        chain = self.custody.setdefault(sample_id, {"holder": "现场临时库房", "open_checkout": None, "chain": []})
        chain["chain"].append({"action": "consume", "amount": amount,
                               "at": event["happened_at"], "event_id": event["event_id"],
                               "purpose": p.get("purpose")})
        return "applied", None

    # -- 保管链：出库 / 归还 / 跨库移交 -----------------------------------

    def _on_custody_recorded(self, event, p):
        ref, action = p.get("ref"), p.get("action")
        if not ref or action not in CUSTODY_ACTIONS:
            return self._reject(event, "missing_field",
                                f"需要 ref 与 action∈{sorted(CUSTODY_ACTIONS)}"), "rejected"
        if ref not in self.resources:
            return "defer", None
        chain = self.custody.setdefault(ref, {"holder": "现场临时库房", "open_checkout": None, "chain": []})
        at = event["happened_at"]
        if action == "checkout":
            if not p.get("to") or not p.get("purpose"):
                return self._reject(event, "missing_field", "出库需要 to 与 purpose"), "rejected"
            if chain["open_checkout"]:
                return self._reject(event, "checkout_already_open",
                                    f"{ref} 已临时出库未归还，不能重复出库"), "rejected"
            chain["open_checkout"] = {"to": p["to"], "purpose": p["purpose"],
                                      "since": at, "event_id": event["event_id"]}
            chain["chain"].append({"action": "checkout", "counterparty": p["to"],
                                   "purpose": p["purpose"], "at": at,
                                   "event_id": event["event_id"]})
            return "applied", None
        if action == "return":
            if not chain["open_checkout"]:
                return self._reject(event, "return_without_checkout",
                                    f"{ref} 没有未闭环的出库记录"), "rejected"
            checkout = chain["open_checkout"]
            chain["open_checkout"] = None
            chain["chain"].append({"action": "return", "counterparty": checkout["to"],
                                   "purpose": f"归还：{checkout['purpose']}", "at": at,
                                   "linked_event_id": checkout["event_id"],
                                   "event_id": event["event_id"]})
            return "applied", None
        # transfer（跨库移交）
        if not p.get("from_holder") or not p.get("to"):
            return self._reject(event, "missing_field", "移交需要 from_holder 与 to"), "rejected"
        if chain["open_checkout"]:
            return self._reject(event, "transfer_while_checked_out",
                                f"{ref} 临时出库检测期间不得跨库移交"), "rejected"
        if p["from_holder"] != chain["holder"]:
            return self._reject(event, "custody_chain_broken",
                                f"移交方 {p['from_holder']} 不是当前保管方 {chain['holder']}"), "rejected"
        chain["holder"] = p["to"]
        chain["chain"].append({"action": "transfer", "from": p["from_holder"],
                               "to": p["to"], "purpose": p.get("purpose", "跨库移交"),
                               "at": at, "event_id": event["event_id"]})
        return "applied", None

    # -- 竞争性观点与同行评议 ---------------------------------------------

    def _on_claim_proposed(self, event, p):
        for key in ("claim_id", "topic", "subject_ref", "statement", "confidence",
                    "evidence", "proposed_by"):
            if key not in p:
                return self._reject(event, "missing_field", f"缺少字段 {key}"), "rejected"
        evidence = p["evidence"]
        if not isinstance(evidence, list) or not evidence:
            return self._reject(event, "claim_without_evidence", "观点必须引用至少一条具体证据"), "rejected"
        refs = [item["ref"] if isinstance(item, dict) else item for item in evidence]
        if not self._require_refs(*refs):
            return "defer", None
        confidence = CONFIDENCE_ALIASES.get(p["confidence"], p["confidence"])
        if confidence not in VALID_CONFIDENCE:
            return self._reject(event, "bad_enum", f"置信度 {p['confidence']} 不受支持"), "rejected"
        if p["claim_id"] in self.claims:
            return self._reject(event, "duplicate_identity_conflict", "观点 id 重复"), "rejected"
        self.claims[p["claim_id"]] = {
            "claim_id": p["claim_id"], "topic": p["topic"], "subject_ref": p["subject_ref"],
            "statement": p["statement"], "confidence": confidence,
            "confidence_note": p.get("confidence_note"),
            "evidence": [dict(item) if isinstance(item, dict) else {"ref": item} for item in evidence],
            "basis_datings": list(p.get("basis_datings", [])),
            "proposed_by": p["proposed_by"],
            "proposed_at": p.get("proposed_at", event["happened_at"]),
            "event_id": event["event_id"], "status": "active",
            "reviews": []}
        return "applied", None

    def _on_claim_reviewed(self, event, p):
        for key in ("claim_id", "reviewer", "verdict"):
            if key not in p:
                return self._reject(event, "missing_field", f"缺少字段 {key}"), "rejected"
        if p["claim_id"] not in self.claims:
            return "defer", None
        verdict = REVIEW_ALIASES.get(p["verdict"], p["verdict"])
        if verdict not in VALID_REVIEW:
            return self._reject(event, "bad_enum", f"评议结论 {p['verdict']} 不受支持"), "rejected"
        self.claims[p["claim_id"]]["reviews"].append({
            "reviewer": p["reviewer"], "verdict": verdict,
            "comments": p.get("comments"), "at": p.get("reviewed_at", event["happened_at"]),
            "event_id": event["event_id"]})
        return "applied", None

    def _on_claim_withdrawn(self, event, p):
        if p.get("claim_id") not in self.claims:
            return "defer", None
        if not p.get("reason"):
            return self._reject(event, "withdrawal_without_reason", "撤回观点必须注明原因"), "rejected"
        claim = self.claims[p["claim_id"]]
        claim["status"] = "withdrawn"
        claim["withdrawn_reason"] = p["reason"]
        claim["withdrawn_at"] = event["happened_at"]
        return "applied", None

    # -- 发布快照（不可变事件） -------------------------------------------

    def _on_publication_certified(self, event, p):
        pid = p.get("publication_id")
        if not pid:
            return self._reject(event, "missing_field", "缺少 publication_id"), "rejected"
        if pid in self.publications:
            return self._reject(event, "duplicate_identity_conflict", "发布 id 重复"), "rejected"
        snapshot = p.get("snapshot")
        if not isinstance(snapshot, dict):
            return self._reject(event, "missing_field", "发布事件必须内嵌快照"), "rejected"
        self.publications[pid] = snapshot
        return "applied", None

    # -- 状态输出 ---------------------------------------------------------

    def _claim_publishability(self, claim):
        blockers = []
        if claim["status"] != "active":
            blockers.append(f"观点已{claim['status']}")
        if not claim["evidence"]:
            blockers.append("缺少证据")
        evidence_refs = [item["ref"] for item in claim["evidence"]]
        if not self._require_refs(*evidence_refs):
            blockers.append("证据引用无法解析")
        independent = [r for r in claim["reviews"]
                       if r["verdict"] == "supported" and r["reviewer"] != claim["proposed_by"]]
        if not independent:
            blockers.append("缺少提议人之外的同行支持评议")
        withdrawn_basis = [d for d in claim["basis_datings"]
                           if d in self.datings and self.datings[d]["withdrawn"]]
        if withdrawn_basis:
            blockers.append(f"依据测年已撤回：{sorted(withdrawn_basis)}")
        if claim["confidence"] == "high" and UNCERTAIN_PATTERN.search(claim["statement"]):
            blockers.append("高置信度表述含有不确定性措辞，推断不得包装为既定事实")
        return not blockers, blockers, independent

    def _claim_view(self, claim):
        publishable, blockers, independent = self._claim_publishability(claim)
        competitors = sorted(
            cid for cid, other in self.claims.items()
            if cid != claim["claim_id"] and other["status"] == "active"
            and other["topic"] == claim["topic"] and other["subject_ref"] == claim["subject_ref"])
        view = dict(claim)
        view.update({"publishable": publishable, "blockers": blockers,
                     "independent_support_count": len(independent),
                     "competing_claims": competitors, "epistemic": "inference"})
        return view

    def state(self, as_of=None):
        units, strata, features, finds, samples = {}, {}, {}, {}, {}
        for ref, resource in sorted(self.resources.items()):
            view = {"ref": ref, "current": resource.current, "versions": resource.versions}
            bucket = {"unit": units, "stratum": strata, "feature": features,
                      "find": finds, "sample": samples}[resource.kind]
            bucket[ref] = view

        claims = {cid: self._claim_view(claim) for cid, claim in sorted(self.claims.items())}
        assemblages = {}
        for fref, feature in features.items():
            members = sorted(ref for ref, find in finds.items()
                             if find["current"].get("feature_id") == fref)
            assemblages[fref] = members
        stratum_assemblages = {}
        for sref in strata:
            stratum_assemblages[sref] = sorted(
                ref for ref, find in finds.items()
                if find["current"].get("stratum_id") == sref)
        custody = {ref: {"holder": chain["holder"],
                         "open_checkout": chain.get("open_checkout"),
                         "chain": chain["chain"]}
                   for ref, chain in sorted(self.custody.items())}
        return {
            "as_of": as_of,
            "units": units,
            "strata": strata,
            "features": features,
            "finds": finds,
            "samples": samples,
            "inscriptions": dict(sorted(self.inscriptions.items())),
            "photos": dict(sorted(self.photos.items())),
            "datings": dict(sorted(self.datings.items())),
            "claims": claims,
            "assemblages_by_feature": assemblages,
            "assemblages_by_stratum": stratum_assemblages,
            "custody": custody,
            "publications": dict(sorted(self.publications.items())),
            "catalog_conflicts": {no: sorted(refs) for no, refs in sorted(self.catalog_index.items())
                                  if len(refs) > 1},
            "anomalies": self.anomalies,
        }


def fold_events(events, as_of=None):
    """便捷函数：折叠一组事件得到状态。"""
    return Projector().fold(list(events), as_of=as_of)


# ---------------------------------------------------------------------------
# 命令服务：在线校验 + 发布认证
# ---------------------------------------------------------------------------

class FieldworkService:
    def __init__(self, log: EventLog):
        self.log = log
        self._lock = threading.RLock()

    def _project(self):
        return fold_events(self.log.all())

    def project_as_of(self, as_of):
        return fold_events(self.log.all(), as_of=as_of)

    def submit(self, device_id, command, happened_at=None):
        """在线提交一条命令：校验、补信封、入日志，返回事件与当前异常提示。"""
        with self._lock:
            command = dict(command)
            cmd_type = command.pop("type", None)
            if cmd_type not in COMMAND_TO_EVENT:
                raise ValidationError(f"未知命令类型 {cmd_type!r}")
            happened_at = happened_at or command.pop("happened_at", None) or _now_utc()
            event_type = COMMAND_TO_EVENT[cmd_type]
            payload = {k: v for k, v in command.items() if k != "happened_at"}
            self._validate_online(cmd_type, payload)
            event = envelope(device_id, event_type, payload, self.log.tick(), happened_at)
            self.log.add_online(event)
            state = self._project()
            return {"event_id": event["event_id"], "clock": event["clock"],
                    "anomalies": [a for a in state["anomalies"] if a["event_id"] == event["event_id"]]}

    def merge_batch(self, device_id, events):
        """归队一批离线事件，允许只给 type/payload/clock/happened_at 的简写。"""
        with self._lock:
            full = []
            for index, item in enumerate(events):
                if "event_id" in item and "type" in item and "payload" in item:
                    full.append(item)
                    continue
                missing = [k for k in ("type", "payload", "clock", "happened_at") if k not in item]
                if missing:
                    raise ValidationError(f"第 {index} 条简写事件缺少 {missing}，或需提供完整信封")
                full.append(envelope(device_id, item["type"], item["payload"],
                                     item["clock"], item["happened_at"],
                                     item.get("event_id")))
            report = self.log.merge(full)
            state = self._project()
            report["anomaly_count"] = len(state["anomalies"])
            report["log_hash"] = event_hash(self.log.all())
            return report

    def _validate_online(self, cmd_type, p):
        """在线提前拦截明显错误；投影仍对乱序/离线数据做最终裁决。"""
        state = self._project()
        refs = self._all_refs(state)
        if cmd_type == "register_unit":
            if not isinstance(p.get("grid_e"), (int, float)) or not isinstance(p.get("grid_n"), (int, float)):
                raise ValidationError("grid_e/grid_n 必须为数值坐标")
            if p.get("unit_id") in state["units"]:
                raise ValidationError("探方编号已存在；如需修改请走校正")
        elif cmd_type == "record_stratum":
            if p.get("unit_id") not in state["units"]:
                raise ValidationError("所属探方不存在")
            if p.get("stratum_id") in state["strata"]:
                raise ValidationError("地层编号已存在")
            if not _ordered_depths(p):
                raise ValidationError("depth_top 必须浅于 depth_bottom")
        elif cmd_type == "record_feature":
            if p.get("kind") not in FEATURE_KINDS:
                raise ValidationError(f"kind 必须为 {sorted(FEATURE_KINDS)} 之一")
            if p.get("unit_id") not in state["units"]:
                raise ValidationError("所属探方不存在")
            if p.get("stratum_id") and p["stratum_id"] not in state["strata"]:
                raise ValidationError("所属地层不存在")
            if p.get("feature_id") in state["features"]:
                raise ValidationError("遗迹编号已存在")
        elif cmd_type == "register_find":
            if p.get("feature_id") and p["feature_id"] not in state["features"]:
                raise ValidationError("所属遗迹不存在")
            if p.get("stratum_id") and p["stratum_id"] not in state["strata"]:
                raise ValidationError("所属地层不存在")
            if p.get("find_id") in state["finds"]:
                raise ValidationError("器物编号已存在")
        elif cmd_type == "link_inscription":
            if p.get("find_id") not in state["finds"]:
                raise ValidationError("铭文所属器物不存在")
            if not p.get("text"):
                raise ValidationError("铭文文本不能为空")
            if p.get("inscription_id") in state["inscriptions"]:
                raise ValidationError("铭文 id 已存在")
        elif cmd_type == "link_photo":
            if p.get("ref") not in refs:
                raise ValidationError("照片挂载对象不存在")
            if not p.get("coordinates"):
                raise ValidationError("照片必须携带坐标")
        elif cmd_type == "register_sample":
            if p.get("find_id") and p["find_id"] not in state["finds"]:
                raise ValidationError("样本所属器物不存在")
            mass = p.get("mass_initial")
            if not isinstance(mass, (int, float)) or mass <= 0:
                raise ValidationError("mass_initial 必须为正数")
            if p.get("sample_id") in state["samples"]:
                raise ValidationError("样本编号已存在")
        elif cmd_type == "record_dating":
            if p.get("sample_id") not in state["samples"]:
                raise ValidationError("样本不存在")
            if p.get("dating_id") in state["datings"]:
                raise ValidationError("测年编号已存在")
        elif cmd_type == "correct_record":
            ref = p.get("ref")
            if ref not in state["units"] | state["strata"] | state["features"] \
                    | state["finds"] | state["samples"]:
                raise ValidationError(f"被校正对象 {ref} 不存在")
            resource = self._resource(state, ref)
            allowed = CORRECTABLE[resource]
            illegal = set(p.get("changes", {})) - allowed
            if illegal:
                raise ValidationError(f"不允许校正字段 {sorted(illegal)}")
            if not p.get("reason"):
                raise ValidationError("校正必须注明原因")
        elif cmd_type == "propose_claim":
            refs = [item["ref"] if isinstance(item, dict) else item for item in p.get("evidence", [])]
            if not refs:
                raise ValidationError("观点必须引用至少一条证据")
            known = set(state["units"]) | set(state["strata"]) | set(state["features"]) \
                | set(state["finds"]) | set(state["samples"]) | set(state["inscriptions"]) \
                | set(state["photos"]) | set(state["datings"])
            unknown = [r for r in refs if r not in known]
            if unknown:
                raise ValidationError(f"证据引用不存在：{unknown}")
            confidence = CONFIDENCE_ALIASES.get(p.get("confidence"), p.get("confidence"))
            if confidence not in VALID_CONFIDENCE:
                raise ValidationError("confidence 必须为 low/medium/high")
            if confidence == "high" and UNCERTAIN_PATTERN.search(p.get("statement", "")):
                raise ValidationError("表述含不确定性措辞时不得声明 high 置信度")
            if p.get("claim_id") in state["claims"]:
                raise ValidationError("观点 id 已存在")
        elif cmd_type == "review_claim":
            if p.get("claim_id") not in state["claims"]:
                raise ValidationError("被评议观点不存在")
            verdict = REVIEW_ALIASES.get(p.get("verdict"), p.get("verdict"))
            if verdict not in VALID_REVIEW:
                raise ValidationError("verdict 必须为 supported/changes_requested/rejected")
        elif cmd_type == "withdraw_dating":
            if p.get("dating_id") not in state["datings"]:
                raise ValidationError("测年记录不存在")
            if not p.get("reason"):
                raise ValidationError("测年撤回必须注明原因")
        elif cmd_type == "custody_event":
            if p.get("ref") not in self._all_refs(state):
                raise ValidationError("保管对象不存在")
            if p.get("action") not in CUSTODY_ACTIONS:
                raise ValidationError("action 必须为 checkout/return/transfer")
            chain = state["custody"].get(p["ref"])
            if p["action"] == "checkout":
                if chain and chain["open_checkout"]:
                    raise ValidationError("该对象已临时出库未归还")
                if not p.get("to") or not p.get("purpose"):
                    raise ValidationError("出库需要 to 与 purpose")
            elif p["action"] == "return":
                if not chain or not chain["open_checkout"]:
                    raise ValidationError("没有未闭环的出库记录，不能归还")
            else:  # transfer
                if chain and chain["open_checkout"]:
                    raise ValidationError("临时出库检测期间不得跨库移交")
                holder = chain["holder"] if chain else "现场临时库房"
                if p.get("from_holder") != holder:
                    raise ValidationError(f"移交方不是当前保管方 {holder}，保管链断裂")
                if not p.get("to"):
                    raise ValidationError("移交需要 to")
        elif cmd_type == "consume_sample":
            if p.get("sample_id") not in state["samples"]:
                raise ValidationError("样本不存在")
            amount = p.get("amount")
            if not isinstance(amount, (int, float)) or amount <= 0:
                raise ValidationError("消耗量必须为正数")
            balance = state["samples"][p["sample_id"]]["current"]["balance"]
            if amount > balance + 1e-9:
                raise ValidationError(f"消耗 {amount} 超过样本余额 {balance}")

    @staticmethod
    def _all_refs(state):
        refs = set()
        for bucket in ("units", "strata", "features", "finds", "samples"):
            refs |= set(state[bucket])
        return refs

    @staticmethod
    def _resource(state, ref):
        for bucket in ("units", "strata", "features", "finds", "samples"):
            if ref in state[bucket]:
                kind = {"units": "unit", "strata": "stratum", "features": "feature",
                        "finds": "find", "samples": "sample"}[bucket]
                assert state[bucket][ref]["current"] is not None
                return kind
        raise KeyError(ref)

    # -- 发布 -------------------------------------------------------------

    def certify(self, device_id, publication_id, title, happened_at=None):
        """把当前可发布结论固化为不可变快照，并作为事件入日志。"""
        with self._lock:
            happened_at = happened_at or _now_utc()
            state = self._project()
            if publication_id in state["publications"]:
                raise ValidationError("发布 id 已存在")
            conclusions, excluded = [], []
            for claim in state["claims"].values():
                entry = {"claim_id": claim["claim_id"], "topic": claim["topic"],
                         "subject_ref": claim["subject_ref"], "statement": claim["statement"],
                         "confidence": claim["confidence"],
                         "confidence_note": claim.get("confidence_note"),
                         "evidence": claim["evidence"], "proposed_by": claim["proposed_by"],
                         "independent_support": claim["independent_support_count"],
                         "competing_claims": claim["competing_claims"],
                         "epistemic": "inference"}
                if claim["publishable"]:
                    conclusions.append(entry)
                else:
                    entry["blockers"] = claim["blockers"]
                    excluded.append(entry)
            conclusions.sort(key=lambda c: (c["topic"], c["subject_ref"], c["claim_id"]))
            excluded.sort(key=lambda c: c["claim_id"])
            included_ids = sorted(
                e["event_id"] for e in self.log.all()
                if e["type"] != "publication_certified")
            included_events = self.log.by_ids(included_ids)
            snapshot = {
                "publication_id": publication_id,
                "title": title,
                "certified_at": happened_at,
                "schema": SCHEMA,
                "log_hash": event_hash(included_events),
                "included_event_ids": included_ids,
                "conclusions": conclusions,
                "excluded_claims": excluded,
                "sample_balances": {ref: view["current"]["balance"]
                                    for ref, view in sorted(state["samples"].items())},
                "anomaly_codes": sorted({a["code"] for a in state["anomalies"]}),
            }
            payload = {"publication_id": publication_id, "snapshot": snapshot}
            event = envelope(device_id, "publication_certified", payload,
                             self.log.tick(), happened_at)
            self.log.add_online(event)
            return snapshot

    def reconstruct(self, publication_id):
        """按发布快照记载的事件集合重放，独立还原当时结论并核对哈希。"""
        state = self._project()
        snapshot = state["publications"].get(publication_id)
        if snapshot is None:
            raise ValidationError("发布不存在")
        replayed_log = self.log.by_ids(snapshot["included_event_ids"])
        replayed = fold_events(replayed_log)
        replay_hash = event_hash(replayed_log)
        replay_conclusions = [
            {"claim_id": c["claim_id"], "topic": c["topic"], "subject_ref": c["subject_ref"],
             "statement": c["statement"], "confidence": c["confidence"]}
            for c in replayed["claims"].values() if c["publishable"]
        ]
        replay_conclusions.sort(key=lambda c: (c["topic"], c["subject_ref"], c["claim_id"]))
        embedded = [
            {k: c[k] for k in ("claim_id", "topic", "subject_ref", "statement", "confidence")}
            for c in snapshot["conclusions"]
        ]
        return {
            "publication_id": publication_id,
            "certified_at": snapshot["certified_at"],
            "log_hash_matches": replay_hash == snapshot["log_hash"],
            "replay_hash": replay_hash,
            "recorded_hash": snapshot["log_hash"],
            "conclusions_match": replay_conclusions == embedded,
            "replayed_conclusions": replay_conclusions,
            "recorded_conclusions": embedded,
            "sample_balances": {ref: view["current"]["balance"]
                                for ref, view in replayed["samples"].items()},
        }


def _ordered_depths(p):
    top, bottom = p.get("depth_top"), p.get("depth_bottom")
    return isinstance(top, (int, float)) and isinstance(bottom, (int, float)) and top < bottom


def _now_utc():
    import datetime
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
