"""领域管道端到端测试：离线合并、乱序、版本校正、竞争性观点、
同行评议与发布门槛、测年撤回、保管链与样本余额、发布时点重建。"""

import json
import os
import random
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from domain import (EventLog, FieldworkService, Projector, ValidationError,
                    envelope, event_hash, fold_events)


def ev(device, clock, etype, payload, happened_at, eid=None):
    return envelope(f"device-{device}", etype, payload, clock, happened_at,
                    eid or f"{device}-{etype}-{clock}-{len(payload)}")


class MergeTests(unittest.TestCase):
    def _two_device_batches(self):
        a = [
            ev("A", 1, "unit_opened", {"unit_id": "T1", "grid_e": 100, "grid_n": 200},
               "2026-03-01T08:00Z", "a1"),
            ev("A", 2, "stratum_recorded",
               {"stratum_id": "T1-L3", "unit_id": "T1", "depth_top": 1.2, "depth_bottom": 1.8},
               "2026-03-01T09:00Z", "a2"),
        ]
        b = [
            ev("B", 1, "unit_opened", {"unit_id": "T2", "grid_e": 110, "grid_n": 210},
               "2026-03-02T08:00Z", "b1"),
            ev("B", 2, "feature_recorded",
               {"feature_id": "M1", "kind": "主墓", "unit_id": "T1", "stratum_id": "T1-L3"},
               "2026-03-02T09:00Z", "b2"),
        ]
        return a + b

    def test_delivery_order_and_chunking_cannot_change_state(self):
        events = self._two_device_batches()
        hashes = set()
        for seed in range(8):
            shuffled = events[:]
            random.Random(seed).shuffle(shuffled)
            log = EventLog()
            midpoint = len(shuffled) // 2
            log.merge(shuffled[:midpoint])
            log.merge(shuffled[midpoint:])  # 跨批次延后事件应被补齐
            state = fold_events(log.all())
            self.assertEqual(state["anomalies"], [])
            self.assertIn("M1", state["features"])
            self.assertEqual(state["features"]["M1"]["current"]["unit_id"], "T1")
            hashes.add(event_hash(log.all()))
        self.assertEqual(len(hashes), 1)  # 事件集合相同 => 哈希相同

    def test_duplicate_events_idempotent_conflicting_duplicate_flagged(self):
        events = self._two_device_batches()
        log = EventLog()
        first = log.merge(events)
        self.assertEqual(len(first["accepted"]), 4)
        again = log.merge(events)
        self.assertEqual(len(again["duplicated"]), 4)
        self.assertEqual(again["conflicting"], [])

        forged = dict(events[0])
        forged["payload"] = {"unit_id": "T1", "grid_e": 999, "grid_n": 999}
        report = log.merge([forged])
        self.assertEqual(report["conflicting"], ["a1"])
        state = fold_events(log.all())
        # 早期现场信息不被后来的覆盖
        self.assertEqual(state["units"]["T1"]["current"]["grid_e"], 100)

    def test_unresolved_prerequisite_is_anomaly_not_silent_drop(self):
        stray = ev("C", 9, "feature_recorded",
                   {"feature_id": "X1", "kind": "祭祀坑", "unit_id": "MISSING"},
                   "2026-04-01T08:00Z", "c9")
        state = fold_events([stray])
        codes = [a["code"] for a in state["anomalies"]]
        self.assertIn("unresolved_prerequisite", codes)
        self.assertNotIn("X1", state["features"])


class VersioningTests(unittest.TestCase):
    def setUp(self):
        self.events = [
            ev("A", 1, "unit_opened", {"unit_id": "T1", "grid_e": 1, "grid_n": 2},
               "2026-03-01T08:00Z", "u"),
            ev("A", 2, "stratum_recorded",
               {"stratum_id": "L2", "unit_id": "T1", "depth_top": 0.5, "depth_bottom": 1.0},
               "2026-03-01T09:00Z", "s"),
            ev("A", 3, "feature_recorded",
               {"feature_id": "F1", "kind": "陪葬墓", "unit_id": "T1", "stratum_id": "L2"},
               "2026-03-01T10:00Z", "f"),
        ]

    def test_correction_creates_version_and_keeps_original(self):
        self.events.append(ev("A", 4, "resource_corrected", {
            "ref": "F1", "changes": {"kind": "祭祀坑"},
            "reason": "二次清理发现葬式与陪葬墓不符"}, "2026-04-01T08:00Z", "fix"))
        state = fold_events(self.events)
        feature = state["features"]["F1"]
        self.assertEqual(feature["current"]["kind"], "祭祀坑")
        self.assertEqual(len(feature["versions"]), 2)
        self.assertEqual(feature["versions"][0]["data"]["kind"], "陪葬墓")  # 原值保留
        self.assertEqual(feature["versions"][1]["reason"], "二次清理发现葬式与陪葬墓不符")

    def test_correction_without_reason_or_illegal_field_rejected(self):
        self.events.append(ev("A", 4, "resource_corrected",
                              {"ref": "F1", "changes": {"opened_at": "x"}},
                              "2026-04-01T08:00Z", "bad1"))
        self.events.append(ev("A", 5, "resource_corrected",
                              {"ref": "F1", "changes": {"kind": "灰坑"}},
                              "2026-04-01T09:00Z", "bad2"))
        state = fold_events(self.events)
        codes = {a["code"] for a in state["anomalies"]}
        self.assertIn("illegal_correction", codes)
        self.assertIn("correction_without_reason", codes)
        self.assertEqual(state["features"]["F1"]["current"]["kind"], "陪葬墓")

    def test_duplicate_number_different_content_preserves_early_record(self):
        impostor = ev("B", 7, "feature_recorded",
                      {"feature_id": "F1", "kind": "主墓", "unit_id": "T1", "stratum_id": "L2"},
                      "2026-05-01T08:00Z", "impostor")
        state = fold_events(self.events + [impostor])
        self.assertEqual(state["features"]["F1"]["current"]["kind"], "陪葬墓")
        self.assertTrue(any(a["code"] == "duplicate_identity_conflict" for a in state["anomalies"]))


class CatalogTests(unittest.TestCase):
    def test_duplicate_catalog_numbers_both_kept_and_flagged(self):
        events = [
            ev("A", 1, "unit_opened", {"unit_id": "T1", "grid_e": 0, "grid_n": 0},
               "2026-03-01T08:00Z", "u"),
            ev("A", 2, "find_registered",
               {"find_id": "Q1", "find_type": "陶鼎", "catalog_no": "2026-001"},
               "2026-03-01T09:00Z", "q1"),
            ev("B", 3, "find_registered",
               {"find_id": "Q2", "find_type": "铜剑", "catalog_no": "2026-001"},
               "2026-03-03T09:00Z", "q2"),
            ev("A", 4, "resource_corrected",
               {"ref": "Q2", "changes": {"catalog_no": "2026-007"}, "reason": "编号笔误更正"},
               "2026-03-04T09:00Z", "fix"),
        ]
        state = fold_events(events)
        self.assertIn("2026-001", state["catalog_conflicts"])  # 冲突历史留痕
        self.assertEqual(state["finds"]["Q1"]["current"]["catalog_no"], "2026-001")
        self.assertEqual(state["finds"]["Q2"]["current"]["catalog_no"], "2026-007")
        # 两份原始记录都可追溯
        self.assertEqual(state["finds"]["Q2"]["versions"][0]["data"]["catalog_no"], "2026-001")


class ClaimAndPublicationTests(unittest.TestCase):
    def setUp(self):
        self.service = FieldworkService(EventLog())
        self.seed()

    def seed(self):
        s = self.service
        s.submit("tablet", {"type": "register_unit", "unit_id": "T1",
                            "grid_e": 0, "grid_n": 0}, "2026-03-01T08:00Z")
        s.submit("tablet", {"type": "record_feature", "feature_id": "M1", "kind": "主墓",
                            "unit_id": "T1"}, "2026-03-01T09:00Z")
        s.submit("tablet", {"type": "register_find", "find_id": "Q1",
                            "find_type": "铜鼎", "feature_id": "M1"}, "2026-03-01T10:00Z")
        s.submit("tablet", {"type": "link_inscription", "inscription_id": "IN1",
                            "find_id": "Q1", "text": "唯王五祀"}, "2026-03-01T11:00Z")
        s.submit("tablet", {"type": "register_sample", "sample_id": "S1",
                            "material": "炭样", "mass_initial": 10.0,
                            "find_id": "Q1"}, "2026-03-02T08:00Z")
        s.submit("lab", {"type": "record_dating", "dating_id": "D1", "sample_id": "S1",
                         "method": "AMS-C14", "result": "2480±30 BP"}, "2026-03-10T08:00Z")

    def _claim(self, cid, statement, confidence="medium", proposer="张"):
        return self.service.submit("office", {
            "type": "propose_claim", "claim_id": cid, "topic": "墓主",
            "subject_ref": "M1", "statement": statement, "confidence": confidence,
            "confidence_note": "单条铭文与测年仅构成间接证据",
            "evidence": [{"ref": "IN1", "note": "铭文称谓"},
                         {"ref": "D1", "note": "碳十四年代"}],
            "basis_datings": ["D1"], "proposed_by": proposer}, "2026-03-12T08:00Z")

    def test_competing_claims_coexist(self):
        self._claim("C1", "墓主可能为某代国君")
        self._claim("C2", "墓主或为卿大夫一级贵族", proposer="李")
        state = fold_events(self.service.log.all())
        c1, c2 = state["claims"]["C1"], state["claims"]["C2"]
        self.assertEqual(c1["competing_claims"], ["C2"])
        self.assertEqual(c2["competing_claims"], ["C1"])
        self.assertTrue(c1["status"] == "active" and c2["status"] == "active")

    def test_claim_needs_independent_support_to_publish(self):
        self._claim("C1", "墓主可能为某代国君")
        state = fold_events(self.service.log.all())
        self.assertFalse(state["claims"]["C1"]["publishable"])
        self.assertIn("缺少提议人之外的同行支持评议", state["claims"]["C1"]["blockers"])

        # 提议人自评不算
        self.service.submit("office", {"type": "review_claim", "claim_id": "C1",
                                       "reviewer": "张", "verdict": "supported"},
                            "2026-03-13T08:00Z")
        state = fold_events(self.service.log.all())
        self.assertFalse(state["claims"]["C1"]["publishable"])

        self.service.submit("office", {"type": "review_claim", "claim_id": "C1",
                                       "reviewer": "王", "verdict": "supported",
                                       "comments": "铭文与年代相容"}, "2026-03-13T09:00Z")
        state = fold_events(self.service.log.all())
        self.assertTrue(state["claims"]["C1"]["publishable"])

    def test_high_confidence_uncertain_wording_is_rejected_online(self):
        with self.assertRaises(ValidationError):
            self._claim("C1", "墓主可能为某代国君", confidence="high")

    def test_claim_without_evidence_rejected(self):
        with self.assertRaises(ValidationError):
            self.service.submit("office", {
                "type": "propose_claim", "claim_id": "C9", "topic": "年代",
                "subject_ref": "M1", "statement": "战国晚期", "confidence": "low",
                "evidence": [], "proposed_by": "张"}, "2026-03-12T08:00Z")

    def test_dating_withdrawal_unpublishes_claim_but_past_publication_survives(self):
        self._claim("C1", "墓主可能为某代国君")
        self.service.submit("office", {"type": "review_claim", "claim_id": "C1",
                                       "reviewer": "王", "verdict": "supported"},
                            "2026-03-13T08:00Z")
        snapshot = self.service.certify("office", "P-2026-01",
                                        "一季度阶段结论", "2026-03-15T00:00Z")
        self.assertEqual([c["claim_id"] for c in snapshot["conclusions"]], ["C1"])
        self.assertTrue(all(c["confidence"] in ("low", "medium", "high")
                            for c in snapshot["conclusions"]))

        # 实验室撤回测年
        self.service.submit("lab", {"type": "withdraw_dating", "dating_id": "D1",
                                    "reason": "制样污染，结果作废"}, "2026-04-01T08:00Z")
        state = fold_events(self.service.log.all())
        self.assertTrue(state["datings"]["D1"]["withdrawn"])
        self.assertFalse(state["claims"]["C1"]["publishable"])
        self.assertTrue(any("已撤回" in b for b in state["claims"]["C1"]["blockers"]))

        # 还原发布当时：撤回事件不在快照事件集合中，C1 仍然成立
        replay = self.service.reconstruct("P-2026-01")
        self.assertTrue(replay["log_hash_matches"])
        self.assertTrue(replay["conclusions_match"])
        self.assertEqual([c["claim_id"] for c in replay["replayed_conclusions"]], ["C1"])

    def test_excluded_claims_never_published_as_fact(self):
        self._claim("C1", "墓主可能为某代国君")  # 无独立评议
        snapshot = self.service.certify("office", "P-2026-02", "谨慎版", "2026-03-15T00:00Z")
        self.assertEqual(snapshot["conclusions"], [])
        self.assertEqual([c["claim_id"] for c in snapshot["excluded_claims"]], ["C1"])


class CustodyAndSampleTests(unittest.TestCase):
    def setUp(self):
        self.service = FieldworkService(EventLog())
        s = self.service
        s.submit("t", {"type": "register_sample", "sample_id": "S1",
                       "material": "骨骼", "mass_initial": 5.0,
                       "holder": "遗址中心库"}, "2026-03-01T08:00Z")

    def test_over_consumption_rejected_balance_stays_stable(self):
        with self.assertRaises(ValidationError):
            self.service.submit("lab", {"type": "consume_sample", "sample_id": "S1",
                                        "amount": 6.0, "purpose": "制样"},
                                "2026-03-02T08:00Z")
        state = fold_events(self.service.log.all())
        self.assertEqual(state["samples"]["S1"]["current"]["balance"], 5.0)
        self.service.submit("lab", {"type": "consume_sample", "sample_id": "S1",
                                    "amount": 2.0, "purpose": "制样"},
                            "2026-03-02T09:00Z")
        state = fold_events(self.service.log.all())
        self.assertEqual(state["samples"]["S1"]["current"]["balance"], 3.0)
        self.assertEqual(state["samples"]["S1"]["current"]["consumed"], 2.0)

    def test_checkout_blocks_transfer_and_return_closes_loop(self):
        self.service.submit("lab", {"type": "custody_event", "ref": "S1",
                                    "action": "checkout", "to": "省考古院实验室",
                                    "purpose": "临时出库做 C14"}, "2026-03-03T08:00Z")
        # 出库期间跨库移交必须拒绝
        from domain import fold_events as _fold
        bad = envelope("t2", "custody_recorded",
                       {"ref": "S1", "action": "transfer",
                        "from_holder": "遗址中心库", "to": "国家博物馆"},
                       5, "2026-03-04T08:00Z", "bad-transfer")
        state = _fold(self.service.log.all() + [bad])
        self.assertTrue(any(a["code"] == "transfer_while_checked_out"
                            for a in state["anomalies"]))

        self.service.submit("lab", {"type": "custody_event", "ref": "S1",
                                    "action": "return"}, "2026-03-10T08:00Z")
        # 非当前保管方移交：链条断裂，拒绝
        with self.assertRaises(ValidationError):
            self.service.submit("office", {"type": "custody_event", "ref": "S1",
                                           "action": "transfer",
                                           "from_holder": "某高校", "to": "国家博物馆"},
                                "2026-03-11T08:00Z")
        self.service.submit("office", {"type": "custody_event", "ref": "S1",
                                       "action": "transfer",
                                       "from_holder": "遗址中心库",
                                       "to": "国家博物馆",
                                       "purpose": "跨库移交"}, "2026-03-12T08:00Z")
        state = fold_events(self.service.log.all())
        self.assertEqual(state["custody"]["S1"]["holder"], "国家博物馆")
        actions = [step["action"] for step in state["custody"]["S1"]["chain"]]
        self.assertEqual(actions, ["register", "checkout", "return", "transfer"])
        self.assertIsNone(state["custody"]["S1"]["open_checkout"])

    def test_double_return_rejected(self):
        rogue = envelope("t2", "custody_recorded",
                         {"ref": "S1", "action": "return"}, 3, "2026-03-03T08:00Z", "r1")
        state = fold_events(self.service.log.all() + [rogue])
        self.assertTrue(any(a["code"] == "return_without_checkout" for a in state["anomalies"]))


class AssemblageAndTraceTests(unittest.TestCase):
    def test_assemblage_and_photo_coordinates_trace_space(self):
        events = [
            ev("A", 1, "unit_opened", {"unit_id": "T1", "grid_e": 0, "grid_n": 0},
               "2026-03-01T08:00Z", "u"),
            ev("A", 2, "stratum_recorded",
               {"stratum_id": "L1", "unit_id": "T1", "depth_top": 0.2, "depth_bottom": 0.9},
               "2026-03-01T09:00Z", "l"),
            ev("A", 3, "feature_recorded",
               {"feature_id": "M1", "kind": "主墓", "unit_id": "T1", "stratum_id": "L1",
                "geometry": {"type": "polygon"}}, "2026-03-01T10:00Z", "m"),
            ev("A", 4, "find_registered",
               {"find_id": "Q1", "find_type": "陶簋", "feature_id": "M1",
                "stratum_id": "L1", "coordinates": {"x": 1.2, "y": 3.4}},
               "2026-03-01T11:00Z", "q1"),
            ev("A", 5, "find_registered",
               {"find_id": "Q2", "find_type": "石圭", "feature_id": "M1",
                "coordinates": {"x": 1.5, "y": 3.1}}, "2026-03-01T11:30Z", "q2"),
            ev("B", 6, "photo_linked",
               {"photo_id": "P1", "ref": "Q1",
                "coordinates": {"x": 1.2, "y": 3.4, "z": -0.65}},
               "2026-03-01T12:00Z", "p1"),
        ]
        # 照片事件故意排在登记之前，验证乱序延后
        shuffled = [events[5]] + events[:5]
        state = fold_events(shuffled)
        self.assertEqual(state["anomalies"], [])
        self.assertEqual(state["assemblages_by_feature"]["M1"], ["Q1", "Q2"])
        self.assertEqual(state["assemblages_by_stratum"]["L1"], ["Q1"])
        self.assertEqual(state["photos"]["P1"]["ref"], "Q1")
        self.assertEqual(state["photos"]["P1"]["coordinates"]["z"], -0.65)
        # 器物 → 遗迹 → 地层 → 探方的层位链全部可回溯
        q1 = state["finds"]["Q1"]["current"]
        self.assertEqual(state["features"][q1["feature_id"]]["current"]["stratum_id"], "L1")
        self.assertEqual(state["strata"]["L1"]["current"]["unit_id"], "T1")

    def test_withdrawal_arriving_before_measurement_is_deferred(self):
        measure = ev("lab", 10, "dating_measured",
                     {"dating_id": "D1", "sample_id": "S1", "method": "OSL",
                      "result": "2.1ka"}, "2026-05-01T08:00Z", "d1")
        register = ev("A", 1, "sample_registered",
                      {"sample_id": "S1", "material": "土样", "mass_initial": 3.0},
                      "2026-04-01T08:00Z", "s1")
        withdraw = ev("lab", 11, "dating_withdrawn",
                      {"dating_id": "D1", "reason": "仪器故障"}, "2026-06-01T08:00Z", "w1")
        # 完全逆序送达：撤回 → 测年 → 登记
        state = fold_events([withdraw, measure, register])
        self.assertEqual(state["anomalies"], [])
        self.assertTrue(state["datings"]["D1"]["withdrawn"])
        self.assertEqual(state["datings"]["D1"]["withdraw_reason"], "仪器故障")


class AsOfTests(unittest.TestCase):
    def test_as_of_reconstructs_earlier_state(self):
        service = FieldworkService(EventLog())
        service.submit("t", {"type": "register_unit", "unit_id": "T1",
                             "grid_e": 1, "grid_n": 1}, "2026-01-01T00:00Z")
        service.submit("t", {"type": "register_unit", "unit_id": "T2",
                             "grid_e": 2, "grid_n": 2}, "2026-06-01T00:00Z")
        early = service.project_as_of("2026-02-01T00:00Z")
        self.assertIn("T1", early["units"])
        self.assertNotIn("T2", early["units"])


class PersistenceTests(unittest.TestCase):
    def test_jsonl_log_survives_restart(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "events.jsonl")
            service = FieldworkService(EventLog(path))
            service.submit("t", {"type": "register_unit", "unit_id": "T1",
                                 "grid_e": 1, "grid_n": 1}, "2026-01-01T00:00Z")
            reopened = FieldworkService(EventLog(path))
            state = fold_events(reopened.log.all())
            self.assertIn("T1", state["units"])
            # 重复加载同一文件不会产生重复事件
            self.assertEqual(len(reopened.log.all()), 1)


class HttpApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), __import__("service").Handler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=2)

    def _post(self, path, payload):
        req = Request(f"{self.base}{path}", json.dumps(payload).encode("utf-8"),
                      {"Content-Type": "application/json"}, method="POST")
        with urlopen(req, timeout=3) as response:
            return response.status, json.load(response)

    def test_full_flow_over_http(self):
        status, merged = self._post("/merge", {"device_id": "tablet-7", "events": [
            {"type": "unit_opened", "clock": 1, "happened_at": "2026-03-01T08:00Z",
             "payload": {"unit_id": "T1", "grid_e": 10, "grid_n": 20}},
        ]})
        self.assertEqual(status, 200)
        self.assertEqual(len(merged["accepted"]), 1)

        status, result = self._post("/commands", {
            "device_id": "tablet-7",
            "command": {"type": "record_feature", "feature_id": "M1",
                        "kind": "主墓", "unit_id": "T1"},
            "happened_at": "2026-03-01T09:00Z"})
        self.assertEqual(status, 201)
        self.assertIn("event_id", result)

        with urlopen(f"{self.base}/state", timeout=3) as response:
            state = json.load(response)
        self.assertIn("M1", state["features"])

        status, pub = self._post("/publications", {
            "device_id": "office", "publication_id": "P1",
            "title": "HTTP 冒烟发布", "happened_at": "2026-03-20T00:00Z"})
        self.assertEqual(status, 201)
        self.assertEqual(pub["publication_id"], "P1")

        with urlopen(f"{self.base}/publications/P1/reconstruction", timeout=3) as response:
            replay = json.load(response)
        self.assertTrue(replay["log_hash_matches"])
        self.assertTrue(replay["conclusions_match"])

    def test_validation_error_is_400(self):
        req = Request(f"{self.base}/commands",
                      json.dumps({"command": {"type": "record_feature",
                                              "feature_id": "Z", "kind": "主墓",
                                              "unit_id": "NOPE"}}).encode("utf-8"),
                      {"Content-Type": "application/json"}, method="POST")
        with self.assertRaises(HTTPError) as error:
            urlopen(req, timeout=3)
        self.assertEqual(error.exception.code, 400)
        error.exception.close()


if __name__ == "__main__":
    unittest.main()
