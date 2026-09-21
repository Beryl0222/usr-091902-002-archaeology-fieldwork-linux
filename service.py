"""考古现场关系管理的运行入口。

路由：
  GET  /health                          健康检查（契约保持不变）
  POST /commands                        在线提交一条命令（服务端补逻辑时钟）
  POST /merge                           离线记录归队合并（按设备+逻辑时钟）
  GET  /state[?as_of=2026-09-01T00:00Z] 当前状态（或某时点状态）
  POST /publications                    固化发布快照
  GET  /publications/<id>               读取发布快照
  GET  /publications/<id>/reconstruction 按事件集合独立重放，还原当时结论
"""

import argparse
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

from domain import EventLog, FieldworkService, ValidationError, fold_events

SERVICE_ID = "archaeology-fieldwork"
SERVICE_NAME = "考古现场关系管理"


def health_payload():
    """返回稳定的服务身份信息。"""
    return {"status": "ok", "service": SERVICE_ID, "name": SERVICE_NAME}


def build_service(log_path=None):
    return FieldworkService(EventLog(log_path))


class Handler(BaseHTTPRequestHandler):
    """提供健康检查与领域接口，供本地联调和运维巡检使用。"""

    service = build_service(os.environ.get("FIELDWORK_LOG"))

    # -- 基础工具 ---------------------------------------------------------

    def _send_json(self, status, payload):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self):
        length = int(self.headers.get("Content-Length") or 0)
        if length == 0:
            return {}
        return json.loads(self.rfile.read(length).decode("utf-8"))

    def _error(self, status, code, message):
        self._send_json(status, {"error": code, "message": message})

    # -- GET --------------------------------------------------------------

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        if path == "/health":
            self._send_json(200, health_payload())
            return
        if path == "/state":
            as_of = (parse_qs(parsed.query).get("as_of") or [None])[0]
            try:
                self._send_json(200, self.service.project_as_of(as_of) if as_of
                                else fold_events(self.service.log.all()))
            except Exception as exc:  # noqa: BLE001 - 非法时点参数
                self._error(400, "bad_as_of", str(exc))
            return
        if path.startswith("/publications/"):
            rest = path[len("/publications/"):]
            if rest.endswith("/reconstruction"):
                publication_id = rest[: -len("/reconstruction")]
                self._call(lambda: self.service.reconstruct(publication_id))
                return
            self._call(lambda: self._publication(rest))
            return
        self.send_error(404)

    def _publication(self, publication_id):
        state = fold_events(self.service.log.all())
        snapshot = state["publications"].get(publication_id)
        if snapshot is None:
            raise ValidationError("发布不存在")
        return snapshot

    # -- POST -------------------------------------------------------------

    def do_POST(self):
        try:
            data = self._read_json()
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            self._error(400, "bad_json", f"请求体不是合法 JSON：{exc}")
            return
        path = urlparse(self.path).path
        if path == "/commands":
            def action():
                device_id = data.get("device_id", "server")
                return self.service.submit(device_id, data["command"], data.get("happened_at"))
            self._call(action, status=201)
        elif path == "/merge":
            def action():
                return self.service.merge_batch(data.get("device_id", "offline-device"),
                                                data.get("events", []))
            self._call(action)
        elif path == "/publications":
            def action():
                return self.service.certify(
                    data.get("device_id", "server"), data["publication_id"],
                    data["title"], data.get("happened_at"))
            self._call(action, status=201)
        else:
            self.send_error(404)

    def _call(self, action, status=200):
        try:
            self._send_json(status, action())
        except ValidationError as exc:
            self._error(400, "validation_failed", str(exc))
        except KeyError as exc:
            self._error(400, "missing_field", f"缺少字段 {exc.args[0]}")

    def log_message(self, *_args):
        return


def main():
    parser = argparse.ArgumentParser(description=SERVICE_NAME)
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--log", help="事件 JSONL 追加存储路径")
    args = parser.parse_args()
    if args.check:
        assert health_payload()["service"] == SERVICE_ID
        # 冒烟：核心管道在空日志上可折叠
        assert fold_events([])["anomalies"] == []
        print("基础检查通过")
        return
    Handler.service = build_service(args.log or os.environ.get("FIELDWORK_LOG"))
    ThreadingHTTPServer(("0.0.0.0", args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
