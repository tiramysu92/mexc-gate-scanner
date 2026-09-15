"""Read-only HTTP dashboard, bound to localhost by default."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from .journal import dumps


def server(service, host, port):
    html = (Path(__file__).with_name("dashboard.html")).read_bytes()
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/api/status":
                body = dumps(service.status()).encode()
                kind = "application/json"
            elif self.path == "/":
                body,kind = html,"text/html; charset=utf-8"
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type",kind)
            self.send_header("Cache-Control","no-store")
            self.send_header("Content-Length",str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        def log_message(self,*args):
            pass
    return ThreadingHTTPServer((host,port),Handler)
