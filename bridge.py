
import json
import threading
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
from socketserver import ThreadingTCPServer, StreamRequestHandler

HTTP_PORT = 8000
TCP_PORT = 5000

data_lock = threading.Lock()

latest_data = {
    "cycle": 0,
    "source": "WAITING_FOR_QNX",
    "patients": [
        {
            "id": i,
            "spo2": 98,
            "hr": 80,
            "rr": 16,
            "temp": 36.8,
            "fall": False,
            "status": "NORMAL",
            "priority": 0,
            "reason": "Waiting for QNX data"
        }
        for i in range(1, 5)
    ]
}



class DashboardHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=r"F:\medi_room", **kwargs)

    def do_GET(self):
        if self.path.split("?")[0] == "/api/data":
            with data_lock:
                payload = json.dumps(latest_data).encode("utf-8")

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(payload)
            return

        if self.path.split("?")[0] == "/":
            self.path = "/index.html"

        super().do_GET()

    def log_message(self, fmt, *args):
        print("[HTTP]", fmt % args)


class QNXDataHandler(StreamRequestHandler):
    def handle(self):
        client = self.client_address
        print("QNX/data client connected:", client)

        try:
            while True:
                # QNX sends one JSON object per line.
                line = self.rfile.readline(65536)

                if not line:
                    break

                try:
                    incoming = json.loads(line.decode("utf-8"))

                    # Basic structural checks before accepting data.
                    if not isinstance(incoming, dict):
                        raise ValueError("JSON root must be an object")

                    if not isinstance(incoming.get("patients"), list):
                        raise ValueError("'patients' must be a list")

                    if len(incoming["patients"]) != 4:
                        raise ValueError("Expected exactly 4 patients")

                    with data_lock:
                        latest_data.clear()
                        latest_data.update(incoming)

                    print(
                        "Received cycle:",
                        incoming.get("cycle"),
                        "| source:",
                        incoming.get("source", "UNSPECIFIED")
                    )

                    self.wfile.write(b"OK\n")
                    self.wfile.flush()

                except (ValueError, UnicodeDecodeError) as error:
                    print("Rejected data:", error)
                    self.wfile.write(b"ERROR\n")
                    self.wfile.flush()

        except (ConnectionError, OSError) as error:
            print("Client connection ended:", error)

        print("QNX/data client disconnected:", client)


class ReusableTCPServer(ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


if __name__ == "__main__":
    print("MediSentinel Edge bridge starting...")
    print("Dashboard: http://127.0.0.1:8000")
    print("Data API:  http://127.0.0.1:8000/api/data")
    print("QNX TCP port:", TCP_PORT)
    print("Waiting for QNX data...")

    # Serve files from the folder containing bridge.py.
    http_server = ThreadingHTTPServer(
        ("0.0.0.0", HTTP_PORT),
        DashboardHandler
    )

    tcp_server = ReusableTCPServer(
        ("0.0.0.0", TCP_PORT),
        QNXDataHandler
    )

    threading.Thread(
        target=http_server.serve_forever,
        daemon=True
    ).start()

    try:
        tcp_server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping bridge...")
    finally:
        tcp_server.server_close()
        http_server.shutdown()
        http_server.server_close()