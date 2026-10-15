import os
import sys
import re
import socket
import subprocess
import time
import urllib.request
import webbrowser

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
TOOLS_DIR = os.path.join(CURRENT_DIR, "tools")
CLOUDFLARED_EXE = os.path.join(TOOLS_DIR, "cloudflared.exe")
CLOUDFLARED_URL = "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-windows-amd64.exe"
TARGET_PORT = 8000


def check_port_open(port: int = TARGET_PORT) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.8):
            return True
    except Exception:
        return False


def ensure_cloudflared() -> str:
    if not os.path.exists(TOOLS_DIR):
        os.makedirs(TOOLS_DIR, exist_ok=True)

    if os.path.exists(CLOUDFLARED_EXE) and os.path.getsize(CLOUDFLARED_EXE) > 1024 * 1024:
        return CLOUDFLARED_EXE

    print("[*] Dang tai cloudflared ve may...")
    try:
        urllib.request.urlretrieve(CLOUDFLARED_URL, CLOUDFLARED_EXE)
        print("[+] Tai cloudflared thanh cong!")
        return CLOUDFLARED_EXE
    except Exception as e:
        print(f"[!] Khong the tai cloudflared tu dong: {e}")
        return ""


def main():
    print("=" * 70)
    print("   HE THONG PUBLIC WEB_DEMO RA INTERNET (CLOUDFLARE QUICK TUNNEL)  ")
    print("=" * 70)

    # 1. Kiem tra web_demo co dang chay khong
    if not check_port_open(TARGET_PORT):
        print(f"[!] Canh bao: Port {TARGET_PORT} chua mo! web_demo (app.py) chua duoc bat.")
        print("[*] Ban hay mo them 1 terminal chay: python app.py")
        print("[*] Tien trinh se tiep tuc cho den khi web_demo khoi dong...\n")

    # 2. Dam bao cloudflared da co mat
    exe_path = ensure_cloudflared()
    if not exe_path:
        print("[!] Loi: Khong tim thay cloudflared.exe. Vui long kiem tra ket noi mang.")
        sys.exit(1)

    # 3. Khoi dong Cloudflare Tunnel
    cmd = [
        exe_path,
        "tunnel",
        "--url", f"http://127.0.0.1:{TARGET_PORT}",
        "--no-autoupdate"
    ]

    print("[*] Dang ket noi mang luoi Cloudflare Edge toan cau...")
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        encoding="utf-8",
        errors="replace"
    )

    tunnel_url = None
    url_pattern = re.compile(r"https://[a-zA-Z0-9-]+\.trycloudflare\.com")

    try:
        for line in proc.stdout:
            # Tim link trycloudflare
            match = url_pattern.search(line)
            if match and not tunnel_url:
                tunnel_url = match.group(0)
                print("\n" + "=" * 70)
                print(">>> WEB_DEMO DA DUOC PUBLIC RA INTERNET THANH CONG! <<<")
                print("=" * 70)
                print(f"[*] LINK TRUY CAP BAP KY TU NGOAI INTERNET (HTTPS):")
                print(f"    👉  {tunnel_url}")
                print(f"[*] LINK LOCAL:")
                print(f"    👉  http://localhost:{TARGET_PORT}")
                print("=" * 70)
                print("Luu y: Nhan [Ctrl + C] de dong tunnel khi dung xong.\n")

                # Mo trinh duyet tu dong
                try:
                    webbrowser.open(tunnel_url)
                except Exception:
                    pass

            # In tiep log lien quan den request neu can
            if "HTTP" in line or "ERR" in line or "connIndex" in line:
                print(line.strip())

    except KeyboardInterrupt:
        print("\n[*] Dang dong tunnel...")
    finally:
        try:
            proc.terminate()
            proc.wait(timeout=3)
        except Exception:
            proc.kill()
        print("[+] Da dong tunnel an toan.")


if __name__ == "__main__":
    main()

