import time
import threading
from app.work import run_cycle   # your business function

cameraid = 4
cameraid_uniqueid = {
    'C11':cameraid
}


def worker_loop(production_line: str,cameraid:int):
    """单条产线独立循环执行"""
    while True:
        try:
            run_cycle(production_line=production_line,cameraid=cameraid)
        except Exception as e:
            print(f"[ERROR][{production_line}] {e}")
        time.sleep(1)   # 每 1 秒周期


def main():
    threads = []
    
    for pl,cameraid in cameraid_uniqueid.items():
        t = threading.Thread(target=worker_loop, args=(pl,cameraid), daemon=True)
        t.start()
        threads.append(t)
        print(f"[INFO] Started worker for production line: {pl}")

    # 主线程保持运行
    for t in threads:
        t.join()


if __name__ == "__main__":
    main()
