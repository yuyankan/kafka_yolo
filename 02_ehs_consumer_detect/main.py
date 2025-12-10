import threading

from app.consumers import create_trigger_consumer,process_data_work


cameraid = 4
cameraid_uniqueid = {
    'C11':cameraid
}

batchsize = 20#

def main():
    agents = []
    
    for pl,cameraid in cameraid_uniqueid.items():
        agents_temp = create_trigger_consumer(production_line=pl, cameraid=cameraid) 
        agents.append(agents_temp)
    
    threads = []  
    for agent in agents:
        t = threading.Thread(target=agent.run_batch, args=(process_data_work,batchsize))

        t.start()
        threads.append(t)
        print(f"[INFO] Started worker for production line: {pl}")

    # 主线程保持运行
    for t in threads:
        t.join()

if __name__ == "__main__":
    main()