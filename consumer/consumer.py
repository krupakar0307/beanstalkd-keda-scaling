#!/usr/bin/env python3
import os
import time
import beanstalkc
import threading
from datetime import datetime

BEANSTALK_HOST = os.getenv('BEANSTALK_HOST', 'localhost:11300')
TUBE_NAME = os.getenv('TUBE_NAME', 'orders')
WORKERS = int(os.getenv('WORKERS', '10'))  # Number of reserved jobs to maintain
PROCESS_SECONDS = int(os.getenv('PROCESS_SECONDS', '10'))  # Processing time per job
INITIAL_DELAY = float(os.getenv('INITIAL_DELAY', '2'))  # Stagger worker starts

def log(message):
    print(f"[{datetime.now().strftime('%H:%M:%S.%f')}] {message}")

def worker(worker_id):
    """Single worker maintaining one reserved job"""
    host, port = BEANSTALK_HOST.split(':')
    conn = beanstalkc.Connection(host=host, port=int(port))
    conn.watch(TUBE_NAME)
    
    # Stagger initial connections
    time.sleep(worker_id * INITIAL_DELAY)
    
    while True:
        try:
            job = conn.reserve(timeout=5)
            if job:
                log(f"WORKER-{worker_id} RESERVED JOB {job.jid}")
                process_job(job)
                log(f"WORKER-{worker_id} RELEASED JOB {job.jid}")
            else:
                time.sleep(1)
        except Exception as e:
            log(f"WORKER-{worker_id} ERROR: {str(e)}")
            time.sleep(5)

def process_job(job):
    """Process individual job with state tracking"""
    try:
        # Simulate work
        for i in range(PROCESS_SECONDS, 0, -1):
            log(f"PROCESSING JOB {job.jid} ({i}s remaining)")
            time.sleep(1)
            
        job.delete()
    except Exception as e:
        log(f"JOB {job.jid} FAILED: {str(e)}")
        job.bury()

def monitor_stats():
    """Separate thread for real-time stats"""
    host, port = BEANSTALK_HOST.split(':')
    conn = beanstalkc.Connection(host=host, port=int(port))
    
    while True:
        try:
            stats = conn.stats_tube(TUBE_NAME)
            stats_dict = {}
            for line in stats.split('\n'):
                if ':' in line:
                    key, value = line.split(':', 1)
                    stats_dict[key.strip()] = value.strip()
            
            log("\n=== REAL-TIME STATS ===")
            log(f"READY: {stats_dict.get('current-jobs-ready', 0)}")
            log(f"RESERVED: {stats_dict.get('current-jobs-reserved', 0)}")
            log(f"ACTIVE WORKERS: {threading.active_count() - 2}")  # Subtract main + monitor
            log("======================\n")
            
        except Exception as e:
            log(f"MONITOR ERROR: {str(e)}")
        
        time.sleep(3)

if __name__ == "__main__":
    log(f"Starting {WORKERS} workers (target reserved jobs)")
    
    # Start monitoring thread
    threading.Thread(target=monitor_stats, daemon=True).start()
    
    # Start worker pool
    for i in range(WORKERS):
        threading.Thread(target=worker, args=(i,), daemon=True).start()
    
    # Keep main thread alive
    while True:
        time.sleep(10)

##PROCESS_SECONDS=1  GAP_SECONDS=0 BEANSTALK_HOST="localhost:11300" WORKERS=6 python3 con-par.py  - to run with reserved jobs
##PROCESS_SECONDS=1  GAP_SECONDS=0 BEANSTALK_HOST="localhost:11300" WORKERS=6 python3 con-par.py  - to make burry - replace int with float for PROCESS_SECONDS and value to be 1.5# 

