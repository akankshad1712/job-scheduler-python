import time
import threading
import datetime
from typing import Callable, Optional, List

class ScheduledJob:
    def __init__(self, job_id: int, func: Callable, schedule_type: str,
                 minute: Optional[int] = None,
                 hour: Optional[int] = None,
                 day_of_week: Optional[int] = None):
        self.job_id = job_id
        self.func = func
        self.schedule_type = schedule_type
        self.minute = minute
        self.hour = hour
        self.day_of_week = day_of_week

    def should_run(self, now: datetime.datetime) -> bool:
        if self.schedule_type == 'hourly':
            return now.minute == self.minute
        elif self.schedule_type == 'daily':
            return now.hour == self.hour and now.minute == self.minute
        elif self.schedule_type == 'weekly':
            return (now.weekday() == self.day_of_week and
                    now.hour == self.hour and now.minute == self.minute)
        return False

class JobScheduler:
    def __init__(self):
        self.jobs: List[ScheduledJob] = []
        self.lock = threading.Lock()
        self.running = False

    def add_job(self, job: ScheduledJob):
        with self.lock:
            self.jobs.append(job)
            print(f"✅ Job {job.job_id} scheduled ({job.schedule_type})")

    def run_pending(self):
        now = datetime.datetime.now()
        with self.lock:
            for job in self.jobs:
                if job.should_run(now):
                    threading.Thread(target=job.func).start()

    def start(self):
        print("📅 Job Scheduler started...")
        self.running = True
        while self.running:
            self.run_pending()
            time.sleep(60)

    def stop(self):
        self.running = False
        print("🛑 Scheduler stopped.")

def hello_world():
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] Hello World!")
    with open("job_output.txt", "a") as f:
        f.write(f"[{timestamp}] Hello World!\n")

if __name__ == "__main__":
    scheduler = JobScheduler()
    scheduler.add_job(ScheduledJob(1, hello_world, 'hourly', minute=15))
    scheduler.add_job(ScheduledJob(2, hello_world, 'daily', hour=18, minute=30))
    scheduler.add_job(ScheduledJob(3, hello_world, 'weekly', day_of_week=4, hour=20, minute=0))
    scheduler_thread = threading.Thread(target=scheduler.start)
    scheduler_thread.start()
