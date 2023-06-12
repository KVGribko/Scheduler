from typing import Generator

from job import Job, JobStatus
from log import logger
import pickle


class Scheduler:
    def __init__(self, max_jobs: int = 10):
        self.max_jobs: int = max_jobs
        self.running_jobs: list[Job] = []
        self.pending_jobs: list[Job] = []
        self.completed_jobs: list[Job] = []
        self.failed_jobs: list[Job] = []

    def add_job(self, job: Job) -> None:
        self.pending_jobs.append(job)

    def run(self) -> Generator:
        while self.running_jobs or self.pending_jobs:
            print(self.running_jobs)
            print(self.pending_jobs)
            self._exec_running_jobs()
            self._run_pending_jobs()
            yield

    def _exec_running_jobs(self) -> None:
        waiting_jobs: list[Job] = []
        while self.running_jobs:
            job, job_gen = self.running_jobs.pop(0)
            try:
                next(job_gen)
            except StopIteration:
                if job.status == JobStatus.COMPLETED:
                    self.completed_jobs.append(job)
                    logger.info(f"Задача {job.task.__name__} выполнена.")
                elif job.status == JobStatus.ERROR:
                    self.failed_jobs.append(job)
                    logger.error(f"Задача {job.task.__name__} завершилась с ошибкой.")
            except Exception as e:
                self.failed_jobs.append(job)
                logger.error(
                    f"Произошла ошибка при выполнении задачи {job.task.__name__}"
                )
                logger.error(f"Ошибка: {e}")
            if job.status == JobStatus.RUNNING:
                self.running_jobs.append((job, job_gen))
            if job.status in [JobStatus.WAIT_DEPENDENCIES, JobStatus.WAIT_TIME]:
                waiting_jobs.append((job, job_gen))
        self.running_jobs.extend(waiting_jobs)

    def _run_pending_jobs(self) -> None:
        while self.pending_jobs and len(self.running_jobs) <= self.max_jobs:
            job = self.pending_jobs.pop(0)
            job_gen = job.run()
            self.running_jobs.append((job, job_gen))
            logger.info(f"Задача {job.task.__name__} запущена.")

    def save_state(self, file_path: str) -> None:
        state = {
            "max_jobs": self.max_jobs,
            "running_jobs": self.running_jobs,
            "pending_jobs": self.pending_jobs,
            "completed_jobs": self.completed_jobs,
            "failed_jobs": self.failed_jobs,
        }
        with open(file_path, "wb") as file:
            pickle.dump(state, file)

    def load_state(self, file_path: str) -> None:
        with open(file_path, "rb") as file:
            state = pickle.load(file)
        self.max_jobs = state["max_jobs"]
        self.running_jobs = state["running_jobs"]
        self.pending_jobs = state["pending_jobs"]
        self.completed_jobs = state["completed_jobs"]
        self.failed_jobs = state["failed_jobs"]
