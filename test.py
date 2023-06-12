import unittest
from datetime import datetime, time, timedelta
from time import sleep

from job import Job, JobStatus
from scheduler import Scheduler


def normal_task():
    yield "Result 1"
    yield "Result 2"


def long_task():
    while True:
        sleep(6)
        yield


def error_task():
    while True:
        yield
        raise KeyError


class JobTestCase(unittest.TestCase):
    def setUp(self):
        self.job = Job(normal_task)

    def test_check_time_with_start_time_none(self):
        """Проверяет метод __check_time() при start_time = None."""
        self.job.start_time = None
        self.assertTrue(self.job._Job__check_time())

    def test_check_time_with_start_time_before_current_time(self):
        """Проверяет метод __check_time() при start_time до текущего времени."""
        now = time.fromisoformat(datetime.now().strftime("%H:%M:%S"))
        self.job.start_time = now
        sleep(1)
        self.assertTrue(self.job._Job__check_time())

    def test_check_time_with_start_time_after_current_time(self):
        """Проверяет метод __check_time() при start_time после текущего времени."""
        future = datetime.now() + timedelta(minutes=10)
        future = time.fromisoformat(future.strftime("%H:%M:%S"))
        self.job.start_time = future
        self.assertFalse(self.job._Job__check_time())

    def test_check_dependencies_with_no_dependencies(self):
        """Проверяет метод __check_dependencies() без зависимостей."""
        self.assertTrue(self.job._Job__check_dependencies())

    def test_check_dependencies_with_completed_dependencies(self):
        """Проверяет метод __check_dependencies() с завершенными зависимостями."""
        dependency1 = Job(normal_task)
        dependency2 = Job(normal_task)
        dependency1.status = JobStatus.COMPLETED
        dependency2.status = JobStatus.COMPLETED
        self.job.dependencies = [dependency1, dependency2]
        self.assertTrue(self.job._Job__check_dependencies())

    def test_check_dependencies_with_incomplete_dependencies(self):
        """Проверяет метод __check_dependencies() с незавершенными зависимостями."""
        dependency1 = Job(normal_task)
        dependency2 = Job(normal_task)
        dependency3 = Job(normal_task)
        dependency4 = Job(normal_task)
        dependency1.status = JobStatus.PENDING
        dependency2.status = JobStatus.RUNNING
        dependency3.status = JobStatus.ERROR
        dependency4.status = JobStatus.COMPLETED
        self.job.dependencies = [dependency1, dependency2, dependency3, dependency4]
        self.assertFalse(self.job._Job__check_dependencies())

    def test_run_successful_execution(self):
        """Проверяет успешное выполнение задачи в методе run()."""
        gen = self.job.run()
        for _ in range(100):
            try:
                next(gen)
            except StopIteration:
                break
        self.assertEqual(self.job.status, JobStatus.COMPLETED)

    def test_run_task_error(self):
        """Проверяет выполнение задачи с ошибкой в методе run()."""
        self.job.task = error_task
        gen = self.job.run()
        for _ in range(100):
            try:
                next(gen)
            except StopIteration:
                break
        self.assertEqual(self.job.status, JobStatus.ERROR)

    def test_run_exceeds_duration(self):
        """Проверяет превышение длительности выполнения в методе run()."""
        self.job.duration = timedelta(seconds=5)
        self.job.task = long_task
        gen = self.job.run()
        for _ in range(100):
            try:
                next(gen)
            except StopIteration:
                break
        self.assertEqual(self.job.status, JobStatus.ERROR)

    def test_run_exceeds_restart_limit(
        self,
    ):
        """Проверяет превышение количества рестартов в методе run()."""
        self.job.restarts = 5
        self.job.task = error_task
        gen = self.job.run()
        for _ in range(100):
            try:
                next(gen)
            except StopIteration:
                break
        self.assertEqual(self.job.status, JobStatus.ERROR)
        self.assertEqual(self.job.count_restarts, self.job.restarts + 1)

    def test_run_waiting_time(self):
        """Проверяет ожидание времени старта в методе run()."""
        future = datetime.now() + timedelta(minutes=10)
        future = time.fromisoformat(future.strftime("%H:%M:%S"))
        self.job.start_time = future
        self.assertFalse(self.job._Job__check_time())
        gen = self.job.run()
        next(gen)
        next(gen)
        self.assertEqual(self.job.status, JobStatus.WAIT_TIME)

    def test_run_waiting_dependencies(self):
        """Проверяет ожидание зависимостей в методе run()."""
        dependency1 = Job(normal_task)
        dependency2 = Job(normal_task)
        dependency3 = Job(normal_task)
        dependency4 = Job(normal_task)
        dependency1.status = JobStatus.PENDING
        dependency2.status = JobStatus.RUNNING
        dependency3.status = JobStatus.ERROR
        dependency4.status = JobStatus.COMPLETED
        self.job.dependencies = [dependency1, dependency2, dependency3, dependency4]
        gen = self.job.run()
        next(gen)
        next(gen)
        self.assertEqual(self.job.status, JobStatus.WAIT_DEPENDENCIES)


class SchedulerTestCase(unittest.TestCase):
    def setUp(self):
        self.scheduler = Scheduler()

    def test_add_job(self):
        """Тестирование добавления задачи в планировщик."""
        job = Job(task=normal_task)
        self.scheduler.add_job(job)
        self.assertIn(job, self.scheduler.pending_jobs)

    def test_run_single_job(self):
        """Тестирование выполнения одной задачи."""
        job = Job(task=normal_task)
        self.scheduler.add_job(job)
        gen = self.scheduler.run()
        for _ in range(100):
            try:
                next(gen)
            except StopIteration:
                break
        self.assertIn(job, self.scheduler.completed_jobs)
        self.assertEqual(job.status, JobStatus.COMPLETED)

    def test_run_multiple_jobs(self):
        """Тестирование выполнения нескольких задач."""
        jobs = [Job(task=normal_task) for _ in range(3)]
        for job in jobs:
            self.scheduler.add_job(job)

        gen = self.scheduler.run()
        for _ in range(100):
            try:
                next(gen)
            except StopIteration:
                break

        for job in jobs:
            self.assertIn(job, self.scheduler.completed_jobs)
            self.assertEqual(job.status, JobStatus.COMPLETED)

    def test_save_and_load_state(self):
        """Тестирование сохранения и загрузки состояния планировщика."""
        jobs = [Job(task=normal_task) for _ in range(3)]
        for job in jobs:
            self.scheduler.add_job(job)

        self.scheduler.save_state("state.pkl")

        self.scheduler.completed_jobs.extend(jobs)

        self.scheduler.load_state("state.pkl")
        self.assertEqual(len(self.scheduler.pending_jobs), len(jobs))
        self.assertEqual(len(self.scheduler.completed_jobs), 0)
        self.assertEqual(len(self.scheduler.failed_jobs), 0)


if __name__ == "__main__":
    unittest.main()
