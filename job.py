from datetime import datetime, time, timedelta
from enum import Enum
from typing import Generator, Optional

from log import logger


class JobStatus(Enum):
    PENDING = "Ожидание"
    RUNNING = "Выполнение"
    COMPLETED = "Завершено"
    ERROR = "Ошибка"
    WAIT_DEPENDENCIES = "Ожидание зависимостей"
    WAIT_TIME = "Ожидание времени старта"


class Job:
    def __init__(
        self,
        task: Generator,
        duration: Optional[timedelta] = None,
        start_time: Optional[time] = None,
        restarts: int = 0,
        dependencies: Optional[list["Job"]] = None,
    ):
        self.task: Generator = task
        self.duration: Optional[int] = duration
        self.start_time = start_time
        self.restarts = restarts
        self.dependencies = dependencies or []
        self.status = JobStatus.PENDING
        self.count_restarts = 0

    def run(self) -> Generator:
        while not self.__check_time():
            self.status = JobStatus.WAIT_TIME
            logger.info(
                f"Задача {self.task.__name__} может быть запущена после {self.start_time} времени"
            )
            yield

        while not self.__check_dependencies():
            self.status = JobStatus.WAIT_DEPENDENCIES
            logger.info(
                f"Для задачи {self.task.__name__} не выполненны все зависимости"
            )
            yield

        for i in range(self.restarts + 1):
            task_gen = self.task()
            self.count_restarts += 1
            self.status = JobStatus.RUNNING
            logger.debug(f"Попытка #{i} запустить задачу {self.task.__name__}")
            start_time = datetime.now()
            while True:
                if (
                    self.duration is not None
                    and (datetime.now() - start_time) <= self.duration
                ):
                    self.status = JobStatus.ERROR
                    logger.error(
                        f"Длительность выполнения превысила установленный предел: {self.duration} сек."
                    )
                    break
                try:
                    next(task_gen)
                except StopIteration:
                    self.status = JobStatus.COMPLETED
                    logger.info(f"Задача {self.task.__name__} выполнена")
                    break
                except Exception as e:
                    self.status = JobStatus.ERROR
                    logger.exception(
                        f"Произошла ошибка при выполнении задачи {self.task.__name__}"
                    )
                    logger.error(f"Ошибка: {e}")
                    break
            if self.status == JobStatus.COMPLETED:
                break
        else:
            self.status = JobStatus.ERROR
            logger.error(
                f"Превышено количество рестартов. Задача {self.task.__name__} не выполнена."
            )

    def __check_time(self) -> bool:
        now = time.fromisoformat(datetime.now().strftime("%H:%M:%S"))
        return self.start_time is None or self.start_time <= now

    def __check_dependencies(self) -> bool:
        return all(
            dependency.status == JobStatus.COMPLETED for dependency in self.dependencies
        )

    def __repr__(self) -> str:
        return f"Job(task={self.task.__name__} status={self.status})"
