import os
from typing import Generator

import requests

from job import Job
from scheduler import Scheduler


def create_directory(directory_name="test_directory") -> Generator:
    try:
        os.mkdir(directory_name)
        yield f"Директория {directory_name} успешно создана"
    except FileExistsError:
        yield f"Директория {directory_name} уже существует"
    except Exception as e:
        yield f"Ошибка при создании директории {directory_name}: {e}"


def read_file(file_path="test.txt") -> Generator:
    try:
        with open(file_path, "r") as file:
            yield file.read()
    except FileNotFoundError:
        yield f"Файл {file_path} не найден"
    except Exception as e:
        yield f"Ошибка при чтении файла {file_path}: {e}"


def make_get_request(url="https://www.example.com") -> Generator:
    try:
        response = requests.get(url)
        yield f"Статус код: {response.status_code}"
        # yield f"Результат: {response.text}"
    except requests.exceptions.RequestException as e:
        yield f"Ошибка при выполнении GET-запроса: {e}"


if __name__ == "__main__":
    job1 = Job(create_directory, restarts=3)
    job2 = Job(read_file, dependencies=[job1])
    job3 = Job(make_get_request, dependencies=[job1, job2])

    sc = Scheduler()
    sc.add_job(job1)
    sc.add_job(job2)
    sc.add_job(job3)

    gen = sc.run()
