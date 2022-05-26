import sys
import os
from threading import Thread
from typing import List, Union

import wget
from loguru import logger
import docker
from docker.types import Mount
from docker.models.containers import Container


class AsyncContainerLogger:
    """ Asynchronous thread for logging container logs """
    def __init__(self, container: Container):
        self.container = container
        self.thread = self.attach(self.container)

    @staticmethod
    def _run(container: Container):
        for line in container.logs(stream=True):
            logger.info(line.strip())

    @staticmethod
    def attach(container: Container):
        t = Thread(target=AsyncContainerLogger._run, args=(container,))
        t.start()
        return t


class DockerRunner:
    """ Helper class for running docker containers and streaming logs """
    def __init__(
            self,
            image: str = 'alpine',
            volume_hostpath: str = '/tmp',
            volume_mountpath: str = '/tmp'
    ):
        self._client = docker.from_env()
        self._image = image
        self._volume_hostpath = volume_hostpath
        self._volume_mountpath = volume_mountpath
        self._containers = []

    def _add_mounts(self) -> List[Mount]:
        mounts = [Mount(self._volume_mountpath, self._volume_hostpath, type='bind')]
        return mounts

    def run_in_background(self, command: Union[str, List[str]]) -> Container:
        """ Run container in the background """
        container = self._client.containers.run(
            self._image,
            command=command,
            detach=True,
            mounts=self._add_mounts()
        )
        return container

    def run(self, command: Union[str, List[str]], stream_logs: bool = True, with_bash: bool = True) -> Container:
        """ Run container in the foreground """

        if with_bash:
            command = ' '.join(command) if isinstance(command, list) else command
            command = f'/bin/bash -c "{command}"'

        container = self.run_in_background(command)

        if stream_logs:
            AsyncContainerLogger(container)

        resp_dict = container.wait()
        status_code = resp_dict['StatusCode']

        if int(status_code) != 0:
            raise RuntimeError(f"Docker container exited with code {status_code} and error: {resp_dict['Error']}")
        return container

    @staticmethod
    def _container_or_id(container_id: Union[str, Container]) -> str:
        if isinstance(container_id, Container):
            container_id = container_id.id
        return container_id

    def get_status(self, container_id: Union[str, Container]) -> str:
        return self._client.containers.get(self._container_or_id(container_id)).status

    def is_running(self, container_id: Union[str, Container]) -> bool:
        status = self.get_status(container_id)
        return status == 'running'

    def is_exited(self, container_id: Union[str, Container]) -> bool:
        status = self.get_status(container_id)
        return status == 'exited'


def bar_progress(current: float, total: float, width: int) -> None:
    """ Progress bar for wget """
    progress_message = "Downloading: %d%% [%d / %d] bytes" % (current / total * 100, current, total)
    sys.stdout.write("\r" + progress_message)
    sys.stdout.flush()


def download_to_path(url: str, path: str) -> str:
    """ Download URI to path with wget """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    wget.download(url, out=path, bar=bar_progress)
    return path
