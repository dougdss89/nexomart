"""Run spark-submit (client mode) inside the dedicated `spark-driver` container.

The driver used to run inside airflow_worker, sharing its memory limit with ingestion and
compaction tasks. This operator keeps Airflow as the scheduler only: it asks the Docker daemon to
`exec` spark-submit in the always-on spark-driver container (spark/docker-compose.yml) and streams
the output into the task log. Needs /var/run/docker.sock mounted in the Airflow worker.
"""

import re
import uuid

import docker
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

SPARK_USER = "spark"


class SparkDriverSubmitOperator(BaseOperator):
    template_fields = ("application_args", "name")

    def __init__(
        self,
        *,
        application: str,
        application_args: list[str] | None = None,
        name: str | None = None,
        conf: dict[str, str] | None = None,
        env_vars: dict[str, str] | None = None,
        container: str = "spark-driver",
        master: str = "spark://spark-master:7077",
        verbose: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.application = application
        self.application_args = application_args or []
        self.name = name or "airflow-{{ dag.dag_id }}-{{ task.task_id }}"
        self.conf = conf or {}
        self.env_vars = env_vars or {}
        self.container = container
        self.master = master
        self.verbose = verbose
        self._pidfile: str | None = None
        self._client: docker.DockerClient | None = None

    def _build_command(self) -> list[str]:
        spark_submit = ["/opt/spark/bin/spark-submit", "--master", self.master, "--deploy-mode", "client", "--name", self.name]
        for key, value in self.conf.items():
            spark_submit += ["--conf", f"{key}={value}"]
        if self.verbose:
            spark_submit.append("--verbose")
        spark_submit += [self.application, *self.application_args]
        # The pidfile lets on_kill() signal the submit process; `exec` makes it the shell's pid.
        script = 'echo $$ > "$0"; exec "$@"'
        return ["bash", "-c", script, self._pidfile, *spark_submit]

    def execute(self, context):
        self._client = docker.from_env()
        try:
            target = self._client.containers.get(self.container)
        except docker.errors.NotFound as exc:
            raise AirflowException(f"Container {self.container!r} not found; run `make up-spark`.") from exc
        if target.status != "running":
            raise AirflowException(f"Container {self.container!r} is {target.status}, expected running.")

        self._pidfile = f"/tmp/airflow-driver-{uuid.uuid4().hex}.pid"
        cmd = self._build_command()
        self.log.info("Running in %s: %s", self.container, " ".join(cmd[4:]))

        api = self._client.api
        exec_id = api.exec_create(target.id, cmd, user=SPARK_USER, environment=self.env_vars)["Id"]
        pending = ""
        try:
            for chunk in api.exec_start(exec_id, stream=True):
                pending += chunk.decode("utf-8", errors="replace")
                *lines, pending = re.split(r"\r?\n", pending)
                for line in lines:
                    self.log.info("%s", line)
            if pending:
                self.log.info("%s", pending)
        finally:
            self._remove_pidfile(target)

        exit_code = api.exec_inspect(exec_id)["ExitCode"]
        if exit_code != 0:
            raise AirflowException(f"spark-submit exited with code {exit_code}")

    def _exec_quiet(self, target, script: str):
        target.exec_run(["bash", "-c", script, self._pidfile], user=SPARK_USER)

    def _remove_pidfile(self, target):
        try:
            self._exec_quiet(target, 'rm -f "$0"')
        except docker.errors.DockerException:
            self.log.warning("Could not remove %s in %s", self._pidfile, self.container)

    def on_kill(self):
        if not (self._client and self._pidfile):
            return
        try:
            target = self._client.containers.get(self.container)
            self._exec_quiet(target, 'kill -TERM "$(cat "$0")"')
            self.log.info("Sent SIGTERM to the spark-submit process in %s", self.container)
        except docker.errors.DockerException:
            self.log.warning("Could not signal the driver in %s", self.container)
