import re
from pathlib import Path


SYSTEM = Path(__file__).resolve().parents[3]
COMPOSE = SYSTEM / "infrastructure" / "docker-compose.yml"
SUBMIT = SYSTEM / "scripts" / "submit_v05_flink_job.ps1"
ENV_EXAMPLE = SYSTEM / ".env.example"


def service_block(text: str, name: str) -> str:
    return re.split(r"\n(?=  [a-z][\w-]*:)", text.split(f"  {name}:\n", 1)[1], maxsplit=1)[0]


def test_compose_declares_flink_session_cluster():
    text = COMPOSE.read_text(encoding="utf-8")
    jobmanager = service_block(text, "flink-jobmanager")
    taskmanager = service_block(text, "flink-taskmanager")
    for service in (jobmanager, taskmanager):
        assert "image: flink:2.2.0-scala_2.12-java21" in service
        assert "flink_checkpoints:/opt/flink/checkpoints" in service
        assert "../java-services/flink-realtime-job/target:/opt/flink/usrlib:ro" in service
        assert "taskmanager.numberOfTaskSlots: 3" in service
        assert "state.checkpoints.dir: file:///opt/flink/checkpoints" in service
    assert "container_name: stock-flink-jobmanager" in jobmanager
    assert '"8082:8081"' in jobmanager
    assert "http://localhost:8081/overview" in jobmanager
    assert "container_name: stock-flink-taskmanager" in taskmanager
    assert "flink_checkpoints:" in text.split("\nvolumes:\n", 1)[1]


def test_flink_checkpoint_volume_is_initialized_before_cluster_start():
    text = COMPOSE.read_text(encoding="utf-8")
    assert "  flink-checkpoints-init:\n" in text
    init = service_block(text, "flink-checkpoints-init")
    assert "image: flink:2.2.0-scala_2.12-java21" in init
    assert 'user: "0:0"' in init
    assert "flink_checkpoints:/opt/flink/checkpoints" in init
    assert "mkdir -p /opt/flink/checkpoints" in init
    assert "chown 9999:9999 /opt/flink/checkpoints" in init
    assert "rm " not in init
    for name in ("flink-jobmanager", "flink-taskmanager"):
        service = service_block(text, name)
        assert "flink-checkpoints-init:" in service
        assert "condition: service_completed_successfully" in service


def test_compose_initializes_v05_topics_without_losing_existing_topics():
    text = COMPOSE.read_text(encoding="utf-8")
    init = service_block(text, "kafka-init")
    for topic, partitions in (
        ("stock.ods.daily.v1", 3),
        ("stock.ods.basic.v1", 1),
        ("stock.dead-letter.v1", 1),
        ("stock.dws.daily-indicator.v1", 3),
        ("stock.late.daily.v1", 1),
        ("stock.flink.dead-letter.v1", 1),
    ):
        command = f"--create --if-not-exists --topic {topic} --partitions {partitions} --replication-factor 1"
        assert command in init
    assert "KAFKA_TRANSACTION_MAX_TIMEOUT_MS: 900000" in service_block(text, "kafka")


def test_submit_script_builds_waits_and_passes_job_configuration():
    text = SUBMIT.read_text(encoding="utf-8")
    for value in (
        "FlinkJobConfig",
        "mvn",
        "package",
        "flink-realtime-job-0.3.0-SNAPSHOT-all.jar",
        "http://localhost:8082/overview",
        "http://localhost:8082/taskmanagers",
        "stock-daily-indicator-v1",
        "flink list -r",
        "flink run -d",
        "--bootstrap-servers",
        "--input-topic",
        "--output-topic",
        "--late-topic",
        "--dead-letter-topic",
        "--checkpoint-uri",
        "--deployment-namespace",
        "FLINK_DEPLOYMENT_NAMESPACE",
    ):
        assert value in text

    assert "FLINK_DEPLOYMENT_NAMESPACE=stock-flink" in ENV_EXAMPLE.read_text(encoding="utf-8")


def test_submit_script_captures_java_stderr_without_power_shell_51_termination():
    text = SUBMIT.read_text(encoding="utf-8")
    assert "$previousErrorActionPreference = $ErrorActionPreference" in text
    assert "$ErrorActionPreference = 'Continue'" in text
    assert "$javaVersion = (& $javaExe -version 2>&1 | Out-String)" in text
    assert "$versionExitCode = $LASTEXITCODE" in text
    assert "$ErrorActionPreference = $previousErrorActionPreference" in text
    assert "$versionExitCode -ne 0" in text
