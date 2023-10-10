import argparse
import docker
import time
import boto3
from botocore.exceptions import NoCredentialsError


class CloudWatchLogger:
    def __init__(
        self, aws_access_key_id: str, aws_secret_access_key: str, aws_region: str
    ) -> None:
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_region = aws_region
        self.cloudwatch_client = self.create_cloudwatch_client()

    def create_cloudwatch_client(self) -> boto3.client:
        client = boto3.client(
            "logs",
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.aws_region,
        )
        return client

    def create_or_get_log_group(self, group_name: str) -> None:
        try:
            self.cloudwatch_client.create_log_group(logGroupName=group_name)
        except self.cloudwatch_client.exceptions.ResourceAlreadyExistsException:
            pass

    def create_or_get_log_stream(self, group_name: str, stream_name: str) -> None:
        try:
            self.cloudwatch_client.create_log_stream(
                logGroupName=group_name, logStreamName=stream_name
            )
        except self.cloudwatch_client.exceptions.ResourceAlreadyExistsException:
            pass

    def send_logs_to_cloudwatch(
        self, group_name: str, stream_name: str, log_lines: str
    ) -> None:
        try:
            self.cloudwatch_client.put_log_events(
                logGroupName=group_name,
                logStreamName=stream_name,
                logEvents=[
                    {"timestamp": int(time.time() * 1000), "message": line}
                    for line in log_lines
                ],
            )
        except NoCredentialsError:
            print("AWS credentials are invalid.")
            exit(1)


class DockerRunner:
    def __init__(self, docker_image: str, bash_command: str) -> None:
        self.docker_image = docker_image
        self.bash_command = bash_command
        self.docker_client = docker.from_env()

    def run_container(self):
        container = self.docker_client.containers.run(
            self.docker_image,
            command=["bash", "-c", f"python -u -c '{self.bash_command}'"],
            detach=True,
            stdout=True,
            stderr=True,
        )
        return container


class DockerCloudWatchLogger:
    def __init__(
        self,
        docker_runner: DockerRunner,
        cloudwatch_logger: CloudWatchLogger,
        aws_cloudwatch_group: str,
        aws_cloudwatch_stream: str,
    ) -> None:
        self.docker_runner = docker_runner
        self.cloudwatch_logger = cloudwatch_logger
        self.aws_cloudwatch_group = aws_cloudwatch_group
        self.aws_cloudwatch_stream = aws_cloudwatch_stream

    def run_container_and_log(self) -> None:
        self.cloudwatch_logger.create_or_get_log_group(self.aws_cloudwatch_group)
        self.cloudwatch_logger.create_or_get_log_stream(
            self.aws_cloudwatch_group, self.aws_cloudwatch_stream
        )

        container = self.docker_runner.run_container()
        try:
            for log_line in container.logs(stream=True, follow=True):
                log_lines = log_line.decode("utf-8").splitlines()
                self.cloudwatch_logger.send_logs_to_cloudwatch(
                    self.aws_cloudwatch_group, self.aws_cloudwatch_stream, log_lines
                )
        except KeyboardInterrupt:
            print("Interrupted. Stopping the container...")
            container.stop()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run a Docker container and send its logs to AWS CloudWatch."
    )
    parser.add_argument(
        "--docker-image", required=True, help="Name of the Docker image"
    )
    parser.add_argument(
        "--bash-command",
        required=True,
        help="Bash command to run inside the Docker image",
    )
    parser.add_argument(
        "--aws-cloudwatch-group", required=True, help="Name of AWS CloudWatch group"
    )
    parser.add_argument(
        "--aws-cloudwatch-stream", required=True, help="Name of AWS CloudWatch stream"
    )
    parser.add_argument("--aws-access-key-id", required=True, help="AWS Access Key ID")
    parser.add_argument(
        "--aws-secret-access-key", required=True, help="AWS Secret Access Key"
    )
    parser.add_argument("--aws-region", required=True, help="AWS region")

    args = parser.parse_args()

    cloudwatch_logger = CloudWatchLogger(
        args.aws_access_key_id, args.aws_secret_access_key, args.aws_region
    )
    docker_runner = DockerRunner(args.docker_image, args.bash_command)

    logger = DockerCloudWatchLogger(
        docker_runner,
        cloudwatch_logger,
        args.aws_cloudwatch_group,
        args.aws_cloudwatch_stream,
    )
    logger.run_container_and_log()


if __name__ == "__main__":
    main()
