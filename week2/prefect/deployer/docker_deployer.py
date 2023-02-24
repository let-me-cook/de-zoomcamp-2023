from prefect.deployments import Deployment
from prefect.infrastructure.docker import DockerContainer
from flows.docker_web2local2gcs import multi_etl_web_to_gcs

docker_block = DockerContainer.load("zoom")

docker_dep = Deployment.build_from_flow(
    flow=multi_etl_web_to_gcs,
    name="docker-flow",
    infrastructure=docker_block
)

if __name__ == "__main__":
    assert isinstance(docker_dep, Deployment)
    docker_dep.apply() # type: ignore
