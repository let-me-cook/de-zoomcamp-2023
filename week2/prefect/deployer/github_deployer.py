from prefect.deployments import Deployment
from prefect.filesystems import GitHub

github_block = GitHub.load("de-zoom")
github_dep = Deployment(
    name="test_name",
    flow_name="test_flow",
    storage=github_block,
    path=(
        "week2/prefect/flows/docker_web2local2gcs/"
        "parameterized_ingest.py:multi_etl_web_to_gcs"
    ),
)

github_dep.apply()  # type: ignore
