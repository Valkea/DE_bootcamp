from prefect.deployments import Deployment
from prefect.filesystems import GitHub
from etl_web_to_gcs import etl_parent_flow

github_block = GitHub.load("ny-taxi-github")

github_dep = Deployment.build_from_flow(
     flow=etl_parent_flow,
     name="github-flow",
     storage=github_block,
     entrypoint="week02/flows/homework/etl_web_to_gcs.py:etl_parent_flow")

if __name__ == "__main__":
    github_dep.apply()


# This code is the very same as the command line below!
# prefect deployment build path/to/flow.py:flow_name --name deployment_name --tag dev -sb github/dev -a
# >>> prefect deployment build week02/flows/homework/etl_web_to_gcs.py:etl_parent_flow --name github-flow --tag dev -sb github/ny-taxi-github -a