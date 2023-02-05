import os
import pathlib
from prefect.filesystems import GitHub

block = GitHub(
    repository="https://github.com/Valkea/DE_bootcamp.git",
    # access_token=<my_access_token> # only required for private repos
)

# block.get_directory("/week02/flows/homework") # specify a subfolder of repo
block.save("ny-taxi-github")