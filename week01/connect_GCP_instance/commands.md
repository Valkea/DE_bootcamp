# Create a ss key

ssh-keygen -t rsa -f gcp -C valkea -b 2048

--> we can set a password but in this case we will have to write it each time we want to connect the remote instance.



# Put this public key to the Google Cloud Project

## GCP-UI# Go to "Compute Engine" / "Settings" / "Metadata" / "SSH-Keys"

## Local# cat gcp.pub
--> Copy the key

## GCP-UI# Paste the key
Click "Save"

## Local# Install the private key on the current computer
cp gcp ~/.ssh/gcp



# Create an instance

## GCP-UI# Go to "Compute Engine" / "Virtual Machines" / "VM Instances" / "Create Instance"

--> name: de-zoomcamp
--> select region closest to you: europe-west1 (Belgium) # cheaper than Paris
--> Zone: europe-west1-b

--> Machine series: E2
--> Machine type: e2-standard-4 # recommanded but not required

--> Boot disk: Operating system: Ubuntu
--> Boot disk: Version: Ubuntu 22.04 LTS
--> Boot disk: Size: (20 to) 30 GB # the size will impact the price charged when the instanced is stopped but not deleted

Click"Create"

--> Copy the "External-IP" : 34.76.208.239


# Let's login to the GCP remote virtual machine using SSH

ssh -i ~/.ssh/gcp valkea@34.76.208.239 # ssh -i ~/.ssh/gcp SSH-NAME@EXTERNAL-IP

# Let's install the requiered libs

## gcloud should already be install
gcloud --version

## Install Anaconda (with python, pandas etc.) --> https://www.anaconda.com/products/distribution
wget https://repo.anaconda.com/archive/Anaconda3-2022.10-Linux-x86_64.sh
bash Anaconda3-2022.10-Linux-x86_64.sh
--> yes
--> ENTER
--> yes (initialize)

### Reload the bash with anaconda
source ~/.bashrc # or simply logout / login

### Check
which python

python
--> import pandas as pd
--> pd.__version__

## Install Docker

sudo apt-get update # fetch the lists of packages
sudo apt-get install docker.io

logout/login

## if needed
--> sudo groupadd docker
--> sudo gpasswd -a $USER docker
--> sudo service docker restart
--> logout/login

docker run hello-world


## Install Docker-compose --> https://github.com/docker/compose/releases
--> select the docker-compose-linux-x86_64

mkdir bin
cd bin

wget https://github.com/docker/compose/releases/download/v2.15.1/docker-compose-linux-x86_64 -O docker-compose
chmod +x docker-compose

./docker-compose
cd ..

### Add bin folder to the PATH environment variable so we can call the bin from everywhere
vim ~./bashrc
--> add at the bottom: export PATH="${HOME}/bin:${PATH}"
source ~/.bashrc

### Check
which docker-compose
docker-compose


## Install Terraform --> https://developer.hashicorp.com/terraform/downloads?product_intent=terraform
--> select Linux / AMD64

cd ~/bin
wget https://releases.hashicorp.com/terraform/1.3.7/terraform_1.3.7_linux_amd64.zip

sudo apt-get install unzip
unzip terraform_1.3.7_linux_amd64.zip

rm terraform_1.3.7_linux_amd64.zip

### Add bin folder to the PATH environment variable so we can call the bin from everywhere
vim ~./bashrc
--> add at the bottom: export PATH="${HOME}/bin:${PATH}"
source ~/.bashrc


## Install pgcli

pip install pgcli


## DataTalksClub DE repo: https://github.com/DataTalksClub/data-engineering-zoomcamp.git

git clone https://github.com/DataTalksClub/data-engineering-zoomcamp.git

cd data-engineering-zoomcamp/week_1_basics_n_setup/2_docker_sql

docker-compose up -d

pgcli -h localhost -u root -d ny_taxi

docker-compose ps

docker-compose down


## Copy the GCP passkey for Terraform // How to upload files using sftp command

sftp de-zoomcamp
ls
mkdir .gc (or any other folder to store the passekey)
cd .gc
put gcp-passkey.json (the name of the local file to upload)
ls (the file be on the server)


# Set environment variable for CGP authkey (the one we just created and exported as a JSON file)
# We can't use auth as we used on local terraform config because we can't open the pop-up required to validate the identity... so we need to slighly change the approach 

export GOOGLE_APPLICATION_CREDENTIALS=~/.gc/gcp-passkey.json

[NON en remote ]
gcloud auth application-default login
--> Authorize with your google account
[/NON]

[OUI en remote]
gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS
[/OUI]

Expected answer --> Activated service account credentials for: [dt-de-w1-user@lexical-passkey-375922.iam.gserviceaccount.com]


## Stop instance

sudo shutdown now (immediate)
sudo shutdown (in 1 min and shutdown -c to cancel)

But shutting down the instance only stop the instance.
THE COST OF THE INSTANCE ARE FREEZED, BUT NOT THE COST OF THE REQUIRED SPACE TO SAVE THE STATE.






## Set a config file for SSH to easily access the remote instance

cd ~/.ssh
vim config # create a config file

***************
Host de-zoomcamp
	HostName 34.76.208.239 # GCP-EXTERNAL-IP
	User valkea # SSH-USER
	IdentityFile ~/.ssh/gcp # Path to private key
***************

Now we can open the ssh remote instance with the following command (But the IP needs to be updated each time the remote instance is restarted and change it external-ip)

ssh de-zoomcamp # instead of ssh -i ~/.ssh/gcp valkea@34.76.208.239




## Configure Visual Studio Code to work directly on remote server via SSH

# Go go "Extensions" and install "Remote - SSH"
# Click the green bottom "Open a remote window" at the bottom left
# Select "Connect to host ... Remote SSH"
# Select existing ssh (depends on the config files in ~/.ssh folder) or create a new one

Now we can interact with the remote server using the Terminal (View / Terminal)

## Configure port forwarding using Visual Studio Code

# Connect a remote SSH server as explained above
# Go to the Terminal and select tab "PORTS"
# Add 5432 for the pg-database access (so we can use pgcli or any script that insert/read data from the DB)
# Add 8080 for the pg-admin website
# Add 8888 for jupyter notebooks (we can execute the notebook on the GCP server and access it locally !)


