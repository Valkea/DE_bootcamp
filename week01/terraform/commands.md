# Install Terraform

wget -O- https://apt.releases.hashicorp.com/gpg | gpg --dearmor | sudo tee /usr/share/keyrings/hashicorp-archive-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] https://apt.releases.hashicorp.com $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/hashicorp.list
sudo apt update && sudo apt install terraform

terraform --version

# Google cloud

#GCD-UI# Create a new project
project name -> PROJECT-NAME
project id -> PROJECT-ID

#GCD-UI# Switch to the project "PROJECT-NAME"
#GCD-UI# Go to "IAM & Admin" / "Service Account"
#GCD-UI# Go to "Create service account"

-->1/ service account-name: PROJECT-NAME-user
-->2/ role: basic / viewer
-->3/ .
--> Done

#GCD-UI# Once the service account is created we can click '...' and "Manage keys"
#GCD-UI# Go to "Add Key" / "Create new key" / "JSON" --> A json file is saved to the computer

# Install Google Cloud CLI
# https://cloud.google.com/sdk/docs/install-sdk#deb

sudo apt-get install apt-transport-https ca-certificates gnupg

echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list

curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -

sudo apt-get update && sudo apt-get install google-cloud-cli

gcloud -v 

# Set environment variable for CGP authkey (the one we just created and exported as a JSON file)

export GOOGLE_APPLICATION_CREDENTIALS=My_Exported_File.json

gcloud auth application-default login

--> Authorize with your google account

#GCD-UI# Go to "IAM & Admin" / "IAM"
#GCD-UI# Click the email account we just granted access to, then click "edit"
--> Add "Cloud Storange" / "Storage Admin"
--> Add "Cloud Storange" / "Storage Object Admin"
--> Add "BigQuery Admin"
--> Save

# Go to this page https://console.cloud.google.com/apis/library/iam.googleapis.com
--> Select the project (if needed)
--> Enable the API

# Go to this page https://console.cloud.google.com/apis/library/iamcredentials.googleapis.com
--> Select the project (if needed)
--> Enable the API

## Copy the file from the github : https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_1_basics_n_setup/1_terraform_gcp/terraform

--> Configure variables.tf

## Use Terraform

terraform init

terraform apply

terraform destroy
