# Usage: terraform init -backend-config="key=tx-recon/stage/terraform.tfstate"
#        terraform apply -var-file=envs/stage.tfvars
# Promote only after the dev plan is clean and integration CI is green.
project    = "tx-recon"
env        = "stage"
aws_region = "ap-south-1"
