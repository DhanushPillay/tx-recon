# Usage: terraform init -backend-config="key=tx-recon/prod/terraform.tfstate"
#        terraform apply -var-file=envs/prod.tfvars
# Promote only after stage ran a full batch with zero drift alerts.
project    = "tx-recon"
env        = "prod"
aws_region = "ap-south-1"
