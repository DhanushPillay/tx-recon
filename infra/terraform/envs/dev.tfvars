# Usage: terraform init -backend-config="key=tx-recon/dev/terraform.tfstate"
#        terraform apply -var-file=envs/dev.tfvars
project    = "tx-recon"
env        = "dev"
aws_region = "ap-south-1"
