# Partial backend config for prod. Pass explicitly:
#   terraform init -backend-config=envs/prod.backend.hcl
#   terraform plan  -var-file=envs/prod.tfvars
#
# Bucket and lock table come from the bootstrap commands in backend.tf. The key
# is what keeps envs apart, so do not copy these files without changing it.
bucket         = "tx-recon-tfstate"
region         = "ap-south-1"
key            = "prod/terraform.tfstate"
dynamodb_table = "tx-recon-tfstate-lock"
encrypt        = true
