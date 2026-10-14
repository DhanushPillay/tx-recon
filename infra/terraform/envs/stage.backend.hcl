# Partial backend config for stage. Pass explicitly:
#   terraform init -backend-config=envs/stage.backend.hcl
#   terraform plan  -var-file=envs/stage.tfvars
#
# Bucket and lock table come from the bootstrap commands in backend.tf. The key
# is what keeps envs apart, so do not copy these files without changing it.
bucket         = "tx-recon-tfstate"
region         = "ap-south-1"
key            = "stage/terraform.tfstate"
dynamodb_table = "tx-recon-tfstate-lock"
encrypt        = true
