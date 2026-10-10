# Remote state: one key per env. The backend block stays EMPTY on purpose:
# init fails until -backend-config names the env key, so prod state can
# never land under the dev key by accident.
#
#   terraform init -backend-config=envs/dev.backend.hcl
#   terraform plan  -var-file=envs/dev.tfvars
#
# Bootstrap once, before the first init:
#   aws s3api create-bucket --bucket tx-recon-tfstate --region ap-south-1
#   aws dynamodb create-table --table-name tx-recon-tfstate-lock \
#     --attribute-definitions AttributeName=LockID,AttributeType=S \
#     --key-schema AttributeName=LockID,KeyType=HASH --billing-mode PAY_PER_REQUEST
#   aws dynamodb update-item --table-name tx-recon-tfstate-lock \
#     --key '{"LockID":{"S":"tx-recon-tfstate-lock"}}' \
#     --update-expression 'SET #s=:d' --expression-attribute-names '{"#s":"state"}' \
#     --expression-attribute-values '{":d":{"S":""}}'
#
# Promote dev -> stage -> prod, each with its own backend.hcl AND tfvars.
terraform {
  backend "s3" {}
}
