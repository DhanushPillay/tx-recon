# Remote state: one key per env so dev can never clobber prod.
# Bootstrap once: aws s3api create-bucket --bucket tx-recon-tfstate --region ap-south-1
#   + a DynamoDB table tx-recon-tfstate-lock (partition key LockID, string) for locking.
# Promote rule: plan/apply dev -> stage -> prod in order, each with its own
# -var-file AND -backend-config key; never point two envs at the same key.
terraform {
  backend "s3" {
    bucket         = "tx-recon-tfstate"
    key            = "tx-recon/dev/terraform.tfstate"
    region         = "ap-south-1"
    encrypt        = true
    dynamodb_table = "tx-recon-tfstate-lock"
  }
}
