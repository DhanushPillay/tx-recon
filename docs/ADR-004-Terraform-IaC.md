# ADR 004: Infrastructure - Terraform for AWS Deployment

**Status:** Accepted

## Context
While the primary development and testing environment runs entirely locally via Docker Compose (to eliminate cloud costs), enterprise data platforms run on managed cloud services. We needed a way to prove that the architecture translates to a real-world cloud environment without actually incurring AWS billing.

## Alternatives Considered
1. **Manual AWS Console Setup:**
    * *Pros:* Easy for beginners, visual.
    * *Cons:* Not reproducible, error-prone, against DevOps best practices.
2. **AWS CloudFormation / CDK:**
    * *Pros:* AWS-native, deep integration.
    * *Cons:* Vendor lock-in, steeper learning curve for multi-cloud engineers.
3. **Terraform (HCL):**
    * *Pros:* Industry-standard Infrastructure as Code (IaC). Declarative, reproducible, and cloud-agnostic. 
    * *Cons:* Requires managing state files if deployed.

## Decision
We chose **Terraform** to define the equivalent AWS infrastructure (Amazon MSK, Amazon S3, AWS Glue, Amazon MWAA). The code is kept in the `infra/` directory but is not applied by default.

## Rationale
By writing Terraform configurations, we map our local Docker containers to their enterprise AWS equivalents:
* Local Redpanda -> Amazon MSK
* Local MinIO -> Amazon S3
* Local Project Nessie -> AWS Glue Data Catalog
* Local Airflow -> Amazon MWAA

This shows the architecture maps directly to AWS managed services.
