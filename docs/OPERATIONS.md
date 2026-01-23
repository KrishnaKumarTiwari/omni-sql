# OmniSQL: Operations & Deployment Guide

This document outlines the infrastructure, deployment pipelines, and incident response procedures for OmniSQL.

## 1. Infrastructure as Code (IaC)
OmniSQL infrastructure is managed via **Terraform** to ensure consistency across Multi-tenant and BYOC environments.

### Core Modules
- **`compute`**: EKS cluster definitions with Managed Node Groups and Fargate profiles (for stateless connector workers).
- **`networking`**: VPC, Subnets, Security Groups, and Transit Gateway for high-speed SaaS connectivity.
- **`secrets`**: HashiCorp Vault configuration for managing tenant-scoped OAuth tokens and encryption keys.
- **`databases`**: Multi-AZ RDS Postgres for metadata/schema storage and ElastiCache Redis for result caching.

## 2. Continuous Delivery (CD)
We utilize **Helm** and **ArgoCD** for automated, safe deployments.

### Deployment Strategy
- **Canary Releases**: New versions are deployed to 5% of traffic. Automated SLO analysis monitors P99 latency and error rates.
- **Blue-Green**: Used for major Control Plane version upgrades to ensure zero-downtime cutover.
- **Automatic Rollback**: Triggered if error rates increase by >0.5% or P95 latency exceeds 1.5s during canary analysis.

## 3. Disaster Recovery (DR) & Business Continuity
OmniSQL is designed for high availability and regional resilience.

### Architecture
- **Multi-AZ**: Every component (Gateway, Engine, RDS) is spread across 3 Availability Zones.
- **Regional Failover**: Metadata and KMS keys are replicated globally. The Data Plane can be spun up in a secondary region within minutes using IaC.

### Performance Targets
- **RPO (Recovery Point Objective)**: < 5 minutes (Maximum acceptable data loss for metadata/logs).
- **RTO (Recovery Time Objective)**: < 15 minutes (Maximum time to restore service in a failover scenario).

## 4. Operational Runbooks
Standard Operating Procedures (SOPs) for common incident scenarios.

### 🆘 SOP-101: Connector Rate-Limit Floods
- **Symptoms**: Increase in `429 RATE_LIMIT_EXHAUSTED` errors for a specific SaaS.
- **Action**:
    1. Identify the "Noisy Tenant" via Grafana dashboards.
    2. Apply a temporary user-level concurrency cap in the OPA policy store.
    3. Notify the customer's technical contact.

### 🆘 SOP-102: Cache Stampede
- **Symptoms**: Sudden spike in SaaS API latency and control plane CPU usage after a cache invalidation event.
- **Action**:
    1. Enable "Soft Caching" (serve stale data while live fetch is in progress).
    2. Temporarily increase the ElastiCache cluster size via Terraform.

### 🆘 SOP-103: Connector Auth Failures
- **Symptoms**: Bulk `401 Unauthorized` errors from a specific SaaS provider.
- **Action**:
    1. Check for global SaaS outages via status pages.
    2. Initiate a bulk token refresh job via the Control Plane CLI.
    3. Rotation of the provider-level client secret if a breach is suspected.
