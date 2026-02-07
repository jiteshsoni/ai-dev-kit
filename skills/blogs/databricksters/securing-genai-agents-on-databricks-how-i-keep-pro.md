---
name: securing-genai-agents-on-databricks-how-i-keep-pro
description: 
---

# Securing Gen‑AI Agents on Databricks: How I Keep Prompt‑Injection and Data‑Leak Nightmares at Bay

## Overview


## Source
- **Author:** Databricks
- **URL:** https://www.databricksters.com/p/securing-genai-agents-on-databricks

## Tags
mlops, data-engineering, ai, databricks

## Full Content

# Securing Gen‑AI Agents on Databricks: How I Keep Prompt‑Injection and Data‑Leak Nightmares at Bay

**Source:** https://www.databricksters.com/p/securing-genai-agents-on-databricks

---



Securing Gen‑AI Agents on Databricks: How I Keep Prompt‑Injection and Data‑Leak Nightmares at Bay

Databricksters

Subscribe

Sign in

Securing Gen‑AI Agents on Databricks: How I Keep Prompt‑Injection and Data‑Leak Nightmares at Bay

Practical, field‑tested tactics for neutralizing prompt‑injection, blocking data leaks, and shipping secure Gen‑AI workloads on the Databricks Lakehouse.

Debu Sinha

Jul 29, 2025

3

1

Share

I spend most of my day helping customers push LLMs into production on the Databricks Lakehouse. The upside is huge—automated copilots, retrieval‑augmented search, and full‑on agent workflows. The downside? A shiny new attack surface that traditional AppSec tooling simply doesn't see. Below is the playbook I follow (and advise my clients to adopt) to harden Gen‑AI systems on Databricks—rooted in features the platform already ships and backed by hard data from Databricks' own security research.

1. Stop Prompt‑Injection Where It Starts—at the 

Mosaic AI Gateway

Databricks' Mosaic AI Gateway sits in front of any model endpoint—first‑party or external—and processes prompts through AI guardrails and safety filters¹:

AI Guardrails

 use safety models like Meta Llama Guard 2-8b to detect violence, hate speech, and PII before prompts reach your business logic.

Rate limiting

 operates at the user, service principal, and endpoint level to prevent abuse and manage costs.

Advanced custom filtering 

requires the Custom Guardrails Private Preview, as legacy keyword and topic moderation features were deprecated in May 2025.

Comprehensive logging

 captures all traffic to Unity Catalog, providing detailed forensic trails for incident response and compliance.

Important

: While basic guardrails are production-ready, advanced custom filtering capabilities are evolving rapidly. Consult current Databricks documentation for the latest feature availability.

Why it matters:

 OWASP now lists prompt‑injection as LLM01:2025—the #1 risk in its 

Top 10 for Large‑Language‑Model Applications

². Cutting the threat off at the source is cheaper than chasing leaked data after the fact.

2. Red‑Team Your Endpoints with 

NVIDIA Garak

Security theater ≠ security. A Databricks (2 July 2025) post walks through wiring 

Garak

, NVIDIA's open‑source LLM vulnerability scanner, to any Databricks Model‑Serving endpoint³:

Garak blasts the model with >150 attack templates (jailbreaks, toxicity, prompt‑injection, etc.) and scores which ones land.

Because it uses the same REST API your production apps call, setup is a JSON config plus a PAT token.

Typically, teams schedule nightly Garak runs in a Jobs pipeline, failures hit Slack, and they fix guardrails before users ever notice.

3. Follow the 

Databricks AI Security Framework (DASF)

Databricks published DASF in 2024; 

version 2.0

 now maps 

62 distinct technical security risks

 across 12 AI system components—from raw data to inference responses⁴. Prompt‑injection and jailbreaks headline the &quot;Model Serving&quot; stage, right alongside accidental data exposure.

Take‑away:

 Treat Gen‑AI like any other critical workload—threat‑model it end‑to‑end, not just at the model boundary.

4. Control Model Serving Egress with Serverless Security

The real threat isn't compromised clusters—it's unauthorized outbound calls from your AI endpoints. Unlike traditional workloads, Databricks Model Serving runs in a serverless compute plane that requires purpose-built security controls⁵: 

Serverless Egress Control

 (now GA on AWS and Azure) flips the security model to default-deny: 

Every outbound connection is blocked unless explicitly approved through Network Policies.

 FQDN allowlisting ensures models can only reach approved destinations (your data lakes, approved APIs, etc.).

Dry-run mode lets you test policies safely before enforcement.

Comprehensive audit logging captures every denied connection attempt.

Network Connectivity Configurations (NCCs) 

enable private connectivity without VPC complexity

: 

Connect model serving endpoints to internal resources via private endpoints.

Azure: 

Generally Available with up to 100 private endpoints per region.

AWS: 

Public Preview with up to 30 private endpoints per region.

GCP: 

Limited availability, check current region support.

Each NCC supports up to 50 workspaces across all cloud providers.

Why this matters: 

Model serving endpoints can scale to 25K+ queries per second. A single misconfigured endpoint could exfiltrate terabytes before traditional monitoring catches it. Serverless egress control stops data theft at the network layer—before it leaves your cloud account. 

Pro tip:

 Start with a restrictive policy in dry-run mode, analyze the audit logs for a week, then gradually allow legitimate destinations. This approach catches both intentional exfiltration attempts and accidental misconfigurations.

5. Enforce Least‑Privilege Data Access with 

Unity Catalog

Unity Catalog delivers per‑column ACLs, row‑level filters, and time‑bound scoped credentials tied to your IdP groups. Pair that with audit logs and PAT scopes so agent code can read only the tables it truly needs—nothing more.

6. Runtime Protection: 

Noma Security

 Integration

On 5 June 2025, Databricks Ventures announced its investment in Noma Security—bringing comprehensive AI security governance and proactive risk management directly into the Databricks Data Intelligence Platform. Noma provides detailed visibility into all AI assets, maintains comprehensive AI model inventories with AI Bill of Materials (AIBOM), and enables early detection of AI risks before runtime. The platform continuously monitors AI models for suspicious behavior and helps ensure compliance with emerging AI regulations like ISO 42001.

This integration streamlines AISecOps workflows by automating security checks and enforcing policies throughout the AI development pipeline, helping enterprises accelerate secure AI adoption while maintaining regulatory compliance.

My Checklist Before Any Gen‑AI Launch

Bottom Line

Prompt‑injection and data exfiltration aren't hypothetical—they're happening in the wild right now.

Databricks ships the primitives to defend against both, but you have to wire them together:

Block bad prompts

 (Gateway).

Monitor comprehensively

 (Garak testing, Noma governance)

.

Govern data &amp; egress (

Unity Catalog, Serverless Egress Control, NCCs

)

.

Do that, and you can unleash agents that innovate—without waking up to a breach headline.

 Sources

Securing the Future: How AI Gateways Protect AI‑Agent Systems in the Era of Generative AI

 – Databricks Blog, November 13, 2024

OWASP Top 10 for LLM Applications 2025

 – OWASP Foundation

AI Security in Action: Applying NVIDIA's Garak to LLMs on Databricks

 – Databricks Blog, July 2, 2025

Announcing the Databricks AI Security Framework 2.0

 – Databricks Blog, February 12, 2025

Announcing egress control for serverless and model serving workloads – Databricks Blog, Jan 7, 202

5

Securing the AI Lifecycle: Databricks Ventures Invests in Noma Security

 – Databricks Blog, June 5, 2025

3

1

Share

Previous

Next

Discussion about this post

Comments

Restacks

Top

Latest

Discussions

No posts

Ready for more?

Subscribe

© 2026 Soni

 · 

Privacy

 ∙ 

Terms

 ∙ 

Collection notice

 Start your Substack

Get the app

Substack
 is the home for great culture

 This site requires JavaScript to run correctly. Please 
turn on JavaScript
 or unblock scripts





---
*Skill auto-generated from blog content*
*Created: 2026-02-07*
