# ServiceNow + IBM/Confluent — Native Kafka Integration Opportunity

**INTERNAL — FOR REVIEW**

**To:** [GM] / [Partner Relationship Manager]
**From:** [Your Name]
**Re:** ServiceNow + IBM/Confluent — Native Kafka Integration Opportunity
**Date:** May 9, 2026

---

## Situation

IBM completed its $11B acquisition of Confluent on March 17, 2026, making Confluent a wholly-owned IBM subsidiary. Confluent is the leading enterprise data streaming platform, built on Apache Kafka, with 6,500+ enterprise customers and 40% Fortune 500 penetration. With ServiceNow's strategic partnership with IBM now in motion, we have a timely opportunity to build a deeper technical and commercial "better together" story leveraging Confluent's streaming capabilities.

## The Opportunity

ServiceNow's Stream Connect product already supports direct Kafka connections and has Confluent Schema Registry integrated — meaning the foundational technical relationship with Confluent's platform is already live. What does not yet exist is a **native, Confluent-specific integration path** that is co-built, co-marketed, and co-sold with IBM/Confluent.

The existing Confluent-maintained ServiceNow connectors (Source and Sink) use REST API polling — they are managed by Confluent, not ServiceNow, and are not positioned as a joint solution. There is no current commercial or go-to-market agreement connecting the two companies' products.

## What We're Proposing

Two parallel tracks, both requiring IBM-Confluent collaboration:

**Track 1 (Near-term) — Confluent profile in Stream Connect**
Add a named Confluent configuration profile to Stream Connect, enabling customers to connect to Confluent Cloud using the native Kafka protocol — no REST polling, no middleware. This extends existing Stream Connect architecture and Schema Registry integration. The artifact lives in ServiceNow's product and is co-marketed with IBM as a "better together" solution. Precedent is established; the work is configuration and partnership, not net-new engineering.

**Track 2 (Longer-term) — Official Confluent marketplace listing**
Publish a ServiceNow-owned, native Kafka connector to Confluent Hub under the `servicenow` namespace. This gives Confluent customers a discoverable, ServiceNow-supported connector and enables a separate distribution and commercial motion. This track requires formal Verified Integration program enrollment with IBM/Confluent.

## Why Now

- IBM owns Confluent and has a strategic partnership with ServiceNow — the internal door is open in a way it was not six months ago
- Confluent's Source V1 connector reaches end-of-life April 2027 — customers are actively evaluating their integration strategy, creating a natural entry point
- Three separate ServiceNow-Kafka connectors currently exist in the market (IBM open-source, Confluent Source, Confluent Sink) with no coordinated ownership — a joint solution creates clarity and positions ServiceNow as the definitive streaming integration partner
- Real-time data streaming is the foundational layer for enterprise AI and agentic workflows — IBM has explicitly positioned Confluent this way, and ServiceNow's AI platform strategy aligns directly

## Recommended Next Steps

1. **Confirm scope alignment** with IBM partnership team: is the immediate deliverable a Stream Connect profile, a marketplace connector, or both
2. **Establish a joint product working group** with IBM/Confluent product team managing the existing ServiceNow connectors — they own the current roadmap
3. **Formalize partner program enrollment** — route through IBM relationship team rather than self-serve portal (PPA deadline has passed)
4. **Identify GTM motion**: co-sell via IBM/Confluent enterprise accounts, joint solution brief, and event presence at IBM Think / CreatorCon / KafkaSummit

ServiceNow IC on delivery side has been asked to join the IBM kickoff conversations and is prepared to represent technical integration requirements.
