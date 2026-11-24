# Flash Sale & Ticketing Platform: Design Docs

Read in this order:

| # | File | What's in it |
|---|---|---|
| 1 | [01-problem-and-goals.md](01-problem-and-goals.md) | objective, what went wrong, goals |
| 2 | [02-scale-and-numbers.md](02-scale-and-numbers.md) | given numbers, assumptions, back-of-envelope |
| 3 | [03-entities-apis-schema.md](03-entities-apis-schema.md) | entities, APIs, Redis keys, Postgres tables |
| 4 | [04-architecture.md](04-architecture.md) | HLD diagram, each component, end-to-end flows |
| 5 | [05-trade-offs.md](05-trade-offs.md) | our design choices and the trade-offs behind them |
| 6 | [06-scalability-and-fault-tolerance.md](06-scalability-and-fault-tolerance.md) | scaling, overload, failures |
| 7 | [07-testing-and-observability.md](07-testing-and-observability.md) | load and chaos tests, metrics, alerts, runbook |
| 8 | [08-execution-plan.md](08-execution-plan.md) | v0, v1, v2 rollout |
| 9 | [adr/](adr/README.md) | one short record for each big decision: what we chose, what we gave up |
| 10 | [hld.png](hld.png) | the HLD diagram (edit it in hld.excalidraw) |

## Where each brief question is answered

| Brief asks for | Where |
|---|---|
| Problem understanding & goals | 01-problem-and-goals.md |
| Proposed architecture (HLD, components, data, caching, flows) | 03-entities-apis-schema.md, 04-architecture.md |
| Scalability, performance & fault tolerance | 06-scalability-and-fault-tolerance.md |
| Load testing & observability | 07-testing-and-observability.md |
| Trade-offs | 05-trade-offs.md |
| Execution plan (v0, v1, v2) | 08-execution-plan.md |
| Working prototype | `reservation-service/`, `loadtest/` |
