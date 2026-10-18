"""
Automated End-to-End Verification Script for Antigravity.
Tests syntax compilation, FalkorDB connection, multigraph seeding, Track 01/02/03 Cypher queries,
live knowledge injection (inject_live_knowledge), and multi-agent orchestration with special characters in prompts.
"""
import py_compile
import sys
from graph_engine import NexusGraphEngine
from agents import NexusMultiAgentOrchestrator


def main():
    print("1. Verifying Python syntax compilation across all modules...")
    for filename in ["graph_engine.py", "agents.py", "app.py", "verify_zero_bugs.py"]:
        py_compile.compile(filename, doraise=True)
    print("   [PASS] Syntax check passed with 0 errors.")

    print("2. Connecting to FalkorDB and testing clean graph seed...")
    engine = NexusGraphEngine(host="localhost", port=6379, tenant_graph="nexusops_test_verify")
    engine.seed_company_brain(force_reset=True)
    metrics_before = engine.get_graph_metrics()
    assert metrics_before["nodes"] >= 21, f"Expected >= 21 nodes, got {metrics_before['nodes']}"
    assert metrics_before["edges"] >= 30, f"Expected >= 30 edges, got {metrics_before['edges']}"
    print(f"   [PASS] Seeded {metrics_before['nodes']} nodes and {metrics_before['edges']} relationships.")

    print("3. Testing Multi-Agent Orchestrator & Parameterized Cypher Queries (including quotes/special chars)...")
    orchestrator = NexusMultiAgentOrchestrator(engine=engine, api_key="")
    test_prompt = "URGENT: checkout-api's gateway is throwing \"504\" timeouts! Trace blast radius & owner's Slack."
    res = orchestrator.run_investigation(test_prompt, session_id="verify-session-01")

    assert res["target_service"] == "checkout-api"
    assert res["root_cause_service"] == "payment-gateway"
    assert "checkout-api" in res["shortest_fault_path"]
    assert len(res["brain_step"]["owners"]) >= 1
    assert len(res["brain_step"]["runbooks"]) >= 1
    assert len(res["brain_step"]["decisions"]) >= 1
    assert "graph_trace" in res and len(res["graph_trace"]["cypher_queries"]) >= 4
    assert len(res["graph_trace"]["multi_hop_paths"]) >= 3
    print(f"   [PASS] Track 01 + 03 verified. Fault path: {res['shortest_fault_path']}")

    print("4. Verifying Track 02 Cross-Session Persistent Memory & Handoffs...")
    metrics_after = engine.get_graph_metrics()
    assert metrics_after["memories"] == metrics_before["memories"] + 1
    assert metrics_after["handoffs"] == metrics_before["handoffs"] + 1
    print(
        f"   [PASS] Track 02 verified. Persisted Memory {res['memory_step']['new_memory_id']} "
        f"& Handoff {res['memory_step']['new_handoff_id']}."
    )

    print("4b. Verifying Live Graph Mutator (inject_live_knowledge)...")
    mut = engine.inject_live_knowledge(
        service_name="payment-gateway",
        new_status="warning",
        adr_id="ADR-115",
        adr_title="Isolate Dedicated Redis Pool for Payment Idempotency Locks",
        adr_rationale="Decouples payment-gateway lock contention from auth-service token bursts.",
        author_name="Elena Rostova",
    )
    assert mut["adr_id"] == "ADR-115"
    brain_updated = engine.query_company_brain("payment-gateway")
    assert any(d["id"] == "ADR-115" for d in brain_updated["decisions"])
    print(f"   [PASS] Live Knowledge Injection verified: {mut['adr_id']} linked to {mut['service']}.")

    print("5. Seeding production graph ('nexusops_prod') for live Streamlit dashboard...")
    prod_engine = NexusGraphEngine(host="localhost", port=6379, tenant_graph="nexusops_prod")
    prod_engine.seed_company_brain(force_reset=True)
    print("   [PASS] 'nexusops_prod' seeded and ready.")
    print("\nALL CHECKS PASSED WITH ZERO BUGS!")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"\n[FAIL] Verification error: {exc}", file=sys.stderr)
        sys.exit(1)
