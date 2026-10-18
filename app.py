import uuid
import streamlit as st
import streamlit.components.v1 as components
from pyvis.network import Network

from graph_engine import NexusGraphEngine
from agents import KNOWN_SERVICES, NexusMultiAgentOrchestrator

st.set_page_config(
    page_title="NexusOps | FalkorDB AI Agent Brain",
    page_icon="🕸️",
    layout="wide",
)

# Initialize all session_state keys at the top before any widgets render
if "session_id" not in st.session_state:
    st.session_state.session_id = f"sess-{str(uuid.uuid4())[:6]}"
if "highlighted_nodes" not in st.session_state:
    st.session_state.highlighted_nodes = []
if "last_result" not in st.session_state:
    st.session_state.last_result = None
if "last_mutation" not in st.session_state:
    st.session_state.last_mutation = None

with st.sidebar:
    st.title("🕸️ NexusOps Control")
    st.caption("Powered by **FalkorDB** Context Layer")

    st.subheader("1. FalkorDB Connection")
    falkor_host = st.text_input("Host", value="localhost", key="input_falkor_host")
    falkor_port = st.number_input("Port", value=6379, step=1, key="input_falkor_port")

    st.subheader("2. Multigraph Tenant Isolation")
    tenant_graph = st.selectbox(
        "Active Isolated Graph (Track 02)",
        options=["nexusops_prod", "nexusops_staging", "nexusops_eu_tenant"],
        help="FalkorDB supports thousands of isolated graphs in a single instance.",
        key="select_tenant_graph",
    )

    st.subheader("3. LLM API Key (Optional)")
    api_key = st.text_input(
        "Gemini / Groq / OpenAI / OpenRouter Key",
        type="password",
        help="Auto-detects AIza... (Gemini), gsk_... (Groq), sk-or-... (OpenRouter), or sk-... (OpenAI). Works in deterministic Graph-Native mode if left blank.",
        key="input_llm_api_key",
    )

    st.divider()
    st.markdown(f"**Active Session ID:** `{st.session_state.session_id}`")
    col_s1, col_s2 = st.columns(2)
    with col_s1:
        if st.button("🔄 New Session", use_container_width=True, key="btn_new_session"):
            st.session_state.session_id = f"sess-{str(uuid.uuid4())[:6]}"
            st.toast(f"New session started: {st.session_state.session_id}. FalkorDB memory persists!")
            st.rerun()
    with col_s2:
        reset_clicked = st.button("🧹 Reset Graph", use_container_width=True, key="btn_reset_graph")

try:
    engine = NexusGraphEngine(host=falkor_host, port=int(falkor_port), tenant_graph=tenant_graph)
    if reset_clicked:
        engine.seed_company_brain(force_reset=True)
        st.session_state.highlighted_nodes = []
        st.session_state.last_result = None
        st.session_state.last_mutation = None
        st.toast("FalkorDB graph reset to clean baseline!")
    else:
        engine.seed_company_brain(force_reset=False)
except Exception as e:
    st.error(
        f"Could not connect to FalkorDB at `{falkor_host}:{falkor_port}`. "
        f"Ensure Docker is running (`docker compose up -d`).\n\nDetails: {e}"
    )
    st.stop()

st.title("🕸️ NexusOps: Autonomous Incident & Company Brain")
st.markdown(
    "**Multi-Agent Context Layer on FalkorDB** — *Unifying Connected Data (Track 01), "
    "Persistent Multi-Agent Memory (Track 02), and Organizational Knowledge (Track 03).*"
)

metrics = engine.get_graph_metrics()
m1, m2, m3, m4, m5 = st.columns(5)
m1.metric("Active FalkorDB Graph", tenant_graph)
m2.metric("Graph Nodes", metrics["nodes"])
m3.metric("Graph Relationships", metrics["edges"])
m4.metric("Episodic Memories", metrics["memories"])
m5.metric("Agent Handoffs", metrics["handoffs"])

st.divider()

col_graph, col_agent = st.columns([1.15, 1.0], gap="large")


def render_pyvis_graph(graph_data: dict, highlighted: list) -> None:
    net = Network(
        height="540px",
        width="100%",
        bgcolor="#0e1117",
        font_color="#ffffff",
        directed=True,
        cdn_resources="in_line",
    )
    net.barnes_hut(gravity=-3500, central_gravity=0.35, spring_length=120)

    color_map = {
        "Service": "#3b82f6",
        "Person": "#10b981",
        "Team": "#8b5cf6",
        "Runbook": "#f59e0b",
        "Decision": "#ec4899",
        "EpisodicMemory": "#06b6d4",
        "AgentHandoff": "#f97316",
    }

    for n in graph_data["nodes"]:
        node_id = n["id"]
        cat = n["category"]
        is_hit = node_id in highlighted
        color = "#facc15" if is_hit else color_map.get(cat, "#94a3b8")
        size = 30 if is_hit else 18
        border_width = 4 if is_hit else 1

        safe_details = n["details"].encode("ascii", "ignore").decode("ascii")
        title_text = f"[{cat}] {node_id} - {safe_details}"
        net.add_node(
            node_id,
            label=node_id,
            title=title_text,
            color={"background": color, "border": "#ef4444" if is_hit else color},
            size=size,
            borderWidth=border_width,
        )

    for e in graph_data["edges"]:
        both_hit = (e["source"] in highlighted) and (e["target"] in highlighted)
        net.add_edge(
            e["source"],
            e["target"],
            label=e["label"],
            color="#facc15" if both_hit else "#475569",
            width=3 if both_hit else 1,
            arrows="to",
        )

    html_content = net.generate_html()
    components.html(html_content, height=555)


with col_graph:
    st.subheader("🔍 Live FalkorDB Context Graph & Decision Path")
    st.caption(
        "🟡 **Yellow/Red Border Nodes** = Traversed by agents on the active query | "
        "🔵 Service | 🟢 Engineer | 🟣 Team | 🟠 Runbook | 🩷 ADR Decision | 🩵 Episodic Memory"
    )
    viz_data = engine.get_full_graph_for_viz()
    render_pyvis_graph(viz_data, st.session_state.highlighted_nodes)

with col_agent:
    with st.expander("⚡ Live Graph Mutator: Keep Company Brain Current (Track 02 & 03)", expanded=False):
        st.caption(
            "Mutate live service health and inject new Architecture Decision Records (ADRs) / Procedural Rules "
            "directly into FalkorDB. Next agent queries will immediately reason over this updated topology."
        )
        mut_c1, mut_c2 = st.columns(2)
        with mut_c1:
            mut_service = st.selectbox(
                "Target Service",
                options=KNOWN_SERVICES,
                index=0,
                key="mut_select_service",
            )
            mut_adr_id = st.text_input(
                "New ADR / Rule ID",
                value="ADR-115",
                key="mut_input_adr_id",
            )
        with mut_c2:
            mut_status = st.selectbox(
                "New Operational Status",
                options=["critical", "warning", "degraded", "healthy"],
                index=1,
                key="mut_select_status",
            )
            mut_author = st.selectbox(
                "Authoring Engineer",
                options=["Elena Rostova", "Priya Nair", "Aarav Mehta", "Rohan Desai"],
                index=0,
                key="mut_select_author",
            )
        mut_title = st.text_input(
            "ADR / Procedural Rule Title",
            value="Isolate Dedicated Redis Pool for Payment Idempotency Locks",
            key="mut_input_adr_title",
        )
        mut_rationale = st.text_input(
            "Architectural Rationale",
            value="Decouples payment-gateway lock contention from auth-service token bursts during flash sales.",
            key="mut_input_adr_rationale",
        )
        if st.button(
            "⚡ Inject ADR & Update Service in FalkorDB",
            use_container_width=True,
            key="btn_inject_live_knowledge",
        ):
            mut_res = engine.inject_live_knowledge(
                service_name=mut_service,
                new_status=mut_status,
                adr_id=mut_adr_id,
                adr_title=mut_title,
                adr_rationale=mut_rationale,
                author_name=mut_author,
            )
            st.session_state.last_mutation = mut_res
            st.session_state.highlighted_nodes = mut_res["highlighted_nodes"]
            st.toast(f"Injected {mut_res['adr_id']} and updated {mut_service} -> {mut_status} in FalkorDB!")
            st.rerun()

        if st.session_state.last_mutation:
            lm = st.session_state.last_mutation
            st.success(
                f"Live Graph Updated in `{lm['latency_ms']} ms`: `{lm['service']}` status -> `{lm['new_status']}`, "
                f"linked to `{lm['adr_id']}` (authored by **{lm['author']}**)."
            )
            st.code(lm["cypher"], language="cypher")

    st.subheader("🤖 Multi-Agent Incident & Company Brain Console")
    st.caption("Select a preset scenario or type any engineering incident query below:")

    p1, p2, p3 = st.columns(3)
    preset_prompt = None
    if p1.button("🚨 Scenario 1: Checkout 504 Spike", use_container_width=True, key="btn_scenario_1"):
        preset_prompt = (
            "URGENT: checkout-api is throwing 504 Gateway Timeouts! Trace the multi-hop root cause, "
            "blast radius, governing ADRs, who owns the failing services, and execute the remediation runbook."
        )
    if p2.button("🔐 Scenario 2: Auth & Redis Contention", use_container_width=True, key="btn_scenario_2"):
        preset_prompt = (
            "We see high latency on auth-service and redis-cluster. Why are these coupled in our architecture, "
            "what breaks upstream if redis-cluster fails, and who authored that decision?"
        )
    if p3.button("🧠 Scenario 3: Payment Gateway Memory", use_container_width=True, key="btn_scenario_3"):
        preset_prompt = (
            "Investigate payment-gateway failures: recall all past episodic memories across previous sessions, "
            "check downstream fraud-scorer and redis dependencies, and coordinate a resolution handoff."
        )

    custom_prompt = st.chat_input(
        "Ask NexusOps (e.g., 'What breaks if payment-gateway goes down and who owns it?')",
        key="chat_incident_prompt",
    )
    active_query = preset_prompt or custom_prompt

    if active_query:
        orchestrator = NexusMultiAgentOrchestrator(engine=engine, api_key=api_key)
        with st.spinner("Agents traversing FalkorDB graph, recalling memory & writing state handoffs..."):
            result = orchestrator.run_investigation(active_query, session_id=st.session_state.session_id)
            st.session_state.last_result = result
            st.session_state.highlighted_nodes = result["highlighted_nodes"]
            st.rerun()

    if st.session_state.last_result:
        res = st.session_state.last_result
        trace = res.get("graph_trace", {})
        st.success(
            f"**Trace Complete** — Highlighted **{len(res['highlighted_nodes'])} connected nodes** in FalkorDB "
            f"& persisted Memory `{res['memory_step']['new_memory_id']}` + Handoff `{res['memory_step']['new_handoff_id']}`."
        )
        st.markdown(res["narrative"])

        with st.expander("🔍 FalkorDB Graph Reasoning & Cypher Trace", expanded=True):
            tm1, tm2, tm3, tm4 = st.columns(4)
            tm1.metric("FalkorDB Latency", f"{trace.get('total_latency_ms', 0.5)} ms")
            tm2.metric("Cypher Queries", trace.get("queries_executed", 6))
            tm3.metric("Nodes Traversed", trace.get("nodes_traversed", len(res["highlighted_nodes"])))
            tm4.metric("Fault Path Hops", max(len(res["shortest_fault_path"]) - 1, 1))

            st.info(f"**Why the Graph Changed the Agent's Decision:** {trace.get('decision_rationale', '')}")

            st.markdown("**Multi-Hop Graph Traversal Path (`Node -> [RELATIONSHIP] -> Node`):**")
            st.code("\n".join(trace.get("multi_hop_paths", [])), language="text")

            st.markdown("**Executed Read & Write FalkorDB Cypher Queries:**")
            cypher_blocks = []
            for q_item in trace.get("cypher_queries", []):
                cypher_blocks.append(
                    f"// [{q_item['mode']}] {q_item['step']} ({q_item['latency_ms']} ms)\n{q_item['cypher']}"
                )
            st.code("\n\n".join(cypher_blocks), language="cypher")

            st.markdown("**Graph State Write-Back & Bottleneck Centrality:**")
            st.json(
                {
                    "service_state_writeback": trace.get("state_writeback", {}),
                    "top_bottleneck_centrality": trace.get("centrality_ranking", []),
                }
            )

        with st.expander("🕵️ Track 01 Trace: Multi-Hop Cypher & Blast Radius Data", expanded=False):
            st.code(res["triage_step"]["cypher"], language="cypher")
            st.json(
                {
                    "shortest_fault_path": res["shortest_fault_path"],
                    "downstream_dependencies": res["triage_step"]["downstream_dependencies"],
                    "upstream_impacted_services": res["triage_step"]["upstream_impacted_services"],
                }
            )

        with st.expander("🧠 Track 03 Trace: Company Brain Provenance Cypher (People, Runbooks, ADRs)", expanded=False):
            st.code(res["brain_step"]["cypher"], language="cypher")
            st.json(
                {
                    "owners": res["brain_step"]["owners"],
                    "runbooks": res["brain_step"]["runbooks"],
                    "decisions": res["brain_step"]["decisions"],
                }
            )

        with st.expander("🤖 Track 02 Trace: Persistent Memory Recall & Agent Handoff Cypher", expanded=False):
            st.markdown("**1. Cross-Session Memory Recall Query:**")
            st.code(res["memory_step"]["recall_cypher"], language="cypher")
            st.markdown("**2. Multi-Agent State Handoff & Episodic Memory Write Query:**")
            st.code(res["memory_step"]["write_cypher"], language="cypher")
            st.json(res["memory_step"]["prior_memories"])
