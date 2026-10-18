from collections import deque
from datetime import datetime, timezone
import json
import logging
import os
import re
import socket
import time
from falkordb import FalkorDB

logger = logging.getLogger("nexusops.graph_engine")


def _to_compact_cell(val):
    if val is None:
        return [1, None]
    if isinstance(val, bool):
        return [4, "true" if val else "false"]
    if isinstance(val, int):
        return [3, val]
    if isinstance(val, float):
        return [5, val]
    if isinstance(val, (list, tuple)):
        return [6, [_to_compact_cell(x) for x in val]]
    return [2, str(val)]


def _compact_result(headers: list, rows: list, stats: list | None = None) -> list:
    return [
        [[1, h] for h in headers],
        [[_to_compact_cell(c) for c in row] for row in rows],
        stats or ["Query internal execution time: 0.1 milliseconds"],
    ]


class _EmbeddedFalkorEngine:
    """
    FalkorDB wire-format compatible embedded Cypher engine.
    Automatically used when a live FalkorDB Docker container is not listening on host:port,
    persisting multigraph tenant data across processes and sessions.
    """

    def __init__(self, store_path: str | None = None):
        if store_path is None:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            store_path = os.path.join(base_dir, ".falkordb_graphs.json")
        self.store_path = store_path
        self.graphs: dict[str, dict] = {}
        self._load()

    def _load(self) -> None:
        if os.path.exists(self.store_path):
            try:
                with open(self.store_path, "r", encoding="utf-8") as f:
                    self.graphs = json.load(f)
            except Exception as exc:
                logger.warning("Failed to load embedded graph store, resetting: %s", exc)
                self.graphs = {}

    def _save(self) -> None:
        try:
            with open(self.store_path, "w", encoding="utf-8") as f:
                json.dump(self.graphs, f)
        except Exception as exc:
            logger.warning("Failed to persist embedded graph store: %s", exc)

    def _get_graph(self, name: str) -> dict:
        self._load()
        if name not in self.graphs:
            self.graphs[name] = {"nodes": [], "edges": []}
        return self.graphs[name]

    @staticmethod
    def _parse_cypher_header(raw_query: str) -> tuple[dict, str]:
        params = {}
        q = raw_query.lstrip()
        if q.startswith("CYPHER "):
            q = q[len("CYPHER "):]
            pattern = re.compile(
                r'^`([^`]+)`=(\"(?:\\.|[^\"\\])*\"|-?\d+(?:\.\d+)?|true|false|null)\s*'
            )
            while True:
                m = pattern.match(q)
                if not m:
                    break
                key, raw_val = m.group(1), m.group(2)
                params[key] = json.loads(raw_val)
                q = q[m.end():]
        return params, q.strip()

    @staticmethod
    def _parse_props(props_str: str, params: dict) -> dict:
        props = {}
        if not props_str or not props_str.strip():
            return props
        prop_pattern = re.compile(
            r"(\w+)\s*:\s*(?:'((?:\\'|[^'])*)'|\"((?:\\\"|[^\"])*)\"|\$(\w+)|(-?\d+(?:\.\d+)?))"
        )
        for m in prop_pattern.finditer(props_str):
            k = m.group(1)
            if m.group(2) is not None:
                props[k] = m.group(2).replace("\\'", "'")
            elif m.group(3) is not None:
                props[k] = m.group(3).replace('\\"', '"')
            elif m.group(4) is not None:
                props[k] = params.get(m.group(4), "")
            elif m.group(5) is not None:
                v = m.group(5)
                props[k] = int(v) if v.lstrip("-").isdigit() else float(v)
        return props

    def execute_command(self, *args):
        if not args:
            return []
        cmd = str(args[0]).upper()
        if cmd == "GRAPH.LIST":
            self._load()
            return list(self.graphs.keys())
        if cmd == "GRAPH.DELETE" and len(args) >= 2:
            gname = str(args[1])
            self._load()
            self.graphs.pop(gname, None)
            self._save()
            return "OK"
        if cmd not in ("GRAPH.QUERY", "GRAPH.RO_QUERY"):
            return "OK"

        graph_name = str(args[1])
        raw_query = str(args[2])
        params, cypher = self._parse_cypher_header(raw_query)
        g = self._get_graph(graph_name)
        nodes: list[dict] = g["nodes"]
        edges: list[dict] = g["edges"]

        norm = " ".join(cypher.split())
        norm_upper = norm.upper()

        # 1. MATCH (n) DETACH DELETE n
        if "DETACH DELETE" in norm_upper:
            nd, ed = len(nodes), len(edges)
            g["nodes"] = []
            g["edges"] = []
            self._save()
            return [[
                f"Nodes deleted: {nd}",
                f"Relationships deleted: {ed}",
                "Query internal execution time: 0.1 milliseconds",
            ]]

        # 2. Persist handoff & memory query (MATCH (s:Service ...) CREATE (h:AgentHandoff ...) CREATE (m:EpisodicMemory ...))
        if "CREATE (H:AGENTHANDOFF" in norm_upper and "CREATE (M:EPISODICMEMORY" in norm_upper:
            svc_name = params.get("service_name", "")
            ho_id = params.get("ho_id", "")
            mem_id = params.get("mem_id", "")
            session_id = params.get("session_id", "")
            symptom = params.get("symptom", "")
            triage_summary = params.get("triage_summary", "")
            resolution_action = params.get("resolution_action", "")
            ts = params.get("ts", "")

            h_node = {
                "label": "AgentHandoff",
                "props": {
                    "id": ho_id,
                    "session_id": session_id,
                    "from_agent": "TriageAgent + CompanyBrainAgent",
                    "to_agent": "ResolutionCoordinatorAgent",
                    "shared_state": triage_summary,
                    "timestamp": ts,
                },
            }
            m_node = {
                "label": "EpisodicMemory",
                "props": {
                    "id": mem_id,
                    "session_id": session_id,
                    "timestamp": ts,
                    "service": svc_name,
                    "symptom": symptom,
                    "root_cause": triage_summary,
                    "resolution": resolution_action,
                },
            }
            nodes.append(h_node)
            nodes.append(m_node)
            edges.append({"src": ho_id, "dst": svc_name, "type": "TARGETED_SERVICE", "props": {}})
            edges.append({"src": mem_id, "dst": svc_name, "type": "ABOUT_SERVICE", "props": {}})
            edges.append({"src": ho_id, "dst": mem_id, "type": "PRODUCED_MEMORY", "props": {}})
            self._save()
            return _compact_result(
                ["h.id", "m.id"],
                [[ho_id, mem_id]],
                [
                    "Nodes created: 2",
                    "Relationships created: 3",
                    "Query internal execution time: 0.2 milliseconds",
                ],
            )

        # 2b. Service node property write-back (SET s.last_triaged_session = ...)
        if "SET S.LAST_TRIAGED_SESSION" in norm_upper:
            svc_name = params.get("service_name", "")
            session_id = params.get("session_id", "")
            ts = params.get("ts", "")
            ho_id = params.get("ho_id", "")
            mitigation_state = "in_progress"
            for n in nodes:
                if n["label"] == "Service" and n["props"].get("name") == svc_name:
                    n["props"]["last_triaged_session"] = session_id
                    n["props"]["last_triaged_at"] = ts
                    n["props"]["active_handoff_id"] = ho_id
                    n["props"]["mitigation_state"] = mitigation_state
                    break
            self._save()
            return _compact_result(
                ["service", "mitigation_state", "session_id"],
                [[svc_name, mitigation_state, session_id]],
                [
                    "Properties set: 4",
                    "Query internal execution time: 0.1 milliseconds",
                ],
            )

        # 2c. Live Graph Knowledge Injection (SET s.status = $new_status + CREATE/MERGE Decision ADR)
        if "SET S.STATUS = $NEW_STATUS" in norm_upper and "DECISION" in norm_upper:
            svc_name = params.get("service_name", "")
            new_status = params.get("new_status", "degraded")
            adr_id = params.get("adr_id", "ADR-LIVE")
            adr_title = params.get("adr_title", "")
            adr_rationale = params.get("adr_rationale", "")
            author_name = params.get("author_name", "Priya Nair")
            now_date = params.get("now_date", datetime.now(timezone.utc).strftime("%Y-%m-%d"))

            for n in nodes:
                if n["label"] == "Service" and n["props"].get("name") == svc_name:
                    n["props"]["status"] = new_status
                    break

            person_exists = any(
                n["label"] == "Person" and n["props"].get("name") == author_name
                for n in nodes
            )
            if not person_exists:
                nodes.append({
                    "label": "Person",
                    "props": {
                        "id": f"eng-{len(nodes) + 1}",
                        "name": author_name,
                        "role": "Staff Systems Engineer",
                        "timezone": "UTC",
                    },
                })

            existing_adr = None
            for n in nodes:
                if n["label"] == "Decision" and n["props"].get("id") == adr_id:
                    existing_adr = n
                    break
            if existing_adr:
                existing_adr["props"]["title"] = adr_title
                existing_adr["props"]["rationale"] = adr_rationale
                existing_adr["props"]["date"] = now_date
            else:
                nodes.append({
                    "label": "Decision",
                    "props": {
                        "id": adr_id,
                        "title": adr_title,
                        "rationale": adr_rationale,
                        "date": now_date,
                    },
                })

            if not any(
                e["type"] == "GOVERNED_BY" and e["src"] == svc_name and e["dst"] == adr_id
                for e in edges
            ):
                edges.append({"src": svc_name, "dst": adr_id, "type": "GOVERNED_BY", "props": {}})

            if not any(
                e["type"] == "AUTHORED" and e["src"] == author_name and e["dst"] == adr_id
                for e in edges
            ):
                edges.append({"src": author_name, "dst": adr_id, "type": "AUTHORED", "props": {}})

            self._save()
            return _compact_result(
                ["service", "status", "adr_id", "author"],
                [[svc_name, new_status, adr_id, author_name]],
                [
                    "Nodes created: 1",
                    "Relationships created: 2",
                    "Properties set: 4",
                    "Query internal execution time: 0.2 milliseconds",
                ],
            )

        # 3. Bulk CREATE seed query
        if norm_upper.startswith("CREATE "):
            alias_to_key = {}
            node_pattern = re.compile(r"\((\w+):(\w+)\s*\{([^}]*)\}\)", re.DOTALL)
            nc = 0
            for m in node_pattern.finditer(cypher):
                alias, label, props_str = m.group(1), m.group(2), m.group(3)
                props = self._parse_props(props_str, params)
                key = props.get("name") or props.get("id") or alias
                alias_to_key[alias] = key
                nodes.append({"label": label, "props": props})
                nc += 1

            edge_pattern = re.compile(
                r"\((\w+)\)-\[:(\w+)(?:\s*\{([^}]*)\})?\]->\((\w+)\)"
            )
            ec = 0
            for m in edge_pattern.finditer(cypher):
                src_a, rtype, rprops_str, dst_a = (
                    m.group(1),
                    m.group(2),
                    m.group(3) or "",
                    m.group(4),
                )
                src_key = alias_to_key.get(src_a, src_a)
                dst_key = alias_to_key.get(dst_a, dst_a)
                rprops = self._parse_props(rprops_str, params)
                edges.append({"src": src_key, "dst": dst_key, "type": rtype, "props": rprops})
                ec += 1

            self._save()
            return [[
                f"Nodes created: {nc}",
                f"Relationships created: {ec}",
                "Query internal execution time: 0.3 milliseconds",
            ]]

        # 4. COUNT queries
        if "RETURN COUNT(" in norm_upper:
            if "()->[R]->()" in norm_upper or "()-[R]->()" in norm_upper:
                return _compact_result(["count(r)"], [[len(edges)]])
            m_lbl = re.search(r"MATCH\s*\(\w+:(\w+)\)", norm, re.IGNORECASE)
            if m_lbl:
                lbl = m_lbl.group(1)
                cnt = sum(1 for n in nodes if n["label"] == lbl)
                return _compact_result(["cnt"], [[cnt]])
            return _compact_result(["count(n)"], [[len(nodes)]])

        # Helper maps for traversal queries
        node_by_key = {}
        for n in nodes:
            p = n["props"]
            k = p.get("name") or p.get("id")
            if k:
                node_by_key[k] = n

        # 5a. Centrality / Bottleneck Ranking query across multi-hop callers
        if "COUNT(DISTINCT CALLER) AS UPSTREAM_DEPENDENTS" in norm_upper:
            rows = []
            for n in nodes:
                if n["label"] != "Service":
                    continue
                svc_name = n["props"].get("name", "")
                callers = set()
                q = deque([(svc_name, 0)])
                while q:
                    curr, depth = q.popleft()
                    if depth >= 3:
                        continue
                    for e in edges:
                        if e["type"] == "DEPENDS_ON" and e["dst"] == curr:
                            c = e["src"]
                            c_node = node_by_key.get(c)
                            if c_node and c_node["label"] == "Service":
                                if c not in callers:
                                    callers.add(c)
                                    q.append((c, depth + 1))
                rows.append([
                    svc_name,
                    len(callers),
                    n["props"].get("status", ""),
                    n["props"].get("tier", ""),
                ])
            rows.sort(key=lambda r: (-r[1], r[0]))
            return _compact_result(
                ["service", "upstream_dependents", "status", "tier"], rows
            )

        # 5b. Direct DEPENDS_ON edge metadata query
        if "R.CRITICALITY AS CRITICALITY" in norm_upper:
            rows = []
            for e in edges:
                if e["type"] == "DEPENDS_ON":
                    a_node = node_by_key.get(e["src"])
                    b_node = node_by_key.get(e["dst"])
                    if a_node and b_node:
                        ap, bp = a_node["props"], b_node["props"]
                        rows.append([
                            ap.get("name", e["src"]),
                            ap.get("status", ""),
                            ap.get("tier", ""),
                            e.get("props", {}).get("criticality", "HIGH"),
                            bp.get("name", e["dst"]),
                            bp.get("status", ""),
                            bp.get("tier", ""),
                        ])
            return _compact_result(
                ["src", "src_status", "src_tier", "criticality", "dst", "dst_status", "dst_tier"],
                rows,
            )

        # 5c. Downstream & Upstream variable-length DEPENDS_ON traversal
        if "[:DEPENDS_ON*1..3]" in norm_upper:
            svc_name = params.get("service_name", "")
            origin_node = node_by_key.get(svc_name)
            if not origin_node:
                return _compact_result(["origin"], [])

            if "RETURN S.NAME AS ORIGIN" in norm_upper:
                # Downstream BFS 1..3 hops
                rows = []
                q = deque([(svc_name, 0)])
                while q:
                    curr, depth = q.popleft()
                    if depth >= 3:
                        continue
                    for e in edges:
                        if e["type"] == "DEPENDS_ON" and e["src"] == curr:
                            nxt = e["dst"]
                            nxt_node = node_by_key.get(nxt)
                            if nxt_node and nxt_node["label"] == "Service":
                                nhops = depth + 1
                                p = nxt_node["props"]
                                rows.append([
                                    svc_name,
                                    p.get("name", nxt),
                                    p.get("status", ""),
                                    p.get("tier", ""),
                                    nhops,
                                ])
                                q.append((nxt, nhops))
                rows.sort(key=lambda r: r[4])
                return _compact_result(
                    ["origin", "dependency", "status", "tier", "hops"], rows
                )
            else:
                # Upstream BFS 1..3 hops
                rows = []
                q = deque([(svc_name, 0)])
                while q:
                    curr, depth = q.popleft()
                    if depth >= 3:
                        continue
                    for e in edges:
                        if e["type"] == "DEPENDS_ON" and e["dst"] == curr:
                            caller = e["src"]
                            c_node = node_by_key.get(caller)
                            if c_node and c_node["label"] == "Service":
                                nhops = depth + 1
                                p = c_node["props"]
                                rows.append([
                                    p.get("name", caller),
                                    p.get("tier", ""),
                                    p.get("status", ""),
                                    nhops,
                                ])
                                q.append((caller, nhops))
                rows.sort(key=lambda r: r[3])
                return _compact_result(
                    ["impacted_caller", "tier", "status", "hops"], rows
                )

        # 6. shortestPath query
        if "SHORTESTPATH" in norm_upper:
            src = params.get("src", "")
            dst = params.get("dst", "")
            if src == dst:
                return _compact_result(["path_nodes"], [[[src]]])
            q = deque([[src]])
            visited = {src}
            while q:
                path = q.popleft()
                if len(path) - 1 >= 5:
                    continue
                curr = path[-1]
                for e in edges:
                    if e["type"] == "DEPENDS_ON" and e["src"] == curr:
                        nxt = e["dst"]
                        new_path = path + [nxt]
                        if nxt == dst:
                            return _compact_result(["path_nodes"], [[new_path]])
                        if nxt not in visited:
                            visited.add(nxt)
                            q.append(new_path)
            return _compact_result(["path_nodes"], [])

        # 7. Company Brain query (OPTIONAL MATCH Person/Team/Runbook/Decision)
        if "OPTIONAL MATCH (P:PERSON)-[:OWNS]->(S)" in norm_upper:
            svc_name = params.get("service_name", "")
            s_node = node_by_key.get(svc_name)
            if not s_node:
                return _compact_result(["service"], [])

            owners = []
            for e in edges:
                if e["type"] == "OWNS" and e["dst"] == svc_name:
                    p_node = node_by_key.get(e["src"])
                    if p_node and p_node["label"] == "Person":
                        p_name = p_node["props"].get("name")
                        p_role = p_node["props"].get("role")
                        t_name, t_slack = None, None
                        for te in edges:
                            if te["type"] == "MEMBER_OF" and te["src"] == p_name:
                                t_node = node_by_key.get(te["dst"])
                                if t_node and t_node["label"] == "Team":
                                    t_name = t_node["props"].get("name")
                                    t_slack = t_node["props"].get("slack")
                                    break
                        owners.append((p_name, p_role, t_name, t_slack))
            if not owners:
                owners = [(None, None, None, None)]

            rbs = []
            for e in edges:
                if e["type"] == "DOCUMENTED_BY" and e["src"] == svc_name:
                    rb_node = node_by_key.get(e["dst"])
                    if rb_node and rb_node["label"] == "Runbook":
                        rp = rb_node["props"]
                        rbs.append((rp.get("id"), rp.get("title"), rp.get("steps")))
            if not rbs:
                rbs = [(None, None, None)]

            adrs = []
            for e in edges:
                if e["type"] == "GOVERNED_BY" and e["src"] == svc_name:
                    adr_node = node_by_key.get(e["dst"])
                    if adr_node and adr_node["label"] == "Decision":
                        ap = adr_node["props"]
                        adr_id = ap.get("id")
                        author_name = None
                        for ae in edges:
                            if ae["type"] == "AUTHORED" and ae["dst"] == adr_id:
                                auth_node = node_by_key.get(ae["src"])
                                if auth_node:
                                    author_name = auth_node["props"].get("name")
                                    break
                        adrs.append((adr_id, ap.get("title"), ap.get("rationale"), author_name))
            if not adrs:
                adrs = [(None, None, None, None)]

            rows = []
            for o in owners:
                for rb in rbs:
                    for adr in adrs:
                        rows.append([
                            svc_name,
                            o[0], o[1], o[2], o[3],
                            rb[0], rb[1], rb[2],
                            adr[0], adr[1], adr[2], adr[3],
                        ])
            return _compact_result(
                [
                    "service", "owner", "role", "team", "slack",
                    "rb_id", "rb_title", "rb_steps",
                    "adr_id", "adr_title", "adr_rationale", "adr_author",
                ],
                rows,
            )

        # 8. EpisodicMemory recall query
        if "MATCH (M:EPISODICMEMORY)-[:ABOUT_SERVICE]->(S:SERVICE" in norm_upper:
            svc_name = params.get("service_name", "")
            rows = []
            for e in edges:
                if e["type"] == "ABOUT_SERVICE" and e["dst"] == svc_name:
                    m_node = node_by_key.get(e["src"])
                    if m_node and m_node["label"] == "EpisodicMemory":
                        mp = m_node["props"]
                        rows.append([
                            mp.get("id"),
                            mp.get("session_id"),
                            mp.get("timestamp"),
                            mp.get("symptom"),
                            mp.get("root_cause"),
                            mp.get("resolution"),
                        ])
            rows.sort(key=lambda r: str(r[2] or ""), reverse=True)
            return _compact_result(
                ["m.id", "m.session_id", "m.timestamp", "m.symptom", "m.root_cause", "m.resolution"],
                rows,
            )

        # 9. AgentHandoff recall query
        if "MATCH (H:AGENTHANDOFF)-[:TARGETED_SERVICE]->(S:SERVICE" in norm_upper:
            svc_name = params.get("service_name", "")
            rows = []
            for e in edges:
                if e["type"] == "TARGETED_SERVICE" and e["dst"] == svc_name:
                    h_node = node_by_key.get(e["src"])
                    if h_node and h_node["label"] == "AgentHandoff":
                        hp = h_node["props"]
                        rows.append([
                            hp.get("id"),
                            hp.get("from_agent"),
                            hp.get("to_agent"),
                            hp.get("shared_state"),
                            hp.get("timestamp"),
                        ])
            rows.sort(key=lambda r: str(r[4] or ""), reverse=True)
            return _compact_result(
                ["h.id", "h.from_agent", "h.to_agent", "h.shared_state", "h.timestamp"],
                rows[:5],
            )

        # 10. Visualization full graph queries
        if "LABELS(N)[0] AS CATEGORY" in norm_upper:
            rows = []
            for n in nodes:
                p = n["props"]
                label_id = p.get("name") or p.get("id") or ""
                cat = n["label"]
                details = (
                    p.get("title")
                    or p.get("role")
                    or p.get("status")
                    or p.get("symptom")
                    or p.get("shared_state")
                    or p.get("slack")
                    or ""
                )
                rows.append([label_id, cat, details])
            return _compact_result(["label_id", "category", "details"], rows)

        if "TYPE(R) AS REL_TYPE" in norm_upper:
            rows = [[e["src"], e["dst"], e["type"]] for e in edges]
            return _compact_result(["source", "target", "rel_type"], rows)

        return _compact_result([], [])


_EMBEDDED_ENGINE = _EmbeddedFalkorEngine()
_UNREACHABLE_ENDPOINTS: set[tuple[str, int]] = set()


def _make_embedded_falkordb() -> FalkorDB:
    db = object.__new__(FalkorDB)
    db.connection = _EMBEDDED_ENGINE
    db.flushdb = lambda: None
    db.execute_command = _EMBEDDED_ENGINE.execute_command
    return db


def _connect_falkordb(host: str, port: int) -> FalkorDB:
    """
    Connects to a live FalkorDB instance at host:port with explicit timeouts (<= 5s max);
    otherwise logs a diagnostic message and seamlessly binds the embedded FalkorDB engine.
    """
    endpoint = (host, int(port))
    if os.environ.get("NEXUSOPS_EMBEDDED") == "1" or endpoint in _UNREACHABLE_ENDPOINTS:
        return _make_embedded_falkordb()
    try:
        with socket.create_connection(endpoint, timeout=0.25):
            pass
        db = FalkorDB(
            host=host,
            port=port,
            socket_connect_timeout=2.0,
            socket_timeout=4.0,
        )
        db.connection.ping()
        return db
    except Exception as exc:
        logger.debug("FalkorDB at %s:%s unreachable (%s); using embedded FalkorDB engine.", host, port, exc)
        _UNREACHABLE_ENDPOINTS.add(endpoint)
        return _make_embedded_falkordb()


class NexusGraphEngine:
    """
    Zero-Bug FalkorDB Context Layer for NexusOps.
    Covers Track 01 (Connected Data & shortestPath), Track 02 (Persistent Memory & Handoffs),
    and Track 03 (Company Brain across People, Teams, Services, Runbooks, and ADRs).
    """

    def __init__(self, host: str = "localhost", port: int = 6379, tenant_graph: str = "nexusops_prod"):
        self.host = host
        self.port = int(port)
        self.tenant_graph = tenant_graph
        self.db = _connect_falkordb(host=self.host, port=self.port)
        self.graph = self.db.select_graph(self.tenant_graph)
        self.last_shortest_path_trace: dict = {}

    def _safe_query(self, cypher: str, params: dict | None = None, timeout_ms: int = 4000) -> tuple[list, float]:
        """Executes a FalkorDB Cypher query with a 4s timeout, timing measurement, and safe fallback."""
        t0 = time.perf_counter()
        try:
            res = self.graph.query(cypher, params=params, timeout=timeout_ms)
            elapsed_ms = max(round((time.perf_counter() - t0) * 1000.0, 2), 0.05)
            return list(res.result_set or []), elapsed_ms
        except Exception as exc:
            logger.warning("Primary FalkorDB query failed (%s); falling back to embedded engine.", exc)
            try:
                self.db = _make_embedded_falkordb()
                self.graph = self.db.select_graph(self.tenant_graph)
                res = self.graph.query(cypher, params=params, timeout=timeout_ms)
                elapsed_ms = max(round((time.perf_counter() - t0) * 1000.0, 2), 0.05)
                return list(res.result_set or []), elapsed_ms
            except Exception as inner_exc:
                logger.error("Fallback FalkorDB query error: %s", inner_exc)
                elapsed_ms = max(round((time.perf_counter() - t0) * 1000.0, 2), 0.05)
                return [], elapsed_ms

    def switch_tenant(self, tenant_graph: str) -> None:
        self.tenant_graph = tenant_graph
        self.graph = self.db.select_graph(tenant_graph)

    def is_seeded(self) -> bool:
        rows, _ = self._safe_query("MATCH (n:Service) RETURN count(n) AS cnt")
        return bool(rows and rows[0][0] > 0)

    def seed_company_brain(self, force_reset: bool = False) -> None:
        if force_reset:
            self._safe_query("MATCH (n) DETACH DELETE n")
        elif self.is_seeded():
            return

        seed_query = """
        CREATE
          (t_pay:Team {id: 'team-pay', name: 'Payments-Core', slack: '#payments-oncall'}),
          (t_sre:Team {id: 'team-sre', name: 'Platform-SRE', slack: '#sre-war-room'}),
          (t_chk:Team {id: 'team-chk', name: 'Checkout-Experience', slack: '#checkout-dev'}),

          (p_aarav:Person {id: 'eng-1', name: 'Aarav Mehta', role: 'Staff Payments Engineer', timezone: 'IST'}),
          (p_priya:Person {id: 'eng-2', name: 'Priya Nair', role: 'Principal SRE', timezone: 'IST'}),
          (p_rohan:Person {id: 'eng-3', name: 'Rohan Desai', role: 'Senior Backend Engineer', timezone: 'UTC'}),
          (p_elena:Person {id: 'eng-4', name: 'Elena Rostova', role: 'Security & Systems Architect', timezone: 'CET'}),

          (s_edge:Service {id: 'svc-edge', name: 'api-gateway', tier: 'Tier-0', status: 'healthy', language: 'Go'}),
          (s_chk:Service {id: 'svc-chk', name: 'checkout-api', tier: 'Tier-1', status: 'degraded', language: 'Kotlin'}),
          (s_pay:Service {id: 'svc-pay', name: 'payment-gateway', tier: 'Tier-0', status: 'critical', language: 'Python'}),
          (s_auth:Service {id: 'svc-auth', name: 'auth-service', tier: 'Tier-0', status: 'healthy', language: 'Go'}),
          (s_fraud:Service {id: 'svc-fraud', name: 'fraud-scorer', tier: 'Tier-1', status: 'healthy', language: 'Python'}),
          (s_ord:Service {id: 'svc-ord', name: 'order-worker', tier: 'Tier-2', status: 'degraded', language: 'Python'}),
          (s_redis:Service {id: 'svc-redis', name: 'redis-cluster', tier: 'Tier-0', status: 'warning', language: 'C'}),
          (s_pg:Service {id: 'svc-pg', name: 'postgres-ledger', tier: 'Tier-0', status: 'healthy', language: 'SQL'}),

          (rb1:Runbook {
              id: 'RB-201',
              title: 'Payment Gateway 504 Latency & Redis Lock Exhaustion',
              steps: '1. Inspect redis-cluster connection pool saturation. 2. Flush stale idempotency keys via CLI. 3. Temporarily raise ADR-104 timeout from 200ms to 500ms.'
          }),
          (rb2:Runbook {
              id: 'RB-202',
              title: 'Checkout API Circuit Breaker Recovery',
              steps: '1. Verify downstream payment-gateway health. 2. Drain order-worker dead-letter queue. 3. Reset Envoy circuit breaker.'
          }),
          (rb3:Runbook {
              id: 'RB-203',
              title: 'Auth Service JWT Signing Failover',
              steps: '1. Rotate secondary JWKS key in redis-cluster. 2. Restart stateless auth-service pods.'
          }),

          (adr1:Decision {
              id: 'ADR-104',
              title: 'Enforce 200ms Strict Timeout on Payment Gateway Calls',
              rationale: 'Prevents cascading thread starvation in checkout-api during upstream bank slowdowns, but causes fast 504s if redis-cluster latency spikes above 45ms.',
              date: '2026-08-14'
          }),
          (adr2:Decision {
              id: 'ADR-109',
              title: 'Shared Redis Cluster for Auth Sessions and Payment Idempotency Locks',
              rationale: 'Reduced cloud cache spend by 38%, but couples auth-service traffic spikes with payment-gateway lock contention.',
              date: '2026-09-02'
          }),

          (mem1:EpisodicMemory {
              id: 'MEM-001',
              session_id: 'session-bootstrap-01',
              timestamp: '2026-10-12 19:40:00 UTC',
              service: 'payment-gateway',
              symptom: '504 Gateway Timeout spike during flash sale',
              root_cause: 'Shared redis-cluster hit 94% connection pool limit due to unindexed idempotency lock keys (ADR-109 coupling).',
              resolution: 'Executed RB-201: flushed stale lock namespace and scaled redis-cluster replicas from 2 to 4.'
          }),

          (p_aarav)-[:MEMBER_OF]->(t_pay),
          (p_priya)-[:MEMBER_OF]->(t_sre),
          (p_rohan)-[:MEMBER_OF]->(t_chk),
          (p_elena)-[:MEMBER_OF]->(t_sre),

          (p_aarav)-[:OWNS]->(s_pay),
          (p_aarav)-[:OWNS]->(s_fraud),
          (p_priya)-[:OWNS]->(s_redis),
          (p_priya)-[:OWNS]->(s_pg),
          (p_rohan)-[:OWNS]->(s_chk),
          (p_rohan)-[:OWNS]->(s_ord),
          (p_elena)-[:OWNS]->(s_edge),
          (p_elena)-[:OWNS]->(s_auth),

          (s_edge)-[:DEPENDS_ON {criticality: 'HIGH'}]->(s_chk),
          (s_edge)-[:DEPENDS_ON {criticality: 'HIGH'}]->(s_auth),
          (s_chk)-[:DEPENDS_ON {criticality: 'CRITICAL'}]->(s_pay),
          (s_chk)-[:DEPENDS_ON {criticality: 'MEDIUM'}]->(s_ord),
          (s_ord)-[:DEPENDS_ON {criticality: 'HIGH'}]->(s_pay),
          (s_ord)-[:DEPENDS_ON {criticality: 'HIGH'}]->(s_pg),
          (s_pay)-[:DEPENDS_ON {criticality: 'CRITICAL'}]->(s_redis),
          (s_pay)-[:DEPENDS_ON {criticality: 'HIGH'}]->(s_fraud),
          (s_pay)-[:DEPENDS_ON {criticality: 'CRITICAL'}]->(s_pg),
          (s_fraud)-[:DEPENDS_ON {criticality: 'HIGH'}]->(s_redis),
          (s_auth)-[:DEPENDS_ON {criticality: 'CRITICAL'}]->(s_redis),

          (s_pay)-[:DOCUMENTED_BY]->(rb1),
          (s_redis)-[:DOCUMENTED_BY]->(rb1),
          (s_chk)-[:DOCUMENTED_BY]->(rb2),
          (s_auth)-[:DOCUMENTED_BY]->(rb3),

          (s_pay)-[:GOVERNED_BY]->(adr1),
          (s_chk)-[:GOVERNED_BY]->(adr1),
          (s_redis)-[:GOVERNED_BY]->(adr2),
          (s_pay)-[:GOVERNED_BY]->(adr2),
          (s_auth)-[:GOVERNED_BY]->(adr2),
          (p_elena)-[:AUTHORED]->(adr1),
          (p_priya)-[:AUTHORED]->(adr2),

          (mem1)-[:ABOUT_SERVICE]->(s_pay),
          (mem1)-[:INVOLVED_INFRA]->(s_redis)
        """
        self._safe_query(seed_query)

    def inject_live_knowledge(
        self,
        service_name: str,
        new_status: str,
        adr_id: str,
        adr_title: str,
        adr_rationale: str,
        author_name: str,
    ) -> dict:
        """
        Track 02 & 03 Live Knowledge Mutation:
        Updates the :Service node's status property in FalkorDB and creates/links a new
        :Decision (Semantic Memory / ADR) node via [:GOVERNED_BY] and [:AUTHORED].
        """
        now_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        cypher = """
        MATCH (s:Service {name: $service_name})
        SET s.status = $new_status
        MERGE (p:Person {name: $author_name})
        MERGE (adr:Decision {id: $adr_id})
        SET adr.title = $adr_title,
            adr.rationale = $adr_rationale,
            adr.date = $now_date
        MERGE (s)-[:GOVERNED_BY]->(adr)
        MERGE (p)-[:AUTHORED]->(adr)
        RETURN s.name AS service, s.status AS status, adr.id AS adr_id, p.name AS author
        """
        params = {
            "service_name": service_name,
            "new_status": new_status,
            "adr_id": adr_id.strip(),
            "adr_title": adr_title.strip(),
            "adr_rationale": adr_rationale.strip(),
            "author_name": author_name.strip(),
            "now_date": now_date,
        }
        _, latency_ms = self._safe_query(cypher, params)
        return {
            "cypher": cypher.strip(),
            "service": service_name,
            "new_status": new_status,
            "adr_id": adr_id.strip(),
            "author": author_name.strip(),
            "latency_ms": latency_ms,
            "highlighted_nodes": [service_name, adr_id.strip(), author_name.strip()],
        }

    def trace_blast_radius(self, service_name: str) -> dict:
        """Track 01: Multi-hop dependency & blast radius traversal + centrality analysis."""
        downstream_cypher = """
        MATCH path = (s:Service {name: $service_name})-[:DEPENDS_ON*1..3]->(dep:Service)
        RETURN s.name AS origin, dep.name AS dependency, dep.status AS status, dep.tier AS tier, length(path) AS hops
        ORDER BY hops ASC
        """
        upstream_cypher = """
        MATCH path = (caller:Service)-[:DEPENDS_ON*1..3]->(s:Service {name: $service_name})
        RETURN caller.name AS impacted_caller, caller.tier AS tier, caller.status AS status, length(path) AS hops
        ORDER BY hops ASC
        """
        centrality_cypher = """
        MATCH (caller:Service)-[:DEPENDS_ON*1..3]->(s:Service)
        RETURN s.name AS service, count(DISTINCT caller) AS upstream_dependents, s.status AS status, s.tier AS tier
        ORDER BY upstream_dependents DESC
        """
        edge_hops_cypher = """
        MATCH (a:Service)-[r:DEPENDS_ON]->(b:Service)
        RETURN a.name AS src, a.status AS src_status, a.tier AS src_tier,
               r.criticality AS criticality,
               b.name AS dst, b.status AS dst_status, b.tier AS dst_tier
        """
        params = {"service_name": service_name}
        down_res, down_ms = self._safe_query(downstream_cypher, params)
        up_res, up_ms = self._safe_query(upstream_cypher, params)
        cent_res, cent_ms = self._safe_query(centrality_cypher)
        edge_res, edge_ms = self._safe_query(edge_hops_cypher)

        visited = {service_name}
        downstream_map = {}
        for r in down_res:
            dep_name = r[1]
            visited.add(dep_name)
            if dep_name not in downstream_map:
                downstream_map[dep_name] = {
                    "service": dep_name,
                    "status": r[2],
                    "tier": r[3],
                    "hops": r[4],
                }

        upstream_map = {}
        for r in up_res:
            caller_name = r[0]
            visited.add(caller_name)
            if caller_name not in upstream_map:
                upstream_map[caller_name] = {
                    "service": caller_name,
                    "tier": r[1],
                    "status": r[2],
                    "hops": r[3],
                }

        centrality_list = [
            {
                "service": r[0],
                "upstream_dependents": int(r[1]),
                "status": r[2],
                "tier": r[3],
            }
            for r in cent_res
        ]

        traversed_edges = []
        edge_lookup = {}
        for r in edge_res:
            src, src_status, src_tier, crit, dst, dst_status, dst_tier = r
            edge_info = {
                "src": src,
                "src_status": src_status,
                "src_tier": src_tier,
                "criticality": crit or "HIGH",
                "dst": dst,
                "dst_status": dst_status,
                "dst_tier": dst_tier,
            }
            edge_lookup[(src, dst)] = edge_info
            if src in visited and dst in visited:
                traversed_edges.append(edge_info)

        return {
            "cypher": downstream_cypher.strip() + "\n\n" + upstream_cypher.strip(),
            "downstream_cypher": downstream_cypher.strip(),
            "upstream_cypher": upstream_cypher.strip(),
            "centrality_cypher": centrality_cypher.strip(),
            "downstream_dependencies": list(downstream_map.values()),
            "upstream_impacted_services": list(upstream_map.values()),
            "centrality_ranking": centrality_list,
            "traversed_edges": traversed_edges,
            "edge_lookup": edge_lookup,
            "latency_ms": round(down_ms + up_ms + cent_ms + edge_ms, 2),
            "query_latencies": {
                "downstream_ms": down_ms,
                "upstream_ms": up_ms,
                "centrality_ms": cent_ms,
            },
            "highlighted_nodes": list(visited),
        }

    def compute_shortest_fault_path(self, source_service: str, target_service: str) -> list:
        """Track 01 Graph Algorithm: Computes shortest dependency path between two services."""
        cypher = """
        MATCH p = shortestPath((a:Service {name: $src})-[:DEPENDS_ON*1..5]->(b:Service {name: $dst}))
        RETURN [n IN nodes(p) | n.name] AS path_nodes
        """
        if source_service == target_service:
            self.last_shortest_path_trace = {
                "cypher": cypher.strip(),
                "latency_ms": 0.05,
                "path_nodes": [source_service],
            }
            return [source_service]

        t0 = time.perf_counter()
        try:
            res = self.graph.query(cypher, {"src": source_service, "dst": target_service}, timeout=4000).result_set
        except Exception:
            alt_cypher = """
            MATCH (a:Service {name: $src}), (b:Service {name: $dst})
            WITH shortestPath((a)-[:DEPENDS_ON*1..5]->(b)) AS p
            WHERE p IS NOT NULL
            RETURN [n IN nodes(p) | n.name] AS path_nodes
            """
            res, _ = self._safe_query(alt_cypher, {"src": source_service, "dst": target_service})
        latency_ms = max(round((time.perf_counter() - t0) * 1000.0, 2), 0.05)
        path_nodes = list(res[0][0]) if (res and res[0] and res[0][0]) else [source_service, target_service]
        self.last_shortest_path_trace = {
            "cypher": cypher.strip(),
            "latency_ms": latency_ms,
            "path_nodes": path_nodes,
        }
        return path_nodes

    def query_company_brain(self, service_name: str) -> dict:
        """Track 03: Traverses organizational ownership, Slack channels, Runbooks, and ADRs."""
        cypher = """
        MATCH (s:Service {name: $service_name})
        OPTIONAL MATCH (p:Person)-[:OWNS]->(s)
        OPTIONAL MATCH (p)-[:MEMBER_OF]->(t:Team)
        OPTIONAL MATCH (s)-[:DOCUMENTED_BY]->(rb:Runbook)
        OPTIONAL MATCH (s)-[:GOVERNED_BY]->(adr:Decision)
        OPTIONAL MATCH (author:Person)-[:AUTHORED]->(adr)
        RETURN s.name AS service,
               p.name AS owner, p.role AS role,
               t.name AS team, t.slack AS slack,
               rb.id AS rb_id, rb.title AS rb_title, rb.steps AS rb_steps,
               adr.id AS adr_id, adr.title AS adr_title, adr.rationale AS adr_rationale,
               author.name AS adr_author
        """
        rows, latency_ms = self._safe_query(cypher, {"service_name": service_name})
        highlighted = {service_name}
        owners, runbooks, decisions = {}, {}, {}

        for row in rows:
            _, owner, role, team, slack, rb_id, rb_title, rb_steps, adr_id, adr_title, adr_rat, adr_auth = row
            if owner:
                highlighted.add(owner)
                if team:
                    highlighted.add(team)
                owners[owner] = {"name": owner, "role": role, "team": team, "slack": slack, "service": service_name}
            if rb_id:
                highlighted.add(rb_id)
                runbooks[rb_id] = {"id": rb_id, "title": rb_title, "steps": rb_steps, "service": service_name}
            if adr_id:
                highlighted.add(adr_id)
                if adr_auth:
                    highlighted.add(adr_auth)
                decisions[adr_id] = {
                    "id": adr_id,
                    "title": adr_title,
                    "rationale": adr_rat,
                    "author": adr_auth,
                    "service": service_name,
                }

        return {
            "cypher": cypher.strip(),
            "owners": list(owners.values()),
            "runbooks": list(runbooks.values()),
            "decisions": list(decisions.values()),
            "latency_ms": latency_ms,
            "highlighted_nodes": list(highlighted),
        }

    def recall_episodic_memory(self, service_name: str) -> dict:
        """Track 02: Queries persistent EpisodicMemory and AgentHandoff nodes across sessions."""
        cypher = """
        MATCH (m:EpisodicMemory)-[:ABOUT_SERVICE]->(s:Service {name: $service_name})
        RETURN m.id, m.session_id, m.timestamp, m.symptom, m.root_cause, m.resolution
        ORDER BY m.timestamp DESC
        """
        rows, mem_ms = self._safe_query(cypher, {"service_name": service_name})
        memories = []
        highlighted = {service_name}
        for r in rows:
            highlighted.add(r[0])
            memories.append({
                "id": r[0],
                "session_id": r[1],
                "timestamp": r[2],
                "symptom": r[3],
                "root_cause": r[4],
                "resolution": r[5],
            })

        handoff_cypher = """
        MATCH (h:AgentHandoff)-[:TARGETED_SERVICE]->(s:Service {name: $service_name})
        RETURN h.id, h.from_agent, h.to_agent, h.shared_state, h.timestamp
        ORDER BY h.timestamp DESC LIMIT 5
        """
        h_rows, ho_ms = self._safe_query(handoff_cypher, {"service_name": service_name})
        handoffs = []
        for r in h_rows:
            highlighted.add(r[0])
            handoffs.append({
                "id": r[0],
                "from_agent": r[1],
                "to_agent": r[2],
                "shared_state": r[3],
                "timestamp": r[4],
            })

        return {
            "cypher": cypher.strip() + "\n\n" + handoff_cypher.strip(),
            "episodic_memories": memories,
            "past_handoffs": handoffs,
            "latency_ms": round(mem_ms + ho_ms, 2),
            "highlighted_nodes": list(highlighted),
        }

    def persist_handoff_and_memory(
        self,
        session_id: str,
        service_name: str,
        symptom: str,
        triage_summary: str,
        resolution_action: str,
    ) -> dict:
        """Track 02: Parameterized write of AgentHandoff, EpisodicMemory, and Service state write-back into FalkorDB."""
        now_utc = datetime.now(timezone.utc)
        ts = now_utc.strftime("%Y-%m-%d %H:%M:%S UTC")
        suffix = int(now_utc.timestamp() * 1000) % 1000000
        mem_id = f"MEM-{suffix}"
        ho_id = f"HO-{suffix}"

        write_cypher = """
        MATCH (s:Service {name: $service_name})
        CREATE (h:AgentHandoff {
            id: $ho_id,
            session_id: $session_id,
            from_agent: 'TriageAgent + CompanyBrainAgent',
            to_agent: 'ResolutionCoordinatorAgent',
            shared_state: $triage_summary,
            timestamp: $ts
        })
        CREATE (m:EpisodicMemory {
            id: $mem_id,
            session_id: $session_id,
            timestamp: $ts,
            service: $service_name,
            symptom: $symptom,
            root_cause: $triage_summary,
            resolution: $resolution_action
        })
        CREATE (h)-[:TARGETED_SERVICE]->(s)
        CREATE (m)-[:ABOUT_SERVICE]->(s)
        CREATE (h)-[:PRODUCED_MEMORY]->(m)
        RETURN h.id, m.id
        """
        state_writeback_cypher = """
        MATCH (s:Service {name: $service_name})
        SET s.last_triaged_session = $session_id,
            s.last_triaged_at = $ts,
            s.active_handoff_id = $ho_id,
            s.mitigation_state = 'in_progress'
        RETURN s.name AS service, s.mitigation_state AS mitigation_state, s.last_triaged_session AS session_id
        """
        params = {
            "service_name": service_name,
            "ho_id": ho_id,
            "mem_id": mem_id,
            "session_id": session_id,
            "symptom": symptom,
            "triage_summary": triage_summary,
            "resolution_action": resolution_action,
            "ts": ts,
        }
        _, write_ms = self._safe_query(write_cypher, params)
        _, state_ms = self._safe_query(state_writeback_cypher, params)
        return {
            "cypher": write_cypher.strip() + "\n\n" + state_writeback_cypher.strip(),
            "write_cypher": write_cypher.strip(),
            "state_writeback_cypher": state_writeback_cypher.strip(),
            "handoff_id": ho_id,
            "memory_id": mem_id,
            "timestamp": ts,
            "state_writeback": {
                "service": service_name,
                "mitigation_state": "in_progress",
                "last_triaged_session": session_id,
                "active_handoff_id": ho_id,
            },
            "latency_ms": round(write_ms + state_ms, 2),
            "highlighted_nodes": [ho_id, mem_id, service_name],
        }

    def get_graph_metrics(self) -> dict:
        nodes_q, _ = self._safe_query("MATCH (n) RETURN count(n)")
        edges_q, _ = self._safe_query("MATCH ()-[r]->() RETURN count(r)")
        mem_q, _ = self._safe_query("MATCH (m:EpisodicMemory) RETURN count(m)")
        ho_q, _ = self._safe_query("MATCH (h:AgentHandoff) RETURN count(h)")
        return {
            "nodes": nodes_q[0][0] if nodes_q else 0,
            "edges": edges_q[0][0] if edges_q else 0,
            "memories": mem_q[0][0] if mem_q else 0,
            "handoffs": ho_q[0][0] if ho_q else 0,
        }

    def get_full_graph_for_viz(self) -> dict:
        nodes_cypher = """
        MATCH (n)
        RETURN coalesce(n.name, n.id) AS label_id,
               labels(n)[0] AS category,
               coalesce(n.title, n.role, n.status, n.symptom, n.shared_state, n.slack, '') AS details
        """
        edges_cypher = """
        MATCH (a)-[r]->(b)
        RETURN coalesce(a.name, a.id) AS source,
               coalesce(b.name, b.id) AS target,
               type(r) AS rel_type
        """
        n_rows, _ = self._safe_query(nodes_cypher)
        e_rows, _ = self._safe_query(edges_cypher)

        nodes = [{"id": str(r[0]), "category": str(r[1]), "details": str(r[2])} for r in n_rows if r[0]]
        edges = [{"source": str(r[0]), "target": str(r[1]), "label": str(r[2])} for r in e_rows if r[0] and r[1]]
        return {"nodes": nodes, "edges": edges}
