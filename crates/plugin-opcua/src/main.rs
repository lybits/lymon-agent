// Lymon connector plugin: OPC-UA (built on lymon-collector-sdk).
//
// Connects to an OPC-UA server and reads node values, one Read service call per
// poll. The async session + its event loop are cached across polls (connect
// once, reuse) and torn down on error so the next poll reconnects.
//
// Connector config / ingest selection it expects:
//   config:    { "endpoint_url": "opc.tcp://10.0.0.5:4840",
//                "security_policy": "None",            // None (default) for now
//                "user": "alice" }                     // optional; omit = anonymous
//   secrets:   { "password": "…" }                     // paired with config.user
//   selection: { "node_id": "ns=2;s=Temperature" }     // OPC-UA text NodeId
//
// Deploy: build, then under the agent's plugins dir put
//   plugins/lymon-plugin-opcua/plugin.json     (see plugin.json in this crate)
//   plugins/lymon-plugin-opcua/lymon-plugin-opcua   (this binary)
// and create a connector type="opcua" host=agent in the portal.

use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use lymon_collector_sdk::{run, Collector, Discovery, Node, ReadRequest, Sample};
use opcua::client::{Client, ClientBuilder, DataChangeCallback, IdentityToken, Password, Session};
use opcua::types::{
    BrowseDescription, BrowseDirection, DataValue, MessageSecurityMode, MonitoredItemCreateRequest,
    NodeClass, NodeId, ObjectId, ReadValueId, ReferenceTypeId, StatusCode, TimestampsToReturn,
    UserTokenPolicy, Variant,
};
use tokio::runtime::Runtime;

/// A live, connected session for one endpoint. Kept across polls.
struct Conn {
    endpoint: String,
    session: Arc<Session>,
}

struct OpcUaConnector {
    rt: Runtime,
    conn: Option<Conn>,
}

impl OpcUaConnector {
    fn new() -> Self {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("build tokio runtime");
        Self { rt, conn: None }
    }

    /// Connect to `endpoint` and spawn the event loop on the runtime.
    ///
    /// Bounded by CONNECT_TIMEOUT: `wait_for_connection()` (and the endpoint
    /// discovery before it) have no timeout of their own, so a server that's
    /// unreachable, or that exposes no SecurityPolicy=None endpoint, would
    /// leave the session-retry loop spinning forever — the poll would hang
    /// silently with no log line. Timing out turns that into a clear error.
    async fn connect(endpoint: &str, identity: IdentityToken) -> Result<Arc<Session>, String> {
        let work = async {
            let mut client: Client = ClientBuilder::new()
                .application_name("Lymon Agent")
                .application_uri("urn:lymon:agent")
                .product_uri("urn:lymon:agent")
                .trust_server_certs(true)
                .create_sample_keypair(true)
                .session_retry_limit(3)
                .client()
                .map_err(|e| format!("failed to build OPC-UA client: {e:?}"))?;

            let (session, event_loop) = client
                .connect_to_matching_endpoint(
                    (
                        endpoint,
                        SecurityPolicy_None,
                        MessageSecurityMode::None,
                        UserTokenPolicy::anonymous(),
                    ),
                    identity,
                )
                .await
                .map_err(|e| format!("connect {endpoint}: {e}"))?;

            // Drive the event loop in the background so the session stays alive.
            let _handle = event_loop.spawn();
            session.wait_for_connection().await;
            Ok::<Arc<Session>, String>(session)
        };

        match tokio::time::timeout(CONNECT_TIMEOUT, work).await {
            Ok(r) => r,
            Err(_) => Err(format!(
                "connect {endpoint}: timed out after {}s — server unreachable from the agent, \
                 or it exposes no SecurityPolicy=None + Anonymous endpoint (the only mode this \
                 plugin speaks today)",
                CONNECT_TIMEOUT.as_secs()
            )),
        }
    }

    /// Get a live session for `endpoint`, (re)connecting if we have none or it's
    /// for a different endpoint. Shared by read() and discover().
    fn session_for(
        &mut self,
        endpoint: &str,
        identity: IdentityToken,
    ) -> Result<Arc<Session>, String> {
        let need_connect = match &self.conn {
            Some(c) => c.endpoint != endpoint,
            None => true,
        };
        if need_connect {
            let session = self
                .rt
                .block_on(Self::connect(endpoint, identity))
                .inspect_err(|_| {
                    self.conn = None;
                })?;
            self.conn = Some(Conn {
                endpoint: endpoint.to_string(),
                session,
            });
        }
        Ok(self.conn.as_ref().unwrap().session.clone())
    }
}

/// Identity from `config.user` (+ `secrets.password`), else anonymous.
fn identity_from(req: &ReadRequest) -> IdentityToken {
    match req.config_str("user") {
        Some(user) => {
            let pass = req
                .secrets
                .get("password")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            IdentityToken::UserName(user.to_string(), Password(pass.to_string()))
        }
        None => IdentityToken::Anonymous,
    }
}

/// SecurityPolicy::None as the wire string the endpoint tuple expects.
#[allow(non_upper_case_globals)]
const SecurityPolicy_None: &str = "http://opcfoundation.org/UA/SecurityPolicy#None";

/// Upper bound for establishing a session before we give up and report an
/// error (instead of the poll hanging silently). Generous enough for a slow
/// PLC handshake, short enough that a misconfig surfaces within one poll.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);

impl Collector for OpcUaConnector {
    fn read(&mut self, req: &ReadRequest) -> Result<Vec<Sample>, String> {
        let endpoint = req
            .config_str("endpoint_url")
            .ok_or("config.endpoint_url is required")?
            .to_string();

        // ADR 41 F2 — resolve every demanded point up front; ONE Read service
        // call covers the whole batch (a single point is just N=1).
        let points = req.points();
        let mut to_read: Vec<ReadValueId> = Vec::with_capacity(points.len());
        let mut targets: Vec<(String, String)> = Vec::with_capacity(points.len()); // (var_id, node_str)
        for p in &points {
            let node_str = p
                .selection_str("node_id")
                .ok_or("selection.node_id is required (e.g. \"ns=2;s=Temp\")")?;
            let node_id = NodeId::from_str(node_str)
                .map_err(|_| format!("invalid node_id {node_str:?} (expected OPC-UA text form)"))?;
            to_read.push(ReadValueId::from(node_id));
            targets.push((
                p.variable_id().unwrap_or("opcua.value").to_string(),
                node_str.to_string(),
            ));
        }

        let session = self.session_for(&endpoint, identity_from(req))?;

        // One Read service call over every node: newest value (max_age 0 =
        // read from source).
        let result: Result<Vec<DataValue>, StatusCode> =
            self.rt
                .block_on(session.read(&to_read, TimestampsToReturn::Neither, 0.0));

        let values = match result {
            Ok(v) => v,
            Err(status) => {
                // Drop the session so the next poll reconnects cleanly.
                self.conn = None;
                return Err(format!("OPC-UA read failed: {status}"));
            }
        };
        if values.len() < targets.len() {
            self.conn = None;
            return Err(format!(
                "OPC-UA read returned {} of {} values",
                values.len(),
                targets.len()
            ));
        }

        // Map each DataValue back to its point. A node with no numeric value
        // becomes a bad-quality sample (quality != 0) instead of sinking the
        // whole batch — per-node OPC-UA status semantics.
        let mut samples = Vec::with_capacity(targets.len());
        for ((var_id, node_str), dv) in targets.iter().zip(values) {
            match dv.value.as_ref().and_then(variant_to_f64) {
                Some(value) => {
                    eprintln!("[opcua] {endpoint} {node_str} → {value}");
                    samples.push(Sample::new(var_id, value));
                }
                None => {
                    eprintln!(
                        "[opcua] {endpoint} {node_str} → bad (status {:?})",
                        dv.status
                    );
                    samples.push(Sample {
                        variable_id: var_id.clone(),
                        value: 0.0,
                        ts_ms: None,
                        quality: 1,
                    });
                }
            }
        }
        Ok(samples)
    }

    /// Browse one level of the address space (source explorer, lazy). Starts at
    /// `selection.node_id` if given (drill-down) else ObjectsFolder (i=85), and
    /// returns the immediate children — the portal expands deeper on demand by
    /// calling discover again with a child's node_id. Bounded by a node budget.
    fn discover(&mut self, req: &ReadRequest) -> Result<Discovery, String> {
        let endpoint = req
            .config_str("endpoint_url")
            .ok_or("config.endpoint_url is required")?
            .to_string();
        let root = match req.selection.get("node_id").and_then(|v| v.as_str()) {
            Some(s) => NodeId::from_str(s).map_err(|_| format!("invalid node_id {s:?}"))?,
            None => ObjectId::ObjectsFolder.into(),
        };
        let session = self.session_for(&endpoint, identity_from(req))?;
        let budget = Cell::new(MAX_NODES);
        let nodes = self
            .rt
            .block_on(browse_node(&session, root, MAX_DEPTH, &budget));
        Ok(Discovery {
            schema_kind: "opcua_nodes".into(),
            nodes,
        })
    }

    /// Stream value changes for `selection.node_id` via an OPC-UA subscription +
    /// monitored item. The data-change callback (on the event loop) feeds a
    /// channel; this blocks emitting each change until the process is killed.
    fn subscribe(
        &mut self,
        req: &ReadRequest,
        emit: &mut dyn FnMut(&[Sample]),
    ) -> Result<(), String> {
        let endpoint = req
            .config_str("endpoint_url")
            .ok_or("config.endpoint_url is required")?
            .to_string();
        let node_str = req
            .selection
            .get("node_id")
            .and_then(|v| v.as_str())
            .ok_or("selection.node_id is required")?;
        let node =
            NodeId::from_str(node_str).map_err(|_| format!("invalid node_id {node_str:?}"))?;
        let var_id = req.variable_id().unwrap_or("opcua.value").to_string();
        let session = self.session_for(&endpoint, identity_from(req))?;

        // The async data-change callback feeds this channel; the sync loop below
        // drains it. tokio's sender is Send+Sync (the callback needs that).
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<f64>();
        self.rt.block_on(async {
            let cb = DataChangeCallback::new(move |dv: DataValue, _item: &_| {
                if let Some(v) = dv.value.as_ref().and_then(variant_to_f64) {
                    let _ = tx.send(v);
                }
            });
            let sub_id = session
                .create_subscription(Duration::from_millis(1000), 10, 30, 0, 0, true, cb)
                .await
                .map_err(|e| format!("create_subscription: {e}"))?;
            let item: MonitoredItemCreateRequest = node.into();
            session
                .create_monitored_items(sub_id, TimestampsToReturn::Both, vec![item])
                .await
                .map_err(|e| format!("create_monitored_items: {e}"))?;
            Ok::<(), String>(())
        })?;
        eprintln!("[opcua] subscribed {endpoint} {node_str}");

        // Block emitting each pushed value. recv() ends when the process is
        // killed (agent reconfigure/shutdown) and the runtime/session tear down.
        while let Some(v) = rx.blocking_recv() {
            emit(&[Sample::new(&var_id, v)]);
        }
        Ok(())
    }
}

/// Browse one level per call (lazy drill-down from the portal); a node budget
/// caps very wide folders.
const MAX_DEPTH: u32 = 1;
const MAX_NODES: usize = 1000;

/// Recursively browse hierarchical references under `node`, building a Node
/// tree. Variables become leaves; objects/folders recurse until depth/budget
/// run out. Errors at any node degrade to an empty child list (best-effort).
fn browse_node<'a>(
    session: &'a Session,
    node: NodeId,
    depth: u32,
    budget: &'a Cell<usize>,
) -> Pin<Box<dyn Future<Output = Vec<Node>> + 'a>> {
    Box::pin(async move {
        if depth == 0 || budget.get() == 0 {
            return Vec::new();
        }
        let to_browse = [BrowseDescription {
            node_id: node,
            browse_direction: BrowseDirection::Forward,
            reference_type_id: ReferenceTypeId::HierarchicalReferences.into(),
            include_subtypes: true,
            node_class_mask: 0,
            result_mask: 0x3f,
        }];
        // max_references_per_node 0 = let the server decide; no View.
        let results = match session.browse(&to_browse, 0, None).await {
            Ok(r) => r,
            Err(_) => return Vec::new(),
        };
        let mut nodes = Vec::new();
        for res in results {
            let refs = res.references.unwrap_or_default();
            for r in refs {
                if budget.get() == 0 {
                    break;
                }
                budget.set(budget.get() - 1);
                let child_id = r.node_id.node_id.clone();
                let id_text = child_id.to_string();
                let label = r.display_name.text.to_string();
                if r.node_class == NodeClass::Variable {
                    nodes.push(Node::leaf(id_text, label, "variable"));
                } else {
                    let children = browse_node(session, child_id, depth - 1, budget).await;
                    nodes.push(Node::branch(id_text, label, "folder", children));
                }
            }
        }
        nodes
    })
}

/// Coerce an OPC-UA Variant into f64 (the warehouse stores numeric samples).
fn variant_to_f64(v: &Variant) -> Option<f64> {
    match v {
        Variant::Double(x) => Some(*x),
        Variant::Float(x) => Some(*x as f64),
        Variant::Int64(x) => Some(*x as f64),
        Variant::UInt64(x) => Some(*x as f64),
        Variant::Int32(x) => Some(*x as f64),
        Variant::UInt32(x) => Some(*x as f64),
        Variant::Int16(x) => Some(*x as f64),
        Variant::UInt16(x) => Some(*x as f64),
        Variant::SByte(x) => Some(*x as f64),
        Variant::Byte(x) => Some(*x as f64),
        Variant::Boolean(b) => Some(if *b { 1.0 } else { 0.0 }),
        _ => None,
    }
}

fn main() {
    run(OpcUaConnector::new());
}
