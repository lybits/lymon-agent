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

use lymon_collector_sdk::{run, Collector, Discovery, Node, ReadRequest, Sample};
use opcua::client::{Client, ClientBuilder, IdentityToken, Password, Session};
use opcua::types::{
    BrowseDescription, BrowseDirection, DataValue, MessageSecurityMode, NodeClass, NodeId,
    ObjectId, ReadValueId, ReferenceTypeId, StatusCode, TimestampsToReturn, UserTokenPolicy,
    Variant,
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
    async fn connect(endpoint: &str, identity: IdentityToken) -> Result<Arc<Session>, String> {
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
        Ok(session)
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

impl Collector for OpcUaConnector {
    fn read(&mut self, req: &ReadRequest) -> Result<Vec<Sample>, String> {
        let endpoint = req
            .config_str("endpoint_url")
            .ok_or("config.endpoint_url is required")?
            .to_string();
        let node_str = req
            .selection
            .get("node_id")
            .and_then(|v| v.as_str())
            .ok_or("selection.node_id is required (e.g. \"ns=2;s=Temp\")")?;
        let node_id = NodeId::from_str(node_str)
            .map_err(|_| format!("invalid node_id {node_str:?} (expected OPC-UA text form)"))?;
        let var_id = req.variable_id().unwrap_or("opcua.value").to_string();

        let session = self.session_for(&endpoint, identity_from(req))?;

        // One Read service call: newest value (max_age 0 = read from source).
        let to_read = vec![ReadValueId::from(node_id)];
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
        let dv = values
            .into_iter()
            .next()
            .ok_or("OPC-UA read returned no value")?;
        let variant = dv
            .value
            .ok_or_else(|| format!("node {node_str} has no value (status {:?})", dv.status))?;
        let value = variant_to_f64(&variant)
            .ok_or_else(|| format!("node {node_str} value {variant:?} is not numeric"))?;

        eprintln!("[opcua] {endpoint} {node_str} → {value}");
        Ok(vec![Sample::new(var_id, value)])
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
