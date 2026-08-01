//! Server introspection: slowlog, clients, cluster topology, pub/sub.
//!
//! Every one of these is backed by a command some managed host blocks, so each accessor
//! checks its [`Feature`] first and reports [`Unavailable`](crate::Availability::Denied)
//! rather than surfacing a raw error. On Upstash and ElastiCache that is the *normal*
//! path, not an edge case.

use crate::capability::Feature;
use crate::conn::Conn;
use crate::error::Result;
use crate::value::display_string;
use fred::prelude::Value;

/// One `SLOWLOG GET` entry.
#[derive(Debug, Clone, PartialEq)]
pub struct SlowEntry {
    pub id: i64,
    /// Unix seconds when the command completed.
    pub timestamp: i64,
    pub duration_us: u64,
    pub command: String,
    pub client_addr: String,
    pub client_name: String,
}

/// One row of `CLIENT LIST`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ClientInfo {
    pub id: String,
    pub addr: String,
    pub name: String,
    /// Seconds since the connection opened.
    pub age: u64,
    /// Seconds the connection has been idle.
    pub idle: u64,
    pub db: String,
    pub cmd: String,
    pub sub: u64,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ClusterNode {
    pub id: String,
    pub addr: String,
    pub flags: String,
    pub master: bool,
    pub link_state: String,
    /// Slot ranges as written by Redis, e.g. `0-5460`.
    pub slots: Vec<String>,
    pub myself: bool,
}

#[derive(Debug, Clone, Default)]
pub struct ClusterTopology {
    pub enabled: bool,
    pub state: String,
    pub slots_assigned: u64,
    pub known_nodes: u64,
    pub size: u64,
    pub nodes: Vec<ClusterNode>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PubSubChannel {
    pub name: String,
    pub subscribers: u64,
}

impl Conn {
    /// Slowest recent commands. Enormously useful and unsurfaced in every other TUI.
    pub async fn slowlog(&self, count: u32) -> Result<Vec<SlowEntry>> {
        if !self.capabilities().has(Feature::Slowlog) {
            return Ok(Vec::new());
        }
        let reply = self
            .cmd("SLOWLOG", vec!["GET".into(), count.into()])
            .await?;
        Ok(parse_slowlog(&reply))
    }

    pub async fn client_list(&self) -> Result<Vec<ClientInfo>> {
        if !self.capabilities().has(Feature::ClientList) {
            return Ok(Vec::new());
        }
        let reply = self.cmd("CLIENT", vec!["LIST".into()]).await?;
        Ok(parse_client_list(&display_string(&reply)))
    }

    pub async fn cluster_topology(&self) -> Result<ClusterTopology> {
        if !self.capabilities().has(Feature::Cluster) {
            return Ok(ClusterTopology::default());
        }

        let info = self.cmd("CLUSTER", vec!["INFO".into()]).await?;
        let mut topology = parse_cluster_info(&display_string(&info));

        // A standalone server answers CLUSTER INFO with cluster_enabled:0; asking it for
        // NODES is pointless and, on some forks, an error.
        if topology.enabled
            && let Ok(nodes) = self.cmd("CLUSTER", vec!["NODES".into()]).await
        {
            topology.nodes = parse_cluster_nodes(&display_string(&nodes));
        }

        Ok(topology)
    }

    /// Active pub/sub channels with subscriber counts.
    pub async fn pubsub_channels(&self, limit: usize) -> Result<Vec<PubSubChannel>> {
        if !self.capabilities().has(Feature::PubSub) {
            return Ok(Vec::new());
        }

        let reply = self.cmd("PUBSUB", vec!["CHANNELS".into()]).await?;
        let Value::Array(items) = reply else {
            return Ok(Vec::new());
        };

        let names: Vec<String> = items.iter().map(display_string).take(limit).collect();
        if names.is_empty() {
            return Ok(Vec::new());
        }

        let mut args: Vec<Value> = vec!["NUMSUB".into()];
        args.extend(names.iter().map(|n| Value::from(n.as_str())));
        let counts = self.cmd("PUBSUB", args).await?;

        Ok(parse_numsub(&counts))
    }
}

fn parse_slowlog(reply: &Value) -> Vec<SlowEntry> {
    let Value::Array(entries) = reply else {
        return Vec::new();
    };
    entries
        .iter()
        .filter_map(|e| {
            let Value::Array(f) = e else { return None };
            if f.len() < 4 {
                return None;
            }
            let command = match &f[3] {
                Value::Array(parts) => parts
                    .iter()
                    .map(display_string)
                    .collect::<Vec<_>>()
                    .join(" "),
                other => display_string(other),
            };
            Some(SlowEntry {
                id: f[0].as_i64().unwrap_or(0),
                timestamp: f[1].as_i64().unwrap_or(0),
                duration_us: f[2].as_u64().unwrap_or(0),
                command,
                // Redis 4+ appends client addr and name; older servers stop at the command.
                client_addr: f.get(4).map(display_string).unwrap_or_default(),
                client_name: f.get(5).map(display_string).unwrap_or_default(),
            })
        })
        .collect()
}

/// `CLIENT LIST` is newline-separated rows of `key=value` pairs.
///
/// Values can be empty (`name=`), and the set of fields varies by server version and
/// vendor, so this reads by name and tolerates anything missing.
fn parse_client_list(raw: &str) -> Vec<ClientInfo> {
    raw.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|line| {
            let mut client = ClientInfo::default();
            for pair in line.split_ascii_whitespace() {
                let Some((k, v)) = pair.split_once('=') else {
                    continue;
                };
                match k {
                    "id" => client.id = v.to_string(),
                    "addr" => client.addr = v.to_string(),
                    "name" => client.name = v.to_string(),
                    "age" => client.age = v.parse().unwrap_or(0),
                    "idle" => client.idle = v.parse().unwrap_or(0),
                    "db" => client.db = v.to_string(),
                    "cmd" => client.cmd = v.to_string(),
                    "sub" => client.sub = v.parse().unwrap_or(0),
                    _ => {}
                }
            }
            client
        })
        .collect()
}

fn parse_cluster_info(raw: &str) -> ClusterTopology {
    let mut t = ClusterTopology::default();
    for line in raw.lines() {
        let Some((k, v)) = line.trim().split_once(':') else {
            continue;
        };
        match k {
            "cluster_enabled" => t.enabled = v.trim() == "1",
            "cluster_state" => t.state = v.trim().to_string(),
            "cluster_slots_assigned" => t.slots_assigned = v.trim().parse().unwrap_or(0),
            "cluster_known_nodes" => t.known_nodes = v.trim().parse().unwrap_or(0),
            "cluster_size" => t.size = v.trim().parse().unwrap_or(0),
            _ => {}
        }
    }
    t
}

/// `CLUSTER NODES` rows:
/// `<id> <ip:port@cport[,hostname]> <flags> <master> <ping> <pong> <epoch> <link> <slots...>`
fn parse_cluster_nodes(raw: &str) -> Vec<ClusterNode> {
    raw.lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|line| {
            let f: Vec<&str> = line.split_ascii_whitespace().collect();
            if f.len() < 8 {
                return None;
            }
            let flags = f[2].to_string();
            Some(ClusterNode {
                id: f[0].to_string(),
                // Strip the cluster bus port and any hostname suffix.
                addr: f[1].split('@').next().unwrap_or(f[1]).to_string(),
                master: flags.contains("master"),
                myself: flags.contains("myself"),
                flags,
                link_state: f[7].to_string(),
                // Slots begin at field 8; `[...]` entries are in-flight migrations.
                slots: f[8..]
                    .iter()
                    .filter(|s| !s.starts_with('['))
                    .map(|s| s.to_string())
                    .collect(),
            })
        })
        .collect()
}

fn parse_numsub(reply: &Value) -> Vec<PubSubChannel> {
    let Value::Array(items) = reply else {
        return Vec::new();
    };
    items
        .chunks_exact(2)
        .map(|c| PubSubChannel {
            name: display_string(&c[0]),
            subscribers: c[1].as_u64().unwrap_or(0),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_client_list_rows() {
        let raw = "id=7 addr=127.0.0.1:54321 laddr=127.0.0.1:6379 fd=8 name= age=120 idle=3 flags=N db=0 sub=0 psub=0 cmd=client|list resp=2\n\
                   id=9 addr=10.0.0.4:5000 name=worker-1 age=5 idle=0 db=2 sub=3 cmd=xread resp=3\n";
        let clients = parse_client_list(raw);
        assert_eq!(clients.len(), 2);

        assert_eq!(clients[0].id, "7");
        assert_eq!(clients[0].addr, "127.0.0.1:54321");
        assert_eq!(
            clients[0].name, "",
            "an empty name= must parse, not skip the row"
        );
        assert_eq!(clients[0].age, 120);
        assert_eq!(clients[0].cmd, "client|list");

        assert_eq!(clients[1].name, "worker-1");
        assert_eq!(clients[1].db, "2");
        assert_eq!(clients[1].sub, 3);
    }

    #[test]
    fn client_list_tolerates_missing_fields() {
        // Vendors differ on which fields they emit; a missing one must not lose the row.
        let clients = parse_client_list("id=1 addr=x:1\n");
        assert_eq!(clients.len(), 1);
        assert_eq!(clients[0].age, 0);
        assert_eq!(clients[0].cmd, "");
    }

    #[test]
    fn parses_slowlog_entries() {
        let reply = Value::Array(vec![Value::Array(vec![
            Value::from(14i64),
            Value::from(1_785_513_113i64),
            Value::from(15_000i64),
            // Deliberately not the obvious `KEYS *` sample: the workspace guard test bans
            // that literal anywhere in source, including test data, and it should stay
            // that strict.
            Value::Array(vec![Value::from("SORT"), Value::from("bigset")]),
            Value::from("10.0.0.9:5522"),
            Value::from("analytics"),
        ])]);

        let entries = parse_slowlog(&reply);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, 14);
        assert_eq!(entries[0].duration_us, 15_000);
        assert_eq!(entries[0].command, "SORT bigset");
        assert_eq!(entries[0].client_name, "analytics");
    }

    #[test]
    fn slowlog_entries_from_older_servers_lack_client_fields() {
        // Redis 3 stopped at the command; dropping those rows would blank the pane.
        let reply = Value::Array(vec![Value::Array(vec![
            Value::from(1i64),
            Value::from(100i64),
            Value::from(9i64),
            Value::Array(vec![Value::from("GET"), Value::from("k")]),
        ])]);
        let entries = parse_slowlog(&reply);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].client_addr, "");
    }

    #[test]
    fn standalone_reports_cluster_disabled() {
        let t = parse_cluster_info("cluster_enabled:0\r\ncluster_state:ok\r\n");
        assert!(!t.enabled);
    }

    #[test]
    fn parses_cluster_info_counters() {
        let raw = "cluster_enabled:1\r\ncluster_state:ok\r\ncluster_slots_assigned:16384\r\n\
                   cluster_known_nodes:6\r\ncluster_size:3\r\n";
        let t = parse_cluster_info(raw);
        assert!(t.enabled);
        assert_eq!(t.slots_assigned, 16384);
        assert_eq!(t.known_nodes, 6);
        assert_eq!(t.size, 3);
    }

    #[test]
    fn parses_cluster_nodes() {
        let raw = "\
07c37df 127.0.0.1:30004@31004 slave e7d1eec 0 1426238317239 4 connected
e7d1eec 127.0.0.1:30001@31001,node1.local myself,master - 0 0 1 connected 0-5460
6ec2392 127.0.0.1:30002@31002 master - 0 1426238316232 2 connected 5461-10922 [10923-<-07c37df]
";
        let nodes = parse_cluster_nodes(raw);
        assert_eq!(nodes.len(), 3);

        // The cluster bus port and hostname suffix are noise in a topology view.
        assert_eq!(nodes[1].addr, "127.0.0.1:30001");
        assert!(nodes[1].myself);
        assert!(nodes[1].master);
        assert_eq!(nodes[1].slots, vec!["0-5460"]);

        assert!(
            !nodes[0].master,
            "a slave row must not be counted as a master"
        );

        // Migration markers are not slot ranges.
        assert_eq!(nodes[2].slots, vec!["5461-10922"]);
    }

    #[test]
    fn parses_numsub_pairs() {
        let reply = Value::Array(vec![
            Value::from("bull:emails:events"),
            Value::from(2i64),
            Value::from("other"),
            Value::from(0i64),
        ]);
        let channels = parse_numsub(&reply);
        assert_eq!(channels.len(), 2);
        assert_eq!(channels[0].name, "bull:emails:events");
        assert_eq!(channels[0].subscribers, 2);
        assert_eq!(channels[1].subscribers, 0);
    }

    #[test]
    fn malformed_replies_yield_nothing_rather_than_panicking() {
        assert!(parse_slowlog(&Value::from("nope")).is_empty());
        assert!(parse_numsub(&Value::from("nope")).is_empty());
        assert!(parse_cluster_nodes("garbage line\n").is_empty());
        assert!(parse_client_list("").is_empty());
    }
}
