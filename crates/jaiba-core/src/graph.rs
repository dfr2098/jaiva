use std::collections::{HashMap, HashSet, VecDeque};

use thiserror::Error;

use crate::config::FlowConfig;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum GraphError {
    #[error("the flow must contain at least one processor")]
    Empty,
    #[error("duplicate processor id '{0}'")]
    DuplicateProcessor(String),
    #[error("connection '{from} -> {to}' references an unknown processor")]
    UnknownProcessor { from: String, to: String },
    #[error("the flow graph contains a cycle")]
    Cycle,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphNode {
    pub id: String,
    pub processor_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphEdge {
    pub from: String,
    pub relationship: String,
    pub to: String,
}

/// Validated directed acyclic graph built from a deserialized flow manifest.
///
/// The runtime consumes this structure; it never interprets YAML text.
#[derive(Debug, Clone)]
pub struct FlowGraph {
    pub nodes: Vec<GraphNode>,
    pub edges: Vec<GraphEdge>,
    topological_order: Vec<String>,
}

impl FlowGraph {
    pub fn build(config: &FlowConfig) -> Result<Self, GraphError> {
        if config.processors.is_empty() {
            return Err(GraphError::Empty);
        }
        let mut ids = HashSet::new();
        let nodes = config
            .processors
            .iter()
            .map(|processor| {
                if !ids.insert(processor.id.clone()) {
                    return Err(GraphError::DuplicateProcessor(processor.id.clone()));
                }
                Ok(GraphNode {
                    id: processor.id.clone(),
                    processor_type: processor.processor_type.clone(),
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let mut indegree: HashMap<String, usize> = ids.iter().map(|id| (id.clone(), 0)).collect();
        let mut outgoing: HashMap<String, Vec<String>> = HashMap::new();
        let edges = config
            .connections
            .iter()
            .map(|connection| {
                if !ids.contains(&connection.from) || !ids.contains(&connection.to) {
                    return Err(GraphError::UnknownProcessor {
                        from: connection.from.clone(),
                        to: connection.to.clone(),
                    });
                }
                *indegree
                    .get_mut(&connection.to)
                    .expect("validated destination") += 1;
                outgoing
                    .entry(connection.from.clone())
                    .or_default()
                    .push(connection.to.clone());
                Ok(GraphEdge {
                    from: connection.from.clone(),
                    relationship: connection.relationship.clone(),
                    to: connection.to.clone(),
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let mut pending: VecDeque<String> = indegree
            .iter()
            .filter(|(_, degree)| **degree == 0)
            .map(|(id, _)| id.clone())
            .collect();
        let mut topological_order = Vec::with_capacity(nodes.len());
        while let Some(id) = pending.pop_front() {
            topological_order.push(id.clone());
            for destination in outgoing.get(&id).into_iter().flatten() {
                let degree = indegree
                    .get_mut(destination)
                    .expect("validated destination");
                *degree -= 1;
                if *degree == 0 {
                    pending.push_back(destination.clone());
                }
            }
        }
        if topological_order.len() != nodes.len() {
            return Err(GraphError::Cycle);
        }

        Ok(Self {
            nodes,
            edges,
            topological_order,
        })
    }

    pub fn topological_order(&self) -> &[String] {
        &self.topological_order
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_a_topological_order() {
        let config: FlowConfig = serde_yaml::from_str(
            r#"
id: graph
processors:
  - id: source
    type: generate_records
  - id: sink
    type: log_records
connections:
  - from: source
    relationship: success
    to: sink
"#,
        )
        .unwrap();
        let graph = FlowGraph::build(&config).unwrap();
        assert_eq!(graph.topological_order(), ["source", "sink"]);
    }

    #[test]
    fn rejects_cycles_even_if_an_unrelated_start_exists() {
        let config: FlowConfig = serde_yaml::from_str(
            r#"
id: cycle
processors:
  - id: start
    type: generate_records
  - id: left
    type: log_records
  - id: right
    type: log_records
connections:
  - from: left
    relationship: success
    to: right
  - from: right
    relationship: success
    to: left
"#,
        )
        .unwrap();
        assert_eq!(FlowGraph::build(&config).unwrap_err(), GraphError::Cycle);
    }
}
