use std::default;
use std::hash::{BuildHasherDefault, DefaultHasher, Hash};
use std::{collections::HashMap, hash::Hasher};
use std::sync::Arc;

use lamellar::active_messaging::AmDist;
use lamellar::{active_messaging::prelude::*, darc::prelude::*, Deserialize, Serialize};

#[derive(Copy, Debug, Clone, Serialize, Deserialize, lamellar::ArrayOps, Default, PartialEq, Hash)]
pub struct Edge(pub u64, pub u64);

impl lamellar::memregion::Dist for Edge {}

impl From<(u64, u64)> for Edge {
    fn from(e: (u64, u64)) -> Self {
        Self(e.0, e.1)
    }
}

#[derive(Copy, Debug, Clone, Serialize, Deserialize, lamellar::ArrayOps, Default, PartialEq)]
pub struct WeightedEdge(pub u64, pub u64, pub f64);

impl lamellar::memregion::Dist for WeightedEdge {}

#[derive(Copy, Debug, Clone, Serialize, Deserialize, lamellar::ArrayOps, PartialEq)]
pub enum EdgeType {
    Unweighted(Edge),
    Weighted(WeightedEdge)
}

impl lamellar::memregion::Dist for EdgeType {}

#[derive(Copy, Debug, Clone, Serialize, Deserialize, lamellar::ArrayOps, Default)]
pub struct Vertex {
    pub parent: u64,
    pub rank: usize,
}

impl PartialEq for Vertex {
    fn eq(&self, other: &Self) -> bool {
        self.parent == other.parent
    }
}

impl lamellar::memregion::Dist for Vertex {}