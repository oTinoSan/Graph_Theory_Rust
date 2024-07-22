use std::default;
use std::hash::{BuildHasherDefault, DefaultHasher, Hash};
use std::{collections::HashMap, hash::Hasher};
use std::sync::Arc;

use lamellar::active_messaging::AmDist;
use lamellar::{active_messaging::prelude::*, darc::prelude::*, Deserialize, Serialize};

#[derive(Copy, Debug, Clone, Serialize, Deserialize, lamellar::ArrayOps, Default, PartialEq)]
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

pub struct DistHashMap<K, V>
where
    K: 'static,
    V: 'static,
{
    num_pes: usize,
    team: Arc<LamellarTeam>,
    data: LocalRwDarc<HashMap<K, V>>,
}

impl<K, V> DistHashMap<K, V>
where
    K: Hash
{
    pub fn new(world: &LamellarWorld, num_pes: usize) -> Self {
        let team = world.team();
        DistHashMap {
            num_pes,
            team: team.clone(),
            data: LocalRwDarc::new(team, HashMap::new()).unwrap(),
        }
    }

    fn get_key_pe(&self, k: K) -> usize {
        let mut state = DefaultHasher::new();
        k.hash(&mut state);
        state.finish() as usize % self.num_pes
    }
}