use lamellar::{LamellarEnv, LamellarTeam, LamellarWorld, array::prelude::*};
use rmat_generator::{CloneSeedableRng, RMATGraph, RMATIter};
use dist_structs::Edge;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct DistRMAT<T> {
    generator: RMATGraph<T>,
    team: Arc<LamellarTeam>,
    global_edge_count: usize,
}

impl<T> DistRMAT<T>
where
    T: CloneSeedableRng,
{
    pub fn new(
        world: &LamellarWorld,
        order: usize,
        fuzz: f64,
        seed: Option<u64>,
        edge_count: usize,
        partition: [f64; 4],
        directed: bool,
    ) -> Self {
        let team = world.team();
        Self {
            generator: RMATGraph::new(
                order,
                fuzz,
                if let Some(seed) = seed {
                    Some(seed * team.num_pes() as u64 + team.my_pe() as u64)
                } else {
                    None
                },
                edge_count / team.num_pes() + {
                    if team.my_pe() < edge_count % team.num_pes() {
                        1
                    } else {
                        0
                    }
                },
                partition,
                directed,
            ),
            team,
            global_edge_count: edge_count,
        }
    }

    pub fn iter(&self) -> RMATIter<T> {
        self.generator.iter()
    }
}

impl <T: CloneSeedableRng> IntoIterator for DistRMAT<T> {
    type Item = Edge;
    type IntoIter = RMATIter<T>;
    fn into_iter(self) -> Self::IntoIter {
        self.generator.into_iter()
    }
}