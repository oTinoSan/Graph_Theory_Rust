use std::f32::consts::PI;

use lamellar::array::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Copy, Debug, Clone, Serialize, Deserialize, lamellar::ArrayOps)]
struct Edge (u64, u64);

impl lamellar::memregion::Dist for Edge {}

impl From<(u64, u64)> for Edge {
    fn from(e: (u64, u64)) -> Self {
        Self(e.0, e.1)
    }
}

/// Active message for processing the stochastic hook step of FastSV
#[lamellar::AmData(Clone, Debug)]
struct StochasticHook {
    parents: UnsafeArray<u64>,
    new_parents: UnsafeArray<u64>,
    u: u64,
    v: u64,
    v_parent: Option<u64>,
    v_grandparent: Option<u64>,
    u_parent: Option<u64>,
}

#[lamellar::am]
impl LamellarAM for StochasticHook {
    async fn exec(&self) {
        // Which fields of the message have data in them determine where we are in the chain
        match (self.v_parent, self.v_grandparent, self.u_parent) {
            // If we have found the parent of u and v and the grandparent of v, we are at the last step:
            // comparing the grandparent of v to the new parent of u's parent
            (Some(_), Some(v_grandparent), Some(u_parent)) => {
                // compare v_grandparent to new_parents[u_parent]
                let (_, local_index) = self.parents.pe_and_offset_for_global_index(u_parent as usize).unwrap();
                let u_grandparent;
                unsafe {
                    u_grandparent = self.new_parents.local_as_slice()[local_index];
                }
                if v_grandparent < u_grandparent {
                    unsafe {
                        self.new_parents.local_as_mut_slice()[local_index] = v_grandparent;
                    }
                }
            },
            (Some(_), Some(_), _) => {
                // find u_parent, then launch am to compare u_grandparent to v_grandparent
                let (_, local_index) = self.parents.pe_and_offset_for_global_index(self.u as usize).unwrap();
                let u_parent;
                unsafe {
                    u_parent = self.parents.local_as_slice()[local_index];
                }
                let (remote_pe, _) = self.parents.pe_and_offset_for_global_index(u_parent as usize).unwrap();
                let _ = lamellar::world.exec_am_pe(remote_pe, StochasticHook {parents: self.parents.clone(), new_parents: self.new_parents.clone(), u: self.u, v: self.v, v_parent: self.v_parent, v_grandparent: self.v_grandparent, u_parent: Some(u_parent)});
            },
            (Some(v_parent), _, _) => {
                // find v_grandparent, then launch am to find u_parent
                let (_, local_index) = self.parents.pe_and_offset_for_global_index(v_parent as usize).unwrap();
                let v_grandparent;
                unsafe {
                    v_grandparent = self.parents.local_as_slice()[local_index];
                }
                let (remote_pe, _) = self.parents.pe_and_offset_for_global_index(self.u as usize).unwrap();
                let _ = lamellar::world.exec_am_pe(remote_pe, StochasticHook {parents: self.parents.clone(), new_parents: self.new_parents.clone(), u: self.u, v: self.v, v_parent: self.v_parent, v_grandparent: Some(v_grandparent), u_parent: None});
            },
            (_, _, _) => {
                // find v_parent, then launch am to find v_grandparent
                let (pe, local_index) = self.parents.pe_and_offset_for_global_index(self.v as usize).unwrap();
                // println!("PE{} accessing data from PE{} at local index {}, global index {}", lamellar::current_pe, pe, local_index, self.v);
                let v_parent;
                unsafe {
                    v_parent = self.parents.local_as_slice()[local_index];
                }
                let (remote_pe, _) = self.parents.pe_and_offset_for_global_index(v_parent as usize).unwrap();
                let _ = lamellar::world.exec_am_pe(remote_pe, StochasticHook {parents: self.parents.clone(), new_parents: self.new_parents.clone(), u: self.u, v: self.v, v_parent: Some(v_parent), v_grandparent: None, u_parent: None});
            }
        }
    }
}

/// Active message for processing the aggressive hooking step of FastSV
#[lamellar::AmData(Clone, Debug)]
struct AggressiveHook {
    parents: UnsafeArray<u64>,
    new_parents: UnsafeArray<u64>,
    u: u64,
    v: u64,
    v_parent: Option<u64>,
    v_grandparent: Option<u64>,
    local_index: usize,
}

#[lamellar::am]
impl LamellarAM for AggressiveHook {
    async fn exec(&self) {
        match (self.v_parent, self.v_grandparent) {
            (None, None) => {
                // find v_parent, then launch am to find v_grandparent
                let v_parent;
                unsafe {
                    v_parent = self.parents.local_as_slice()[self.local_index];
                }
                let (remote_pe, local_index) = self.parents.pe_and_offset_for_global_index(v_parent as usize).unwrap();
                let _ = lamellar::world.exec_am_pe(remote_pe, AggressiveHook {parents: self.parents.clone(), new_parents: self.new_parents.clone(), u: self.u, v: self.v, v_parent: Some(v_parent), v_grandparent: None, local_index});
            },
            (Some(_), None) => {
                // find v_grandparent, then launch am to compare to u_parent
                let v_grandparent;
                unsafe {
                    v_grandparent = self.parents.local_as_slice()[self.local_index];
                }
                let (remote_pe, local_index) = self.parents.pe_and_offset_for_global_index(self.u as usize).unwrap();
                let _ = lamellar::world.exec_am_pe(remote_pe, AggressiveHook {parents: self.parents.clone(), new_parents: self.new_parents.clone(), u: self.u, v: self.v, v_parent: self.v_parent, v_grandparent: Some(v_grandparent), local_index});
            },
            (Some(_), Some(v_grandparent)) => {
                // find u_parent, then compare to v_grandparent
                let u_parent;
                unsafe {
                    u_parent = self.new_parents.local_as_slice()[self.local_index];
                }
                if v_grandparent < u_parent {
                    unsafe {
                        self.new_parents.local_as_mut_slice()[self.local_index] = v_grandparent;
                    }
                }
            },
            _ => {}
        }
    }
}

#[lamellar::AmData(Clone, Debug)]
struct Shortcut {
    parents: UnsafeArray<u64>,
    new_parents: UnsafeArray<u64>,
    u: u64,
    u_parent: Option<u64>,
    u_grandparent: Option<u64>,
    local_index: usize
}

#[lamellar::am]
impl LamellarAM for Shortcut {
    async fn exec(&self) {
        match (self.u_parent, self.u_grandparent) {
            (None, None) => {
                // find u_parent, then launch am to find u_grandparent
                let u_parent;
                unsafe {
                    u_parent = self.parents.local_as_slice()[self.local_index];
                }
                let (remote_pe, local_index) = self.parents.pe_and_offset_for_global_index(u_parent as usize).unwrap();
                let _ = lamellar::world.exec_am_pe(remote_pe, Shortcut {parents: self.parents.clone(), new_parents: self.new_parents.clone(), u: self.u, u_parent: Some(u_parent), u_grandparent: None, local_index});
            },
            (Some(_), None) => {
                // find u_grandparent, then launch am to compare new u_parent to u_grandparent
                let u_grandparent;
                unsafe {
                    u_grandparent = self.parents.local_as_slice()[self.local_index];
                }
                let (remote_pe, local_index) = self.parents.pe_and_offset_for_global_index(self.u as usize).unwrap();
                let _ = lamellar::world.exec_am_pe(remote_pe, Shortcut {parents: self.parents.clone(), new_parents: self.new_parents.clone(), u: self.u, u_parent: self.u_parent, u_grandparent: Some(u_grandparent), local_index});
            },
            (Some(_), Some(u_grandparent)) => {
                // find new u_parent, compare to u_grandparent
                let new_u_parent;
                unsafe {
                    new_u_parent = self.new_parents.local_as_slice()[self.local_index];
                }
                if u_grandparent < new_u_parent {
                    unsafe {
                        self.new_parents.local_as_mut_slice()[self.local_index] = u_grandparent;
                    }
                }
            },
            _ => {}
        }
    }
}

#[lamellar::AmData(Clone, Debug)]
struct SetChanged {
    changed: UnsafeArray<bool>,
    val: bool
}

#[lamellar::am]
impl LamellarAm for SetChanged {
    async fn exec(&self) {
        unsafe {self.changed.local_as_mut_slice()[0] = self.val};
    }
}

fn parse_edge_tsv(filename: &str) -> (Vec<Edge>, usize) {
    let mut edges = vec![];
    let mut rdr = csv::ReaderBuilder::new().has_headers(false).delimiter(b'\t').from_path(filename).unwrap();
    for edge in rdr.deserialize() {
        let edge: Edge = edge.unwrap();
        edges.push(edge);
    }
    let vertex_count = edges.last().unwrap().1 as usize + 1;
    (edges, vertex_count)
}

pub fn lamellar_main() {
    // Initialize lamellar variables
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    let distribution = Distribution::Block;

    // // testing graph information
    // let vertex_count = 5;
    // let test_edges = vec![Edge(4, 0), Edge(2, 0), Edge(0, 2), Edge(1, 2), Edge(0, 1), Edge(0, 3), Edge(3, 4), Edge(1, 3), Edge(1, 4), Edge(3, 2), Edge(2, 3)];
    // let edge_count = test_edges.len();
    let vertex_count = 15;
    let test_edges: Vec<Edge> = vec![(0, 1), (0, 3), (1, 0), (1, 2), (1, 5), (1, 10), (2, 1), (2, 4), (2, 5), (3, 0), (3, 4), (4, 2), (4, 3), (4, 5), (5, 1), (5, 2), (5, 4), (5, 7), (5, 10), (6, 8), (6, 9), (7, 5), (8, 6), (8, 11), (9, 6), (9, 11), (10, 1), (10, 5), (11, 8), (11, 9), (11, 12), (12, 11), (12, 13), (13, 12)].into_iter().map(|x| Edge::from(x)).collect();
    let edge_count = test_edges.len();

    // let (test_edges, vertex_count) = parse_edge_tsv("graph.tsv");
    // let edge_count = test_edges.len();

    // initialize edge array
    let edges = UnsafeArray::<Edge>::new(&world, edge_count, distribution);

    unsafe {
        let _ = edges.dist_iter_mut().enumerate().for_each(move |(i, e)| *e = test_edges[i]);
    }

    // original graph will not be modified, can be read only
    let edges = edges.into_read_only();

    // initialize the array of new parents for each vertex to be itself
    let new_parents = UnsafeArray::<u64>::new(&world, vertex_count, distribution);
    unsafe{
        let _ = new_parents.dist_iter_mut().enumerate().for_each(|(i, x)| *x = i as u64);
    }
    world.wait_all();
    world.barrier();

    // initialize a local array of grandparents
    let mut local_grandparents = unsafe {new_parents.local_as_slice().to_owned()};
    new_parents.print();

    // initialize a distributed array used to determine if another iteration is required
    // each PE has one entry in the array, which will be set true at first
    let changed = UnsafeArray::<bool>::new(&world, num_pes, distribution);
    unsafe {changed.local_as_mut_slice()[0] = true};

    // initialize the array of parents in the last iteration
    let old_parents = UnsafeArray::<u64>::new(&world, vertex_count, distribution);

    let mut iterations = 0;

    let start = std::time::Instant::now();

    while unsafe {changed.local_as_mut_slice()[0]} {

        // set up the old_parents array for the next iteration of SV
        unsafe {
            let _ = old_parents.local_iter_mut().zip(new_parents.local_iter()).for_each(|(n, o)| *n = *o);
        }

        // wait for each pe to finish writing to old_parents
        world.wait_all();
        world.barrier();

        // stochastic hooking: for every edge (u, v)
        // if parents[parents[v]] < new_parents[parents[u]]: new_parents[parents[u]] = parents[parents[v]]

        for &Edge(u, v) in edges.local_as_slice() {
            let (remote_pe, _) = old_parents.pe_and_offset_for_global_index(v as usize).unwrap();
            // println!("Edge ({}, {}) launching message to find parent of {} on pe {}", u, v, v, remote_pe);
            let _ = world.exec_am_pe(remote_pe, StochasticHook {parents: old_parents.clone(), new_parents: new_parents.clone(), u, v, v_parent: None, v_grandparent: None, u_parent: None});
        }

        // aggressive hooking: for every edge (u, v)
        // if parents[parents[v]] < new_parents[u]: new_parents[u] = parents[parents[v]]

        for &Edge(u, v) in edges.local_as_slice() {
            let (remote_pe, local_index) = old_parents.pe_and_offset_for_global_index(v as usize).unwrap();
            let _ = world.exec_am_pe(remote_pe, AggressiveHook {parents: old_parents.clone(), new_parents: new_parents.clone(), u, v, v_parent: None, v_grandparent: None, local_index});
        }

        // shortcutting: for every vertex u
        // if parents[parents[u]] < new_parents[u]: new_parents[u] = parents[parents[u]]

        // values to iterate through on each pe vary based on whether the vertices are block or cyclic distributed
        let iter = match &distribution {
            Distribution::Block => {
                (old_parents.first_global_index_for_pe(my_pe).unwrap() ..= old_parents.last_global_index_for_pe(my_pe).unwrap()).step_by(1)
            },
            Distribution::Cyclic => {
                (old_parents.first_global_index_for_pe(my_pe).unwrap() ..= old_parents.last_global_index_for_pe(my_pe).unwrap()).step_by(num_pes)
            }
        };

        for u in iter {
            let (remote_pe, local_index) = old_parents.pe_and_offset_for_global_index(u).unwrap();
            let _ = world.exec_am_pe(remote_pe, Shortcut {parents: old_parents.clone(), new_parents: new_parents.clone(), u: u as u64, u_parent: None, u_grandparent: None, local_index});
        }

        world.wait_all();
        world.barrier();

        // check if parents[parents] == new_parents[new_parents], if so break

        // calculate new grandparents
        let mut new_grandparents = vec![];
        for parent in unsafe {new_parents.local_as_slice()} {
            let f = unsafe {new_parents.at(*parent as usize)};
            new_grandparents.push(new_parents.block_on(f));
        }

        // compare them to the old grandparents
        let eq = new_grandparents.iter().zip(&local_grandparents).map(|(n, o)| *n == *o).fold(true, |acc, x| acc && x);
        // set the local changed value
        unsafe {changed.local_as_mut_slice()[0] = !eq};
        world.barrier();

        // pe 0 folds all the changed values together, then uses an AM to set them to that folded value
        // this could probably safely be done on each PE
        if my_pe == 0 {
            let any_changed = unsafe {changed.onesided_iter().into_iter().fold(false, |acc, x| acc || *x)};
            let _ = world.exec_am_all(SetChanged {changed: changed.clone(), val: any_changed});
            world.wait_all();
        }
        local_grandparents = new_grandparents;
        world.barrier();
        iterations += 1;
        if my_pe == 0 {
            println!("Iteration {} complete after {:?}", iterations, start.elapsed());
        }
    }

    new_parents.print();
}
