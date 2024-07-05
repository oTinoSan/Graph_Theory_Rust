use lamellar::{array::prelude::*, RemoteMemoryRegion};
use serde::{Deserialize, Serialize};

#[derive(Copy, Debug, Clone, Serialize, Deserialize, lamellar::ArrayOps)]
struct Edge (u64, u64);

impl lamellar::memregion::Dist for Edge {}

#[lamellar::AmData(Clone, Debug)]
struct StochasticHook {
    parents: UnsafeArray<u64>,
    new_parents: UnsafeArray<u64>,
    vertex_count: usize,
    u: u64,
    v: u64,
    v_parent: Option<u64>,
    v_grandparent: Option<u64>,
    u_parent: Option<u64>,
}

#[lamellar::am]
impl LamellarAM for StochasticHook {
    async fn exec(&self) {
        match (self.v_parent, self.v_grandparent, self.u_parent) {
            (Some(_), Some(v_grandparent), Some(u_parent)) => {
                // compare v_grandparent to new_parents[u_parent]
                let (_, local_index) = self.parents.pe_and_offset_for_global_index(u_parent as usize).unwrap();
                let u_grandparent;
                unsafe {
                    u_grandparent = self.parents.local_as_slice()[local_index];
                }
                if v_grandparent < u_grandparent {
                    unsafe {
                        self.new_parents.local_as_mut_slice()[local_index] = v_grandparent;
                    }
                }
            },
            (Some(_), Some(_), _) => {
                // find u_parent
                let (_, local_index) = self.parents.pe_and_offset_for_global_index(self.u as usize).unwrap();
                let u_parent;
                unsafe {
                    u_parent = self.parents.local_as_slice()[local_index];
                }
                let (remote_pe, _) = self.parents.pe_and_offset_for_global_index(u_parent as usize).unwrap();
                let _ = lamellar::world.exec_am_pe(remote_pe, StochasticHook {parents: self.parents.clone(), new_parents: self.new_parents.clone(), u: self.u, v: self.v, vertex_count: self.vertex_count, v_parent: self.v_parent, v_grandparent: self.v_grandparent, u_parent: Some(u_parent)});
            },
            (Some(v_parent), _, _) => {
                // find v_grandparent
                let (_, local_index) = self.parents.pe_and_offset_for_global_index(v_parent as usize).unwrap();
                let v_grandparent;
                unsafe {
                    v_grandparent = self.parents.local_as_slice()[local_index];
                }
                let (remote_pe, _) = self.parents.pe_and_offset_for_global_index(self.u as usize).unwrap();
                let _ = lamellar::world.exec_am_pe(remote_pe, StochasticHook {parents: self.parents.clone(), new_parents: self.new_parents.clone(), u: self.u, v: self.v, vertex_count: self.vertex_count, v_parent: self.v_parent, v_grandparent: Some(v_grandparent), u_parent: None});
            },
            (_, _, _) => {
                // find v_parent
                let (pe, local_index) = self.parents.pe_and_offset_for_global_index(self.v as usize).unwrap();
                println!("PE{} accessing data from PE{} at local index {}, global index {}", lamellar::current_pe, pe, local_index, self.v);
                let v_parent;
                unsafe {
                    v_parent = self.parents.local_as_slice()[local_index];
                }
                let (remote_pe, _) = self.parents.pe_and_offset_for_global_index(v_parent as usize).unwrap();
                let _ = lamellar::world.exec_am_pe(remote_pe, StochasticHook {parents: self.parents.clone(), new_parents: self.new_parents.clone(), u: self.u, v: self.v, vertex_count: self.vertex_count, v_parent: Some(v_parent), v_grandparent: None, u_parent: None});
            }
        }
    }
}

pub fn lamellar_main() {
    // Initialize lamellar variables
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();

    // testing graph information
    let vertex_count = 5;
    let test_edges = vec![Edge(4, 0), Edge(2, 0), Edge(0, 2), Edge(1, 2), Edge(0, 1), Edge(0, 3), Edge(3, 4), Edge(1, 3), Edge(1, 4), Edge(3, 2), Edge(2, 3)];
    let edge_count = test_edges.len();

    // initialize edge array
    let edges = UnsafeArray::<Edge>::new(&world, edge_count, Distribution::Block);

    unsafe {
        let _ = edges.dist_iter_mut().enumerate().for_each(move |(i, e)| *e = test_edges[i]);
    }

    // original graph will not be modified, can be read only
    let edges = edges.into_read_only();

    // initialize the array of new parents for each vertex to be itself
    let new_parents = UnsafeArray::<u64>::new(&world, vertex_count, Distribution::Block);
    unsafe{
        let _ = new_parents.dist_iter_mut().enumerate().for_each(|(i, x)| *x = i as u64);
    }

    // initialize the array of parents in the last iteration
    let old_parents = UnsafeArray::<u64>::new(&world, vertex_count, Distribution::Block);

    // set up the old_parents array for the next iteration of SV
    unsafe {
        let _ = old_parents.local_iter_mut().zip(new_parents.local_iter()).for_each(|(n, o)| *n = *o);
    }

    // wait for each pe to finish writing to old_parents
    world.wait_all();
    world.barrier();

    new_parents.print();

    // stochastic hooking: for every edge (u, v)
    // if parents[parents[v]] < new_parents[parents[u]]: new_parents[parents[u]] = parents[parents[v]]
    // let old_parents_clone = old_parents.clone();
    // let new_parents_clone = new_parents.clone();
    // let world_clone = world.clone();
    // let _ = edges.dist_iter().for_each(move|e| {
    //     let (remote_pe, _) = old_parents_clone.pe_and_offset_for_global_index(e.0 as usize).unwrap();
    //     let _ = world_clone.exec_am_pe(remote_pe, StochasticHook {parents: old_parents_clone.clone(), new_parents: new_parents_clone.clone(), vertex_count, u: e.0, v: e.1, v_parent: None, v_grandparent: None, u_parent: None});
    // });

    for &Edge(u, v) in edges.local_as_slice() {
        let (remote_pe, local_index) = old_parents.pe_and_offset_for_global_index(v as usize).unwrap();
        println!("Edge ({}, {}) launching message to find parent of {} on pe {}", u, v, v, remote_pe);
        let _ = world.exec_am_pe(remote_pe, StochasticHook {parents: old_parents.clone(), new_parents: new_parents.clone(), vertex_count, u, v, v_parent: None, v_grandparent: None, u_parent: None});
    }

    world.wait_all();
    world.barrier();

    // aggressive hooking: for every edge (u, v)
    // if parents[parents[v]] < new_parents[u]: new_parents[u] = parents[parents[v]]

    // shortcutting: for every vertex u
    // if parents[parents[u]] < new_parents[u]: new_parents[u] = parents[parents[u]]

    // check if parents[parents] == new_parents[new_parents], if so break
    new_parents.print();

    old_parents.print();
}