use lamellar::array::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Copy, Debug, Clone, Serialize, Deserialize, lamellar::ArrayOps)]
struct Edge (u64, u64);

impl lamellar::memregion::Dist for Edge {}

pub fn lamellar_main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    let node_count = 5;
    let test_edges = vec![Edge(4, 0), Edge(2, 0), Edge(0, 2), Edge(1, 2), Edge(0, 1), Edge(0, 3), Edge(3, 4), Edge(1, 3), Edge(1, 4), Edge(3, 2), Edge(2, 3)];
    let edge_count = test_edges.len();

    let edges = UnsafeArray::<Edge>::new(&world, edge_count, Distribution::Block);

    unsafe {
        let _ = edges.dist_iter_mut().enumerate().for_each(move |(i, e)| *e = test_edges[i]);
    }

    let old_parents = UnsafeArray::<u64>::new(&world, node_count, Distribution::Block);
    unsafe{
        let _ = old_parents.dist_iter_mut().enumerate().for_each(|(i, x)| *x = i as u64);
    }

    let new_parents = UnsafeArray::<u64>::new(&world, node_count, Distribution::Block);

    unsafe {
        let _ = new_parents.local_iter_mut().zip(old_parents.local_iter()).for_each(|(n, o)| *n = *o);
    }

    world.wait_all();
    world.barrier();

    new_parents.print();

    old_parents.print();

    edges.print();
}