use dist_structs::Edge;
use lamellar_disjoint_set::DisjointSet;
use lamellar::active_messaging::prelude::*;

fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();

    let disjoint_set = DisjointSet::new_with_vertices(&world, 14, 14);

    let test_edges: Vec<Edge> = vec![(0, 1), (0, 3), (1, 2), (1, 5), (1, 10), (2, 4), (2, 5), (3, 4), (4, 5), (5, 7), (5, 10), (6, 8), (6, 9), (8, 11), (9, 11), (11, 12), (12, 13)].into_iter().map(|x| Edge::from(x)).collect();

    if my_pe == 0 {
        for edge in test_edges {
            let _ = disjoint_set.add_edge(edge);
        }
    }
    world.wait_all();
    world.barrier();

    disjoint_set.process_edges();

    disjoint_set.print_vertices();
}