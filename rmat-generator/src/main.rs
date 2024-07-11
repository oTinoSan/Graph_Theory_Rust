use rmat_generator::RMATGraph;

fn main() {
    let g = RMATGraph::new(18, 0.1, None, 100000, [0.57, 0.19, 0.19, 0.05], false);
    let edges: Vec<_> = g.clone().into_iter().collect();
    let edges_2: Vec<_> = g.clone().into_iter().collect();
    assert_eq!(edges, edges_2);
    println!("{:?}", edges);
}