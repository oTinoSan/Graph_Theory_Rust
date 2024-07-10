use rmat_generator::RMATGraph;

fn main() {
    let mut g = RMATGraph::new(18, 0.1, [0.57, 0.19, 0.19, 0.05], false);
    for _ in 0..1000000 {
        g.generate_edge();
    }
    println!("{:?}", g);
}