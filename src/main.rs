use array2d::Array2D;
use graph_theory::*;

fn main() {
    // Define the adjacency matrix for the given graph
    let rows = vec![
        vec![0, 1, 1, 1, 0],
        vec![0, 0, 1, 1, 1],
        vec![1, 0, 0, 1, 0],
        vec![0, 0, 1, 0, 1],
        vec![1, 0, 0, 0, 0],
    ];

    let adj = Array2D::from_rows(&rows).unwrap();

    println!("Starting adjacency matrix: {:?}", adj.as_rows());

    let csr = CompressedSparseRows::from_adjacency_matrix(adj);

    println!("Converted to CSR: {:?}", csr);

    let adj = csr.to_adjacency_matrix();

    println!("Back to adjacency matrix: {:?}", adj.as_rows());

    let edges = edge_list_from_file("edges.txt");

    println!("Edges: {:?}", edges);

    let csr = CompressedSparseRows::from_edge_list(edges);

    println!("Converted to CSR: {:?}", csr);
}
