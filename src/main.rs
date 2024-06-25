use array2d::Array2D;
use graph_theory::*;
use graph_theory::shiloach_vishkin::shiloach_vishkin;

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

    // Shiloach Vishkin Ex. 1

    let ex_1 = CompressedSparseRows {
        rof_offsets: vec![0, 2, 7, 8, 9, 10, 11, 12, 12, 14, 15, 16, 17, 18, 19, 20],
        col_indices: vec![3, 5, 4, 10, 12, 13, 14, 6, 0, 1, 0, 2, 9, 11, 8, 1, 8, 1, 1, 1],
    };

    println!("Shiloach Vishkin Example 1: {:?}", shiloach_vishkin(ex_1));

    // Shiloach Vishkin Ex. 2

    let ex_2 = CompressedSparseRows {
        rof_offsets: vec![0, 1, 3, 4, 6, 7, 8, 9, 9, 10, 12, 15, 16, 17, 19, 20],
        col_indices: vec![3, 4, 10, 6, 0, 5, 1, 3, 2, 9, 8, 11, 1, 12, 13, 9, 10, 10, 14, 13],
    };

    println!("Shiloach Vishkin Example 2: {:?}", shiloach_vishkin(ex_2));

    // Shiloach Vishkin Ex. 3

    let ex_3 = CompressedSparseRows {
        rof_offsets: vec![0, 5, 10, 15, 20, 25, 30],
        col_indices: vec![1, 2, 3, 4, 5, 0, 2, 3, 4, 5, 0, 1, 3, 4, 5, 0, 1, 2, 4, 5, 0, 1, 2, 3, 5, 0, 1, 2, 3, 4],
    };

    println!("Shiloach Vishkin Example 3: {:?}", shiloach_vishkin(ex_3));

    // Shiloach Vishkin Ex. 4

    let ex_4 = CompressedSparseRows {
        rof_offsets: vec![0, 2, 6, 9, 11, 14, 19, 21, 22, 24, 26, 28, 31, 33, 34, 34],
        col_indices: vec![1, 3, 0, 2, 5, 10, 1, 4, 5, 0, 4, 2, 3, 5, 1, 2, 4, 7, 10, 8, 9, 5, 6, 11, 6, 11, 1, 5, 8, 9, 12, 11, 13, 12]
    };

    println!("Shiloach Vishkin Example 4: {:?}", shiloach_vishkin(ex_4));
}
