// use array2d::Array2D;
use std::vec::Vec;
// use graph_theo::compressed_sparse_rows::CompressedSparseRows;
use graph_exploration::{
    lamellar_exercise::active_messaging_example, 
    lamellar_exercise::hello_world_am_example,
    shiloach_vishkin};

fn main() {
    // Shiloach Viskin Example
    // // Example 4
    // let rows: usize = 15;
    // let row_offsets: Vec<usize> = vec![0, 2, 6, 9, 11, 14, 19, 21, 22, 24, 26, 28, 31, 33, 34, 34];
    // let col_indices: Vec<usize> = vec![1, 3, 0, 2, 5, 10, 1, 4, 5, 0, 4, 2, 3, 5, 1, 2, 4, 7, 10, 8, 9, 5, 6, 11, 6, 11, 1, 5, 8, 9, 12, 11, 13, 12];

    // let connected_components = shiloach_vishkin::run_connected_components(rows, row_offsets, col_indices);

    // println!("Connected Component ID's: {:?} \n\n", connected_components);


    // // Compressed Sparse Rows Example
    // // Define the adjacency matrix for the given graph
    // let rows: Vec<Vec<u64>> = vec![
    //     vec![0, 1, 1, 1, 0],
    //     vec![0, 0, 1, 1, 1],
    //     vec![1, 0, 0, 1, 0],
    //     vec![0, 0, 1, 0, 1],
    //     vec![1, 0, 0, 0, 0],
    // ];

    // let converted_array = Array2D::from_rows(&rows).expect("Failed?");

    // // Convert the adjacency matrix to CSR format
    // let compression = CompressedSparseRows::from_adjacency(&converted_array);

    // // Print the CSR representation
    // println!("Row Offset: {:?}", compression.row_offset);
    // println!("Column Indices: {:?}", compression.col_indices);


    // // Example with edges vector
    // let edges = vec![(1, 2), (1, 3), (1, 4), (2, 3), (2, 4),
    // (2, 5), (3, 1), (3, 4), (4, 3), (4, 5), (5, 1)];
    // let graph = CompressedSparseRows::from_edge_list(edges);
    // let adj_matrix = graph.to_adjacency_matrix();
    // println!("{:?}", adj_matrix);

    // let converted_array_2 = Array2D::from_rows(&adj_matrix).expect("Failed?");

    // let compression_2 = CompressedSparseRows::from_adjacency(&converted_array_2);

    // println!("Row Offset: {:?}", compression_2.row_offset);
    // println!("Column Indices: {:?}", compression_2.col_indices);
    
    // active_messaging_example::active_example();

    hello_world_am_example::hello_am_example();

}