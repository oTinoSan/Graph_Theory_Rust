use array2d::Array2D;
use graph_theo::compressed_sparse_rows::CompressedSparseRows;

fn main() {
    // Define the adjacency matrix for the given graph
    let rows: Vec<Vec<u64>> = vec![
        vec![0, 1, 1, 1, 0],
        vec![0, 0, 1, 1, 1],
        vec![1, 0, 0, 1, 0],
        vec![0, 0, 1, 0, 1],
        vec![1, 0, 0, 0, 0],
    ];

    let converted_array = Array2D::from_rows(&rows).expect("Failed?");

    // Convert the adjacency matrix to CSR format
    let compression = CompressedSparseRows::from_adjacency(&converted_array);

    // Print the CSR representation
    println!("Row Offset: {:?}", compression.row_offset);
    println!("Column Indices: {:?}", compression.col_indices);
}
