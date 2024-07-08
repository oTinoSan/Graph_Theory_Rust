// use array2d::Array2D;
use std::vec::Vec;
// use graph_theo::compressed_sparse_rows::CompressedSparseRows;
use graph_theo::{lamellar_exercise::active_messaging_example, shiloach_vishkin};

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
    active_messaging_example::message_launch();

}






//     // Example of reading from a file
//     // Replace "path/to/your/file.txt" with the actual file path
//     if let Ok(edges_from_file) = edge_list_from_file("path/to/your/file.txt") {
//         let graph_from_file = Graph::from_edge_list(edges_from_file);
//         let adj_matrix_from_file = graph_from_file.to_adjacency_matrix();
//         println!("{:?}", adj_matrix_from_file);
//     }
// }






use array2d::Array2D;

pub struct CompressedSparseRows {
    pub row_offset: Vec<u64>,
    pub col_indices: Vec<u64>,
}


impl CompressedSparseRows {
    pub fn from_adjacency(adj: &Array2D<u64>) -> Self {
        let mut counter = 0;
        let mut row_offset = vec![0];
        let mut col_indices = Vec::new();

        for row in adj.rows_iter() {
            for (column, &item) in row.enumerate() {
                if item == 1 {
                    counter += 1;
                    col_indices.push(column as u64 + 1);
                }
            }
            row_offset.push(counter);
        }

        Self { row_offset, col_indices }
    }


    pub fn from_edge_list(mut edges: Vec<(u64, u64)>) -> Self {
   

        let mut row_offset: Vec<u64> = vec![0];
        let mut col_indices = vec![];

        let max_node = edges.iter().map(|(src, dest)| std::cmp::max(*src, *dest)).max().unwrap_or(0) as usize;

        let mut edge_counts = vec![0; max_node + 1]; // Adjust for 1-based indexing

        // Count edges for each node to handle nodes with no outgoing edges
        for (src, _) in &edges {
            edge_counts[*src as usize] += 1;
        }

        for i in 1..=max_node {
            edge_counts[i] += edge_counts[i - 1];
            row_offset.push(edge_counts[i]);
        }

        for (src, dest) in edges {
            let index = edge_counts[src as usize - 1]; // Adjust for 1-based indexing
            col_indices.insert(index as usize, dest);
            edge_counts[src as usize - 1] += 1;
        }

        Self { row_offset, col_indices }
    }


    pub fn to_adjacency_matrix(&self) -> Vec<Vec<u64>> {
        let num_rows = self.row_offset.len() - 1;
        let max_col_index = *self.col_indices.iter().max().unwrap_or(&0) as usize;
        let matrix_size = std::cmp::max(num_rows, max_col_index + 1);

        let mut adj_matrix = vec![vec![0; matrix_size]; matrix_size];

        for row in 0..num_rows {
            let start = self.row_offset[row] as usize;
            let end = self.row_offset[row + 1] as usize;
            for &col in &self.col_indices[start..end] {
                adj_matrix[row][col as usize] = 1;
            }
        }

        adj_matrix
    }
}