use array2d::Array2D;

pub struct CompressedSparseRows {
    pub values: Vec<u64>,
    pub row_offset: Vec<u64>,
    pub col_indices: Vec<u64>,
}


impl CompressedSparseRows {
    pub fn from_adjacency(adj: &Array2D<u64>) -> Self {
        let mut counter = 0;
        let mut values = vec![];
        let mut row_offset = vec![0];
        let mut col_indices = Vec::new();

        for row in adj.rows_iter() {
            for (column, &item) in row.enumerate() {
                if item != 0 {
                    counter += 1;
                    values.push(item);
                    col_indices.push(column as u64);
                }
            }
            row_offset.push(counter);
        }

        Self { values, row_offset, col_indices }
    }


    pub fn edges_to_csr(mut edges: Vec<(u64, u64, u64)>) -> Self {
        edges.sort_unstable_by(|(a, _, _), (b, _, _)| a.partial_cmp(b).unwrap());

        let mut values = Vec::new();
        let mut row_offset: Vec<u64> = vec![0];
        let mut col_indices = Vec::new();

        for (source, dest, value) in edges {
            while row_offset.len() <= source as usize {
                row_offset.push(col_indices.len() as u64);
            }
            col_indices.push(dest);
            values.push(value);
        }
        row_offset.push(col_indices.len() as u64); 

        Self { values, row_offset, col_indices }
    }


    pub fn to_adjacency_matrix(&self) -> Vec<Vec<u64>> {
        let num_rows = self.row_offset.len() - 1;
        let max_col_index = *self.col_indices.iter().max().unwrap_or(&0) as usize;
        let matrix_size = std::cmp::max(num_rows, max_col_index + 1);

        let mut adj_matrix = vec![vec![0; matrix_size]; matrix_size];

        for row in 0..num_rows {
            let start = self.row_offset[row] as usize;
            let end = self.row_offset[row] as usize;
            for col_index in start..end {
                let col = self.col_indices[col_index] as usize;
                let value = self.values[col_index]; 
                adj_matrix[row][col] = value; 
            }
        }

        adj_matrix
    }
}

fn main() {
    /////////////////////////////
    /// Coverts a vec to CSR ///
    ///////////////////////////

    let rows: Vec<Vec<u64>> = vec![
        vec![0, 0, 2, 5, 0, 0],
        vec![8, 0, 0, 0, 1, 0],
        vec![0, 9, 3, 0, 0, 4],
        vec![0, 7, 0, 0, 0, 2],
        vec![1, 0, 6, 0, 0, 9],
    ];

    let converted_array = Array2D::from_rows(&rows).expect("Failed?");

    // // Convert the adjacency matrix to CSR format
    let compression = CompressedSparseRows::from_adjacency(&converted_array);

    // // Print the CSR representation
    println!("Values: {:?}", compression.values);
    println!("Column Indices: {:?}", compression.col_indices);
    println!("Row Offset: {:?}", compression.row_offset);

    
    /////////////////////////////
    /// Coverts edges to CSR ///
    ///////////////////////////

    let edge_list = vec![
        (1, 2, 10), (1, 3, 15), (1, 4, 20), 
        (2, 3, 25), (2, 4, 30), (2, 5, 35), 
        (3, 1, 40), (3, 4, 45), (4, 3, 50), 
        (4, 5, 55), (5, 1, 60)
    ];

    let edge_conversion = CompressedSparseRows::edges_to_csr(edge_list);

    println!("Values: {:?}", edge_conversion.values);
    println!("Column Indices: {:?}", edge_conversion.col_indices);
    println!("Row Offset: {:?}", edge_conversion.row_offset);


    ///////////////////////////////
    /// Converts CSR to matrix ///
    /////////////////////////////

    let csr = CompressedSparseRows {
        values: vec![2, 5, 8, 1, 9, 3, 4, 7, 2, 1, 6, 9],
        row_offset: vec![2, 4, 7, 9, 12],
        col_indices: vec![2, 3, 0, 4, 1, 2, 5, 1, 5, 0, 2, 5],
    };

    let adj_matrix = csr.to_adjacency_matrix();
    for row in adj_matrix {
        println!("{:?}", row);
    }

    // let graph = CompressedSparseRows::from_edge_list(edges);
    // let adj_matrix = graph.to_adjacency_matrix();
    // println!("{:?}", adj_matrix);
    // let converted_array_2 = Array2D::from_rows(&adj_matrix).expect("Failed?");

    // let compression_2 = CompressedSparseRows::from_adjacency(&converted_array_2);

    // println!("Row Offset: {:?}", compression_2.row_offset);
    // println!("Column Indices: {:?}", compression_2.col_indices);
}