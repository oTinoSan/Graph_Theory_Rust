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
        edges.sort_unstable_by(|(a, _), (b, _)| a.partial_cmp(b).unwrap());

        let mut row_offset: Vec<u64> = vec![0];
        let mut col_indices = vec![];

        for (src, dest) in edges {
            while row_offset.len() <= src as usize {
                row_offset.push(col_indices.len() as u64);
            }
            col_indices.push(dest);
        }
        row_offset.push(col_indices.len() as u64); // Ensure we can index the end of the last node's edges

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