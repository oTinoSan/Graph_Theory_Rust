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
                    col_indices.push(column as u64);
                }
            }
            row_offset.push(counter);
        }

        Self {
            row_offset,
            col_indices,
        }
    }
}
