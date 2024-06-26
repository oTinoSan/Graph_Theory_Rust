use array2d::Array2D;

#[derive(Debug, Clone)]
pub struct CompressedSparseRows {
    col_indices: Vec<u64>,
    row_offsets: Vec<u64>,
}

impl CompressedSparseRows {
    pub fn from_adjacency_matrix(array: Array2D<u64>) -> Self {
        let mut col_indices = Vec::new();
        let mut row_offsets = Vec::new();
        let mut count = 0;
        for i in 0..array.row_len() {
            row_offsets.push(count);
            for j in 0..array.column_len() {
                if *array.get(i, j).unwrap() == 1 {
                    col_indices.push(j as u64);
                    count = count + 1;
                }
                
            }
        }
        row_offsets.push(count); // push index of the end of the array

        CompressedSparseRows {
            col_indices,
            row_offsets,
        }
    }
}
