use array2d::Array2D;

#[derive(Debug, Clone)]
pub struct Csr {
    pub rof_offset: Vec<u64>,
    pub col_indices: Vec<u64>,
}

impl Csr {
    pub fn from_adj_mat(adj: Array2D<u64>) -> Self {
        let mut rof_offset = vec![];
        let mut col_indices: Vec<u64> = vec![];
        let mut counter = 0;

        rof_offset.push(0);
        for row in adj.rows_iter(){
            for (column, item) in row.enumerate() {
                if *item == 1 {
                    counter+=1;
                    col_indices.push(column as u64);
                }
            }
            rof_offset.push(counter);
        }

        Self{
            rof_offset,
            col_indices,
        }
    }

}



