use array2d::Array2D;
use std::fs;

#[derive(Debug, Clone)]
pub struct CompressedSparseRows {
    rof_offsets: Vec<u64>,
    col_indices: Vec<u64>,
}

impl CompressedSparseRows {
    pub fn from_adjacency_matrix(adj: Array2D<u64>) -> Self {
        let mut offset = 0;
        let mut rof_offsets = Vec::new();
        let mut col_indices = Vec::new();
        for row in adj.rows_iter() {
            rof_offsets.push(offset);
            for (j, col) in row.enumerate() {
                if *col != 0 {
                    col_indices.push(j as u64);
                    offset += 1;
                }
            }
        }
        rof_offsets.push(offset);
        CompressedSparseRows {
            rof_offsets,
            col_indices,
        }
    }

    pub fn from_edge_list(mut edges: Vec<(u64, u64)>) -> Self {
        edges.sort_unstable_by(|(a, _), (b, _)| a.partial_cmp(b).unwrap());

        let mut rof_offsets: Vec<u64> = vec![0];
        let mut col_indices = vec![];

        for (src, dest) in edges {
            if rof_offsets.len() <= src as usize {
                rof_offsets.push(*rof_offsets.get(src as usize - 1).unwrap_or_else(|| &0));
            }
            rof_offsets[src as usize] += 1;
            col_indices.push(dest);
        }

        Self {
            rof_offsets,
            col_indices,
        }
    }

    pub fn to_adjacency_matrix(self) -> Array2D<u64> {
        let size = self.rof_offsets.len() - 1;
        let mut adj = Array2D::filled_with(0, size, size);
        for row in 0..size {
            for i in self.rof_offsets[row]..self.rof_offsets[row + 1] {
                *adj.get_mut(row, self.col_indices[i as usize] as usize)
                    .unwrap() = 1;
            }
        }
        adj
    }
}

pub fn edge_list_from_file(filename: &str) -> Vec<(u64, u64)> {
    let contents = fs::read_to_string(filename).expect("could not read file");
    let mut edges = vec![];
    for line in contents.lines() {
        let line: Vec<&str> = line.split(',').collect();
        edges.push((line[0].trim().parse().unwrap(), line[1].trim().parse().unwrap()));
    }
    edges
}