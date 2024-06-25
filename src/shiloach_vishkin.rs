use super::CompressedSparseRows;

pub fn shiloach_vishkin(graph: CompressedSparseRows) -> Vec<usize> {
    let rows = graph.rof_offsets.len() - 1;
    let mut D: Vec<_> = (0..rows).collect();
    let mut graft = true;
    let mut iterations = 0;

    while graft {
        iterations += 1;
        graft = false;
        
        println!("Shiloach-Vishkin iteration {}", iterations);

        // "graft" phase - union operations

        for i in 0..rows {
            for k in graph.rof_offsets[i]..graph.rof_offsets[i + 1] {
                let row = i;
                let col = graph.col_indices[k as usize] as usize;
                let col_parent = D[col];
                let row_parent = D[row];

                if D[row] < D[col] && D[col] == D[D[col]] {
                    let temp = D[col];
                    D[temp] = temp;
                    graft = true;
                }

                if D[col] < D[row] && D[row] == D[D[row]] {
                    let temp = D[row];
                    D[temp] = D[col];
                    graft = true;
                }
            }
        }

        // "hook" phase - path compression
        for i in 0..rows {
            while D[i] != D[D[i]] {
                D[i] = D[D[i]]
            }
        }
    }

    D
}