fn shiloach_vishkin(rows: usize, row_offsets: &[usize], col_indices: &[usize]) {
    let mut d: Vec<usize> = (0..rows).collect();
    let mut graft = true;
    let mut iterations = 0;

    while graft {
        iterations += 1;
        println!("Shiloach-Vishkin iteration {}", iterations);
        graft = false;

        // Begin "Graft" phase - union operations
        for i in 0..rows {
            for k in row_offsets[i]..row_offsets[i + 1] {
                let row = i;
                let col = col_indices[k];
                let col_parent = d[col];
                let row_parent = d[row];

                // Check whether or not i is pointing to a root and whether any of
                // it's neighbors are pointing to a vertex with smaller label.
                if d[row] < d[col] && d[col] == d[d[col]] {
                    d[d[col]] = d[row];
                    graft = true;
                }
                if d[col] < d[row] && d[row] == d[d[row]] {
                    d[d[row]] = d[col];
                    graft = true;
                }
            }
        }

        // Begin "Hook" phase - path compression
        for i in 0..rows {
            // While there exist paths of length 2 in the pointer graph, hook branches
            // of separate rooted trees onto each other until it is a rooted star...
            while d[i] != d[d[i]] {
                d[i] = d[d[i]];
            }
        }
    }
}