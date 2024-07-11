/* 
// Example 1 - forest of stars
int rows  = 15;
int edges = 20;

int row_offsets[rows+1] {0, 2, 7, 8, 9, 10, 11, 12, 12, 14, 15, 16, 17, 18, 19, 20};
int col_indices[edges]  {3, 5, 4, 10, 12, 13, 14, 6, 0, 1, 0, 2, 9, 11, 8, 1, 8, 1, 1, 1};
*/

/* 
// Example 2 - forest, same components
int rows  = 15;
int edges = 20;

int row_offsets[rows+1] {0, 1, 3, 4, 6, 7, 8, 9, 9, 10, 12, 15, 16, 17, 19, 20};
int col_indices[edges]  {3, 4, 10, 6, 0, 5, 1, 3, 2, 9, 8, 11, 1, 12, 13, 9, 10, 10, 14, 13};
*/

/*
// Example 3 - complete graph on 6 vertices (K6)
int rows  = 6;
int edges = 30;

int row_offsets[rows+1] {0, 5, 10, 15, 20, 25, 30};
int col_indices[edges]  {1, 2, 3, 4, 5, 0, 2, 3, 4, 5, 0, 1, 3, 4, 5, 0, 1, 2, 4, 5, 0, 1, 2, 3, 5, 0, 1, 2, 3, 4};
*/

/*
// Example 4
let rows: usize = 15;
let row_offsets: Vec<usize> = vec![0, 2, 6, 9, 11, 14, 19, 21, 22, 24, 26, 28, 31, 33, 34, 34];
let col_indices: Vec<usize> = vec![1, 3, 0, 2, 5, 10, 1, 4, 5, 0, 4, 2, 3, 5, 1, 2, 4, 7, 10, 8, 9, 5, 6, 11, 6, 11, 1, 5, 8, 9, 12, 11, 13, 12];
*/

pub fn run_connected_components(rows: usize, row_offsets: Vec<usize>, col_indices: Vec<usize>) -> Vec<usize> {
    // Initialize the vector of connected component labels
    let mut d: Vec<usize> = (0..rows).collect();

    let mut graft = true;
    let mut iterations = 0;

    // Begin main Shiloach-Vishkin iteration
    while graft {
        iterations += 1;
        graft = false;

        println!("Shiloach-Vishkin iteration {}", iterations);

        // Begin "Graft" phase - union operations
        for i in 0..rows {
            for k in row_offsets[i]..row_offsets[i + 1] {
                let row = i;
                let col = col_indices[k];
                // Removed unused variables col_parent and row_parent

                // Store the values in temporary variables before mutation
                let col_parent = d[col];
                let row_parent = d[row];

                // Check whether or not i is pointing to a root and whether any of
                // it's neighbors are pointing to a vertex with smaller label.
                if row_parent < col_parent && col_parent == d[col_parent] {
                    d[col_parent] = row_parent;
                    graft = true;
                }

                if col_parent < row_parent && row_parent == d[row_parent] {
                    d[row_parent] = col_parent;
                    graft = true;
                }
            } // End loop over vertex i's out edges
        } // End "graft" phase

        // Begin "Hook" phase - path compression
        for i in 0..rows {
            // While there exist paths of length 2 in the pointer graph, hook branches
            // of separate rooted trees onto each other until it is a rooted star...
            while d[i] != d[d[i]] {
                d[i] = d[d[i]];
            }
        } // End "hook" phase
    } // End Shiloach-Vishkin iteration

    d
}

fn main() {
    let rows: usize = 15;
    let row_offsets: Vec<usize> = vec![0, 2, 6, 9, 11, 14, 19, 21, 22, 24, 26, 28, 31, 33, 34, 34];
    let col_indices: Vec<usize> = vec![1, 3, 0, 2, 5, 10, 1, 4, 5, 0, 4, 2, 3, 5, 1, 2, 4, 7, 10, 8, 9, 5, 6, 11, 6, 11, 1, 5, 8, 9, 12, 11, 13, 12];
    run_connected_components(rows, row_offsets, col_indices);
}