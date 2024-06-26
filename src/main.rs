use array2d::Array2D;
use graph_exploration::*;

fn main() {
    let array: Array2D<u64> = Array2D::from_rows(
        &vec![vec![0, 1, 1, 1, 0],
        vec![0, 0, 1, 1, 1],
        vec![1, 0, 0, 1, 0],
        vec![0, 0, 1, 0, 1],
        vec![1, 0, 0, 0, 0]
        ]
    ).unwrap();

    let result = graph_exploration::CompressedSparseRows::from_adjacency_matrix(array);

    println!("{:?}", result);
}
