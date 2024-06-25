use graphs::*;
use std::env;

fn main() {
    println!("Hello, graphs!");

    let example= csr::Csr{
        rof_offset: vec![0, 2, 6, 9, 11, 14, 19, 21, 22, 24, 26, 28, 31, 33, 34, 34],
        col_indices: vec![1, 3, 0, 2, 5, 10, 1, 4, 5, 0, 4, 2, 3, 5, 1, 2, 4, 7, 10, 8, 9, 5, 6, 11, 6, 11, 1, 5, 8, 9, 12, 11, 13, 12],
    };

    println!("{:?}", shiloach_vishkin::shiloach_vishkin(example));
}
