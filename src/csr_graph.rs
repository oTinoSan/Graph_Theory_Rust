// use std::fs;
// use std::io::{self, BufRead};
// use std::path::Path;

// fn edge_list_from_file<P: AsRef<Path>>(filename: P) -> io::Result<Vec<(u64, u64)>> {
//     let file = fs::File::open(filename)?;
//     let reader = io::BufReader::new(file);
//     let mut edges = vec![];

//     for line in reader.lines() {
//         let line = line?;
//         let parts: Vec<&str> = line.split(',').collect();
//         if parts.len() == 2 {
//             let src = parts[0].trim().parse().unwrap();
//             let dest = parts[1].trim().parse().unwrap();
//             edges.push((src, dest));
//         }
//     }

//     Ok(edges)
// }