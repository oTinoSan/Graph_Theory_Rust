use lamellar::array::prelude::*;
use lamellar::LamellarWorld; 
use std::sync::Arc; 
use serde::{Serialize, Deserialize};
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::path::Path;
use sssp_delta::dist_hash_maps::{self, *};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Serialize, Deserialize)]
struct AdjList {
    edges: Vec<(usize, f32)>,
    tent: f32,
}

async fn get_graph(world: Arc<LamellarWorld>, mat: DistHashMap, max_weight: &mut f32, path: &str) -> io::Result<()> {

    let inf = f32::INFINITY;
    let mut max: f32 = 0.0;

    let file = File::open(path)?;
    let reader = BufReader::new(file);

    let mut curr_node: usize = 0;
    let mut adj: Vec<(usize, f32)> = Vec::new();

    // Skip the first line (header)
    for line in reader.lines().skip(1) {
        let line = line?;
        let row: Vec<&str> = line.split(',').collect();

        if row.len() < 3 {
            continue; 
        }

        let node: usize = row[0].parse().unwrap_or(0);
        let edge: usize = row[1].parse().unwrap_or(0);
        let weight: f32 = row[2].parse().unwrap_or(0.0);


        // Load adjacency row into matrix
        if row[0].parse::<usize>().unwrap_or_default() != curr_node {
            if world.my_pe() == curr_node % world.num_pes() {
                let insert = (adj.clone(), 
                f32::INFINITY); // Assuming `Inf` is f32::INFINITY
                mat.add(curr_node, insert); // Assuming a synchronous `insert` instead of `async_insert`

                // Update maxs
                if let Ok(value) = row[2].parse::<i32>() {
                    if max < value {
                        max = value;
                    }
                }
            }

    adj.clear();
    curr_node += 1;
}
        adj.push((edge, weight));
    }

    if world.my_pe() == curr_node % world.num_pes() {
        let insert = AdjList {
            edges: adj,
            tent: f32::INFINITY,
        };
        mat.insert(curr_node, insert).await;
    }

    world.barrier().await;

    // Assuming `all_reduce_max` is a method you have or can implement
    // This is a placeholder to show where you would perform the reduction.
    // Lamellar does not directly support `all_reduce_max` for f32, so you would need to implement it.
    // For simplicity, we're directly assigning `max` to `max_weight`, but in a real application,
    // you would perform an all-reduce operation here.
    max_weight.set(max).await;

    Ok(())
}


#[lamellar::am]
#[tokio::main]
async fn main() -> io::Result<()> {
    let world = LamellarWorldBuilder::new().build();
    let mat = LamellarHashMap::new(world.clone());
    let max_weight = LamellarAtomic::new(0.0);

    // Accepting file path as a command-line argument
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <path_to_data_csv>", args[0]);
        std::process::exit(1);
    }
    let path = Path::new(&args[1]);

    // Proper error handling
    if let Err(e) = get_graph(world, &mat, max_weight, path).await {
        eprintln!("Error processing graph: {}", e);
        return Err(e);
    }

    Ok(())
}