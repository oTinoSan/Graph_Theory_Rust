use lamellar::array::prelude::*;
use lamellar::LamellarWorld; 
use std::sync::Arc; 
use serde::{Serialize, Deserialize};
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::path::Path;
use sssp_delta::dist_hash_maps::*;

#[derive(Serialize, Deserialize)]
struct AdjList {
    edges: Vec<(usize, f32)>,
    tent: f32,
}

impl AdjList {
    fn new() -> Self {
        AdjList {
            edges: Vec::new(),
            tent: f32::INFINITY,
        }
    }
}

async fn get_graph(world: Arc<LamellarWorld>, mat: DistHashMap, max_weight: &mut f32, path: &str) -> io::Result<()> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);

    let mut max: f32 = 0.0;
    let mut curr_node: usize = 0;
    let mut adj: Vec<(usize, f32)> = Vec::new();

    // Skip the first line (header)
    for line in reader.lines().skip(1) {
        let line = line?;
        let row: Vec<&str> = line.split(',').collect();

        if row.len() < 3 {
            continue; // Skip malformed lines
        }

        let node: usize = row[0].parse().unwrap_or(0);
        let edge: usize = row[1].parse().unwrap_or(0);
        let weight: f32 = row[2].parse().unwrap_or(0.0);

        if node != curr_node {
            if world.my_pe() == curr_node % world.num_pes() {
                let insert = AdjList {
                    edges: adj.clone(),
                    tent: f32::INFINITY,
                };
                mat.insert(curr_node, insert).await;

                max = max.max(weight);
            }

            adj.clear();
            curr_node = node;
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

// Main function to initialize Lamellar and call `get_graph`
#[lamellar::am]
fn main() {
    let world = LamellarWorldBuilder::new().build();
    let mat = LamellarHashMap::new(world.clone());
    let max_weight = LamellarAtomic::new(0.0);

    let path = Path::new("data.csv");
    get_graph(world, &mat, &max_weight, path).await.unwrap();
}