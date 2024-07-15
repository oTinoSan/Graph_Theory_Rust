use std::env;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Instant;
use std::collections::HashMap;
use rayon::prelude::*;
// use super::dist_hash_maps::*; 


struct AdjList {
    edges: Vec<(usize, f32)>,
    tent: f32,
}

fn generate_rmat_graph(
    _world: &(), // placeholder for ygm::comm equivalent
    map: &Mutex<HashMap<usize, AdjList>>,
    rmat_scale: usize,
    max_weight: &mut f32,
) {
    // ... implementation of RMAT graph generation ...
}

fn main() {
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    world.barrier();

    // the array of sets of vertices
    let buckets: Arc<Mutex<Vec<HashSet<usize>>>> = Arc::new(Mutex::new(vec![HashSet::new(); num_buckets]));
    let distributed_map = DistHashMap::new(&world, num_pes);
    let graph: Arc<Mutex<HashMap<usize, Vec<(usize, f32)>>>> = Arc::new(Mutex::new(HashMap::new()));
    
    // placeholder, and will need to be changed
    let mut num_buckets: usize = 0; 
    let mut delta: f32 = 3.0;
    let mut max_weight: f32 = 0.0; // max shortest path, use 21 for testing
    let max_degree: f32;

    if args.len() > 1 {
        // num_buckets = args[1].parse().expect("Error parsing num_buckets");
        // delta = args[2].parse().expect("Error parsing delta"); // 3 for testing
        // here is the lookup map for vertices and their best tent values adj list (as a struct)
        //getGraph(world, map, max_weight, path);
        let rmat_scale: i32 = args[1].parse().expect("Error parsing rmat_scale");
    } else {
        println!("Please run the program with at least 3 arguments.");
    }
   
    // Start timing
    let beg = Instant::now();
    // placeholder for rmat generation
    generate_rmat_graph(&world, &mut map, rmat_scale, &mut max_weight);
    // End timing
    let end = Instant::now();
    let duration = end.duration_since(beg);

    let mut degree = 0;

    // not exactly sure where this 'edges' comes from
    distributed_map.iter().for_each(|(_k, v)| {
        if v.edges.len() > degree {
            degree = v.edges.len();
        }
    });


    if args.len() == 2 {
        let max_degree = max_degree.load(Ordering::SeqCst) as f32;
        delta = 1.0 / max_degree;
        num_buckets = (max_weight / delta).ceil() as usize + 1;
    } else {
        num_buckets = args[2].parse::<usize>().unwrap();
        delta = args[3].parse::<f32>().unwrap();
    }

        // ... remaining code for bucket processing and timing
    }

    // ... remaining code for edge processing and timing
}

