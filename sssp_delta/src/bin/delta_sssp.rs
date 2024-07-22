use lamellar::active_messaging::prelude::*;
use lamellar::darc::prelude::*;
use std::env::{self, args};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Instant;
use std::collections::{HashMap, HashSet};
use rayon::prelude::*;
use sssp_delta::dist_hash_maps::*; 

// arg[0] = executable
// arg[1] = rmat_scale
// arg[2] = num_buckets
// arg[3] = delta



// fn generate_rmat_graph(
//     world: &(), // placeholder for ygm::comm equivalent
//     map: &Mutex<HashMap<usize, AdjList>>,
//     rmat_scale: usize,
//     max_weight: &mut f32,
// ) {
    // ... implementation of RMAT graph generation ...

fn main() {
    let args: Vec<String> = env::args().collect();

    for (index, arg) in args.iter().enumerate() {
        println!("Argument {}: {}", index, arg);
    }

    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    world.barrier();

    let buckets: Vec<HashSet<usize>> = Vec::new();
    let distributed_map = DistHashMap::new(&world, num_pes);
    
    // placeholder, and will need to be changed
    let mut num_buckets: usize = 0;
    let mut delta: f32 = 3.0;k
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
   
    // start timing
    let beg = Instant::now();
    // placeholder for rmat generation
    generate_rmat_graph(&world, &mut map, rmat_scale, &mut max_weight);
    // end timing
    let end = Instant::now();
    let duration = end.duration_since(beg);


    if args.len() == 2 {
        num_buckets = args[2].parse::<usize>().unwrap();
    } else {
        num_buckets = args[2].parse::<usize>().unwrap();
        delta = args[3].parse::<f32>().unwrap();
    }

    // compute total elapsed time
    let duration = end.duration_since(beg);
    let time = duration.as_micros() as u64;
    let universe = mpi::initialize().unwrap();
    let world = universe.world();
    let global_time = world.all_reduce_into(&time, SystemOperation::max());


    // Add the sets to the vector
    for _ in 0..num_buckets {
        buckets.push(&world);
    }

    // start timing
    let beg = Instant::now();
    let idx: u64 = 0;

    // complete a source relaxation --------------------------------------------------------------------------------------
    // relax the source
    // Asynchronous visit to the source node


    // Asynchronous insert into the first bucket
    world.exec_am_all(move || {
        // Assuming `buckets` is accessible and supports async insertion
        buckets[0].async_insert(0);
    });
    distributed_map();


    // Start timing
    let beg = std::time::Instant::now();

    let mut idx = 0;
}