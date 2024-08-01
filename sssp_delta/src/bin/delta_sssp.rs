use lamellar::active_messaging::prelude::*;
use lamellar::darc::prelude::*;
use std::env::{self, args};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Instant;
use std::collections::{HashMap, HashSet};
use rayon::prelude::*;
use sssp_delta::dist_hash_map::{DistHashMap, DistCmdResult, AdjList}; 
use sssp_delta::dist_hash_set::*;

/* 
arg[0] = executable
arg[1] = rmat_scale
arg[2] = num_buckets
arg[3] = delta


fn generate_rmat_graph(
    world: &(), // placeholder for ygm::comm equivalent
    map: &Mutex<HashMap<usize, AdjList>>,
    rmat_scale: usize,
    max_weight: &mut f32,
*/

fn main() {
    let args: Vec<String> = env::args().collect();

    for (index, arg) in args.iter().enumerate() {
        println!("Argument {}: {}", index, arg);
    }

    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    world.barrier();

    let buckets: Vec<DistHashSet> = Vec::new();
    let distributed_map = DistHashMap::new(&world, num_pes);


    ////////////////////////////
    // placeholder variables //
    //////////////////////////
    
    let mut num_buckets: usize = 0;
    let mut delta: f32 = 3.0;
    let mut max_weight: f32 = 0.0;
    let max_degree: f32;
    let inf = f32::INFINITY;

    if args.len() >= 3 {
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
    // generate_rmat_graph(&world, &mut map, rmat_scale, &mut max_weight);
    // end timing
    let end = Instant::now();
    let duration = end.duration_since(beg);


    if args.len() == 2 {
        num_buckets = args[2].parse::<usize>().unwrap();
    } else {
        num_buckets = args[2].parse::<usize>().unwrap();
        delta = args[3].parse::<f32>().unwrap();
    }

    ///////////////////////////
    // compute elapsed time //
    /////////////////////////

    // use std::sync::atomic::{AtomicU64, Ordering};
    // use std::sync::Arc;
    // use std::thread;
    // use std::time::{Duration, Instant};
    
    //     let beg = Instant::now();
    //     // Simulate some work
    //     thread::sleep(Duration::from_millis(100));
    //     let end = Instant::now();
    
    //     let duration = end.duration_since(beg);
    //     let time = duration.as_micros() as u64;
    
    //     // Shared atomic variable to store the global maximum time
    //     let global_max_time = Arc::new(AtomicU64::new(0));
    
    //     // Number of threads to simulate
    //     let num_threads = 4;
    //     let mut handles = vec![];
    
    //     for _ in 0..num_threads {
    //         let global_max_time = Arc::clone(&global_max_time);
    //         let time = time;
    
    //         let handle = thread::spawn(move || {
    //             // Simulate some work
    //             thread::sleep(Duration::from_millis(50));
    
    //             // Update the global maximum time
    //             global_max_time.fetch_max(time, Ordering::SeqCst);
    //         });
    
    //         handles.push(handle);
    //     }
    
    //     // Wait for all threads to finish
    //     for handle in handles {
    //         handle.join().unwrap();
    //     }
    
    //     // Get the global maximum time
    //     let global_max_time = global_max_time.load(Ordering::SeqCst);
    //     println!("Global max time: {}", global_max_time);


    //////////////////////////////////////
    // create world and ditributed map //
    ////////////////////////////////////
    
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    world.barrier();
    let distributed_map = DistHashMap::new(&world, num_pes);
    let adj_list = AdjList {
        edges: vec![(1, 0.5), (2, 1.2), (3, 0.8)],
        tent: 2.5,
    };


    ////////////////////////////////////////////////////////////
    // set up nodes and pe's, set tent distances to infinity //
    //////////////////////////////////////////////////////////
    
    for i in 0..10 {
        let _ = distributed_map.add(i, adj_list.clone());
        //set tentative distances to infinity
        distributed_map.visit(i, inf);
    };


    ////////////////////////////////////////
    // add the sets to the bucket vector //
    //////////////////////////////////////

    for i in 0..num_buckets {
        let new_bucket = DistHashSet::new(&world, num_pes);
        new_bucket.add_set(i as i32);
    }
    
    // start timing  buckets.emplace_back(world);
    let beg = Instant::now();
    let idx: usize = 0;


    ///////////////////////
    // relax the source //
    /////////////////////

    world.block_on(async {
        // your code here
        distributed_map.visit(0, 0.0).await;
        // your code here
    });
    
    buckets[0].add_set(0);
    

    ///////////////////////////////////
    // duplicate the current bucket //
    /////////////////////////////////

    let heavy_bucket = DistHashSet::new(&world, num_pes);
    world.block_on(async {
        for i in buckets[idx].data.read().await.iter() {
            heavy_bucket.add_set(*i);
        }
    });


    //////////////////////////
    // process the buckets //
    ////////////////////////

    while idx < num_buckets {
        world.block_on(async {
            for i in buckets[idx].data.read().await.iter() {
                heavy_bucket.add_set(*i);
                let map_clone = distributed_map.clone();
                // iterates through each edge in adj_list  
                if let DistCmdResult::Get(adj_list_result) = map_clone.get(*i).await {
                for (edge, weight) in adj_list_result.edges {
                    if edge as usize <= delta as usize {
                        let potential_tent = adj_list_result.tent + weight as f32;
                        if let DistCmdResult::Relax(new_idx) = distributed_map.relax_requests(*i, potential_tent, delta).await {
                            buckets[new_idx as usize].add_set(*i);
                            if let DistCmdResult::Get(current_adj_list) = distributed_map.get(*i).await {
                            // check to see if get tent matches potential tent, if so, erase.
                                if let DistCmdResult::Compare(true) = distributed_map.compare_tent(*i, potential_tent).await {
                                    buckets[*i as usize].erase_set_item(*i);
                                }
                            }
                        }
                    }
                }
            }
        }
    });
    
        
        ///////////////////////////////
        // process the heavy bucket //
        /////////////////////////////

        world.block_on(async {
        for i in heavy_bucket.data.read().await.iter() {
            let map_clone = distributed_map.clone();
            if let DistCmdResult::Get(adj_list_result) = map_clone.get(*i).await {
            // iterates through each edge in adj_list
            for (edge, weight) in adj_list_result.edges {
                if edge as usize > delta as usize {
                    let potential_tent = adj_list_result.tent + weight as f32;
                    if let DistCmdResult::Relax(new_idx) = distributed_map.relax_requests(*i, potential_tent, delta).await {
                        buckets[new_idx as usize].add_set(*i);
                        if let DistCmdResult::Get(current_adj_list) = distributed_map.get(*i).await {
                            // check to see if get tent matches potential tent, if so, erase.
                            if let DistCmdResult::Compare(true) = distributed_map.compare_tent(*i, potential_tent).await {
                                heavy_bucket.erase_set_item(*i);
                            }
                        }
                    }
                }
            }
        }
    }
});


        world.barrier();
        // done with this bucket
        idx += 1;


    /////////////////////////
    // empty heavy bucket //
    ///////////////////////
    
    heavy_bucket.empty_set();

    }
        
    // end timing
    let end = Instant::now();

    // compute total elapsed time
    let duration = end.duration_since(beg);
    let time = duration.as_micros();
    let global_time = world.all_reduce_max(time);
    
    let num_edges: u64 = 0;


    for (key, adj_matrix) in distributed_map.iter() {
        num_edges += vertex.edges.len(); 
    }
    
    let global_num_edges = world.all_reduce_sum(num_edges);

    if world_rank() == 0 {
        println!("{}", global_time as f64 / 1000.0);
        println!("{}", global_num_edges);
    }

    // print out final distances from source for each node
    for (node, adj_matrix) in distributed_map.iter() {
    println!("{}, {}", node, adj_matrix.tent);
    }

}

