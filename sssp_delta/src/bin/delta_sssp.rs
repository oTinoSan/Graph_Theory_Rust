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
    ///////////////////////////////////////////////
    // collect arguments, set up world, buckets //
    /////////////////////////////////////////////
     
    
    let args: Vec<String> = env::args().collect();
    let world = lamellar::LamellarWorldBuilder::new().build();
    world.barrier();
    let num_pes = 10; 
    let buckets: Vec<DistHashSet> = Vec::new();
    let map = DistHashMap::new(&world, num_pes);


    ////////////////////////////
    // placeholder variables //
    //////////////////////////
    
    let mut num_buckets: usize = 0;
    let mut delta: f32 = 3.0;
    let mut max_weight: f32 = 0.0;
    let max_degree: f32;
    let inf = f32::INFINITY;


    //////////////////////
    // read arguments  //
    ////////////////////

    if args.len() > 1 {

        //  num_buckets = args[2].parse::<usize>().unwrap(); // = ceil(max_cost / delta) + 2; -> 9 for testing
        // delta = args[2].parse::<f32>().unwrap(); // -> 3 for testing
        // let path = args[1].clone();        
        let rmat_scale: i32 = args[1].parse().expect("Error parsing rmat_scale");
        // here is the lookup map for vertices and their best tent values adj list (as a struct)
        //getGraph(world, map, max_weight, path);

        ////////////////////////////
        // time_rmat_generation  //
        //////////////////////////

        // start timing
        let beg = Instant::now();
        // placeholder for rmat generation
        //generate_rmat_graph(&world, &mut map, rmat_scale, &mut max_weight);
        // end timing
        let end = Instant::now();
        let degree = 0;


    ///////////////////////
    // find_map_degree  //
    /////////////////////
    
    world.wait_all();
    world.barrier();
    let num_pes = args[2].parse().expect("Error parsing number of pes");

        let distributed_map = DistHashMap::new(&world, num_pes);

        let map_clone = distributed_map.clone();
        world.block_on(async move {
            for i in 0..num_pes {
                if let DistCmdResult::Get(adj_list) = map_clone.get(i as i32).await {
                let degree = adj_list.edges.len();
                }
            }
        });

    if args.len() > 2 {
        let max_degree = world.all_reduce_max(degree);
        let delta = 1.0/max_degree;
        num_buckets = ((max_weight/delta).ceil() + 1.0) as usize;
    } else {
        num_buckets = args[2].parse().expect("Error parsing number of buckets");
        let delta = args[3].parse().expect("Error parsing delta input");
    }

    //////////////////////////////////
    // compute_total_elapsed_time  //
    ////////////////////////////////

    let duration = end.duration_since(beg);
    let time = duration.as_secs();
    let global_time = world.all_reduce_max(time);


    ////////////////////////////////////
    // generate rmat graph and time  //
    //////////////////////////////////
    
    world.block_on(async move {
        if world_rank == 0 {
            println!("{}", num_buckets);
            println!("{}", global_time / 1000.0);
        }
         else {
            let path = "";
            let degree = 0;

            let beg = Instant::now();
            generate_rmat_graph(world, map, 8, max_weight);
            let end = Instant::now();
            world.block_on(async move {
                for i in 0..num_pes {
                    if let DistCmdResult::Get(adj_list) = map_clone.get(i as i32).await {
                    let degree = adj_list.edges.len();
                    }
                }
            });
            let max_degree = world.all_reduce_max(degree);
            let delta = 1.0/max_degree;
            num_buckets = ((max_weight/delta).ceil() + 1.0) as usize;
        }
    });
    }



    //////////////////////////////////////
    // create world and ditributed map //
    ////////////////////////////////////
    
    let world = lamellar::LamellarWorldBuilder::new().build();
    // let my_pe = world.my_pe();
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
    
    world.wait_all();
    world.barrier();
    let map_clone = distributed_map.clone();
    world.block_on(async move {
        for i in 0..num_pes {
            let _ = map_clone.add(i as i32, adj_list.clone());
            //set tentative distances to infinity
            map_clone.visit(i as i32, inf).await;
        }
    });


    ////////////////////////////////////////
    // add the sets to the bucket vector //
    //////////////////////////////////////
    world.wait_all();
    world.barrier();
    let new_bucket = DistHashSet::new(&world, num_pes);
    world.block_on(async move {
        for i in 0..num_buckets {
            let _ = new_bucket.add_set(i as i32);
        }
    });
    
    // start timing  buckets.emplace_back(world);
    // let beg = Instant::now();
    let mut idx: usize = 0;


    ///////////////////////
    // relax the source //
    /////////////////////

    world.block_on(async {
        distributed_map.visit(0, 0.0).await;
        let _ = buckets[0].add_set(0);
    });
    
    
    ///////////////////////////////////
    // duplicate the current bucket //
    /////////////////////////////////

    let heavy_bucket = DistHashSet::new(&world, num_pes);
    world.block_on(async {
        for i in buckets[idx].data.read().await.iter() {
            let _ = heavy_bucket.add_set(*i);
        }
    });


    //////////////////////////
    // process the buckets //
    ////////////////////////

    while idx < num_buckets {
        world.block_on(async {
            for i in buckets[idx].data.read().await.iter() {
                let _ = heavy_bucket.add_set(*i);
                let map_clone = distributed_map.clone();
                // iterates through each edge in adj_list  
                if let DistCmdResult::Get(adj_list_result) = map_clone.get(*i).await {
                    for (edge, weight) in adj_list_result.edges {
                        if edge as usize <= delta as usize {
                            let potential_tent = adj_list_result.tent + weight as f32;
                            if let DistCmdResult::Relax(new_idx) = distributed_map.relax_requests(*i, potential_tent, delta).await {
                                let _ = buckets[new_idx as usize].add_set(*i);
                                if let DistCmdResult::Get(current_adj_list) = distributed_map.get(*i).await {
                                // check to see if get tent matches potential tent, if so, erase.
                                    let tent_to_compare = current_adj_list.tent;
                                    if let DistCmdResult::Compare(true) = distributed_map.compare_tent(*i, tent_to_compare).await {
                                        let _ = buckets[*i as usize].erase_set_item(*i);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        })
    }


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
                            let _ = buckets[new_idx as usize].add_set(*i);
                            if let DistCmdResult::Get(current_adj_list) = distributed_map.get(*i).await {
                                // check to see if get tent matches potential tent, if so, erase.
                                let tent_to_compare = current_adj_list.tent;
                                if let DistCmdResult::Compare(true) = distributed_map.compare_tent(*i, tent_to_compare).await {
                                    let _ = buckets[*i as usize].erase_set_item(*i);
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
    
    
        ///////////////////////////////////////
        // empty heavy bucket, print result //
        /////////////////////////////////////
        
        world.block_on(async {
            heavy_bucket.empty_set().await;
        });
    

        let end = Instant::now();
        let duration = end.duration_since(beg);
        let time = duration.as_secs();
        let global_time = world.all_reduce_max(time);

        let num_edges = 0;
    
        
        for (key, adj_matrix) in distributed_map.iter() {
            num_edges += vertex.edges.len(); 
        }
        
        let global_num_edges = world.all_reduce_sum(num_edges);
    
        if world_rank() == 0 {
             println!("{}", global_time as f64 / 1000.0);
             println!("{}", global_num_edges);
        }
    
        // // print out final distances from source for each node
        // for (node, adj_matrix) in distributed_map.iter() {
        // println!("{}, {}", node, adj_matrix.tent);
        // }
    
}

