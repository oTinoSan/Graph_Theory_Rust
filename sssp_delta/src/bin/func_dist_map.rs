use lamellar::active_messaging::prelude::*;
use lamellar::darc::prelude::*;
use std::env::{self, args};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Instant;
use std::collections::{HashMap, HashSet};
use rayon::prelude::*;
use sssp_delta::dist_hash_set::*;
use sssp_delta::dist_hash_map::*;
fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    world.barrier();
    let distributed_map = DistHashMap::new(&world, num_pes);
    let adj_list = AdjList {
        edges: vec![(1, 0.5), (2, 1.2), (3, 0.8)],
        tent: 2.5,
    };

    for i in 0..10 {
        // we can ignore the 'unused' result here because we call 'wait_all' below, otherwise to ensure each request completed we could use 'block_on'
        let _ = distributed_map.add(i, adj_list.clone());
    };


    world.wait_all();
    world.barrier();
    let map_clone = distributed_map.clone();
    world.block_on(async move {
        for i in 0..10 {
            println!("{}: {:?}", i, map_clone.get(i).await);
        }
    });


    // world.barrier();
    // let local_data = world.block_on(distributed_map.data.read());
    // println!(
    //     "[{my_pe}] local data: {:?}",
    //     local_data
    // );
    // drop(local_data);


    // let n_tent = 5.0;
    // world.block_on(async {
    //     if let DistCmdResult::Visit(Some(updated_adj_list)) = distributed_map.visit(9, n_tent).await {
    //         println!("{:?}", updated_adj_list);
    //     }
    //     else {
    //         println!("Key does not exist")
    //     }
    // });
}