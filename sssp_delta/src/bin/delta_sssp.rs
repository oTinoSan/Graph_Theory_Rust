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

struct AdjList {
    edges: Vec<(usize, f32)>,
    tent: f32,
}

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
   
    // start timing
    let beg = Instant::now();
    // placeholder for rmat generation
    generate_rmat_graph(&world, &mut map, rmat_scale, &mut max_weight);
    // end timing
    let end = Instant::now();
    let duration = end.duration_since(beg);

    let mut degree = 0;

    // loops through the the hash map data to find degree
    distributed_map.iter().for_each(|(_k, v)| {
        if v.data.len() > degree {  // 'data' must be substituted for adjacency list edges
            degree = v.data.len(); // 'data' must be substituted for adjacency list edges
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

    let duration = end.duration_since(beg);
    // convert duration to microseconds
    let time = duration.as_micros() as u64; // Ensure it fits into u64
    // assuming `world` is the communicator
    let universe = mpi::initialize().unwrap();
    let world = universe.world();
    // perform all-reduce operation to find maximum time across all processes
    let global_time = world.all_reduce_into(&time, SystemOperation::max());

    if world.rank() == 0 {
        println!("{}", num_buckets);
        println!("{}", global_time as f64 / 1000.0);
    
    } else {
        let path = String::new();
        let mut degree = 0;
    
        // Placeholder for the logic to populate `map` with graph data
        // Assuming `generate_rmat_graph` and `map` are appropriately defined and accessible here
        let beg = Instant::now();
        generate_rmat_graph(&world, &graph.lock().unwrap(), 8, &mut max_weight);
        let end = Instant::now();
    
        // Assuming `distributed_map` is a suitable structure to iterate over for degree calculation
        // and it's accessible here
        distributed_map.iter().for_each(|(_k, v)| {
            let edges_len = v.edges.len(); // Assuming `v` has an `edges` field
            if edges_len > degree {
                degree = edges_len;
            }
        });
    
        // Perform an all-reduce operation to find the maximum degree across all processes
        let max_degree = world.all_reduce_into(&degree, SystemOperation::max());
    
        let delta = 1.0 / max_degree as f32;
        num_buckets = (max_weight / delta).ceil() as usize + 1;
    }

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
    world.exec_am_all(move || {
        // Assuming `map` is accessible and has a method to asynchronously visit and modify elements
        distributed_map.async_visit(0, |source_info| {
            source_info.tent = 0; // Modify the source_info as needed
        });
    });

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




use serde::{Deserialize, Serialize};
use std::future::Future;
use std::sync::Arc;

#[derive(Clone)]
pub struct DistHashMap {
    num_pes: usize,
    team: Arc<LamellarTeam>,
    data: LocalRwDarc<HashMap<i32, i32>>, //unforunately we can't use generics here due to constraints imposed by ActiveMessages
}

impl DistHashMap {
    pub fn new(world: &LamellarWorld,  num_pes: usize) -> Self {
        let team = world.team();
        DistHashMap {
            num_pes,
            team: team.clone(),
            data: LocalRwDarc::new(team, HashMap::new()).unwrap(),
        }
    }

    fn get_key_pe(&self, k: i32) -> usize {
        k as usize % self.num_pes
    }
    pub fn add(&self, k: i32, v: i32) -> impl Future {
        let dest_pe = self.get_key_pe(k);
        self.team.exec_am_pe(
            dest_pe,
            DistHashMapOp {
                data: self.data.clone(),
                cmd: DistCmd::Add(k, v),
            },
        )
    }

    pub fn get(&self, k: i32) -> impl Future<Output = DistCmdResult> {
        let dest_pe = self.get_key_pe(k);
        self.team.exec_am_pe(
            dest_pe,
            DistHashMapOp {
                data: self.data.clone(),
                cmd: DistCmd::Get(k),
            },
        )
    }
}

// this is one way we can implement commands for the distributed hashmap
// a maybe more efficient way to do this would be to create an individual
// active message for each command
// #[AmData(Debug, Clone)] eventually we will be able to do this... instead  derive serialize and deserialize directly with serde
#[derive(Debug, Clone, Serialize, Deserialize)]
enum DistCmd {
    Add(i32, i32),
    Get(i32),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DistCmdResult {
    Add,
    Get(i32),
}

#[AmData(Debug, Clone)]
struct DistHashMapOp {
    data: LocalRwDarc<HashMap<i32, i32>>, //unforunately we can't use generics here due to constraints imposed by ActiveMessages
    cmd: DistCmd,
}

#[am]
impl LamellarAM for DistHashMapOp {
    async fn exec(self) -> DistCmdResult {
        match self.cmd {
            DistCmd::Add(k, v) => {
                self.data.write().await.insert(k, v);
                DistCmdResult::Add
            }
            DistCmd::Get(k) => {
                let data = self.data.read().await;
                let v = data.get(&k);
                println!("{}", v.unwrap());
                DistCmdResult::Get(k)
            }
        }
    }
}

fn main() {
    let world = LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    world.barrier();
    let distributed_map = DistHashMap::new(&world, num_pes);

    for i in 0..10 {
        // we can ignore the 'unused' result here because we call 'wait_all' below, otherwise to ensure each request completed we could use 'block_on'
        distributed_map.add(i, i);
    }
    world.wait_all();
    world.barrier();
    let map_clone = distributed_map.clone();
    world.block_on(async move {
        for i in 0..10 {
            println!("{}: {:?}", i, map_clone.get(i).await);
        }
    });
 
    world.barrier();
    let local_data = world.block_on(distributed_map.data.read());
    println!(
        "[{my_pe}] local data: {:?}",
        local_data
    );
}