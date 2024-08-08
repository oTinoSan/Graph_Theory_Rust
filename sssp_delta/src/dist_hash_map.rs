use lamellar::active_messaging::prelude::*;
use lamellar::darc::prelude::*;
use serde::{Deserialize, Serialize};
use lamellar::LamellarTeam;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

// edge: (vertex, weight)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AdjList {
    pub edges: Vec<(usize, f64)>,
    pub tent: f32,
}

impl std::fmt::Display for AdjList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AdjList {{ vertex, weight: {:?}, tent: {} }}", self.edges, self.tent)
    }
}

// data: <node, (tent, (vertex, weight)>
#[derive(Clone, Debug)]
pub struct DistHashMap {
    num_pes: usize,
    team: Arc<LamellarTeam>,
    pub data: LocalRwDarc<HashMap<i32, AdjList>>, //unforunately we can't use generics here due to constraints imposed by ActiveMessages
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

    pub fn add(&self, k: i32, v: AdjList) -> impl Future<Output = DistCmdResult> {
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

    pub fn visit(&self, k: i32, v: f32) -> impl Future<Output = DistCmdResult> {
        let dest_pe = self.get_key_pe(k);
        self.team.exec_am_pe(
            dest_pe,
            DistHashMapOp {
                data: self.data.clone(),
                cmd: DistCmd::Visit(k, v),
            },
        )
    }

    // k = node, v = potential_tent, d = delta
    pub fn relax_requests(&self, k: i32, v: f32, d: f32) -> impl Future<Output = DistCmdResult> {
        let dest_pe = self.get_key_pe(k);
        self.team.exec_am_pe(
            dest_pe,
            DistHashMapOp {
                data: self.data.clone(),
                cmd: DistCmd::Relax(k, v, d),
            },
        )
    }

    // k = node, v = new_tent
    pub fn compare_tent(&self, k: i32, v: f32) -> impl Future<Output = DistCmdResult> {
        let dest_pe = self.get_key_pe(k);
        self.team.exec_am_pe(
            dest_pe,
            DistHashMapOp {
                data: self.data.clone(),
                cmd: DistCmd::Compare(k, v)
            },
        )
    }  

    pub fn all_reduce_max(&self, k: i32, v: Vec<i32>) -> impl Future<Output = DistCmdResult> {
        let dest_pe = self.get_key_pe(k);
        self.team.exec_am_pe(
            dest_pe,
            DistHashMapOp {
                data: self.data.clone(),
                cmd: DistCmd::ReduceMax(k, v)
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
    Add(i32, AdjList),
    Get(i32),
    Visit(i32, f32),
    Relax(i32, f32, f32),
    Compare(i32, f32),
    ReduceMax(i32, Vec<i32>)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DistCmdResult {
    Add,
    Get(AdjList),
    Visit,
    Relax(i32),
    Compare(bool),
    ReduceMax(usize)
}

#[AmData(Debug, Clone)]
struct DistHashMapOp {
    data: LocalRwDarc<HashMap<i32, AdjList>>, //unforunately we can't use generics here due to constraints imposed by ActiveMessages
    cmd: DistCmd,
}

#[lamellar::am]
impl LamellarAM for DistHashMapOp {
    async fn exec(self) -> DistCmdResult {
        match &self.cmd {
            DistCmd::Add(k, v) => {
                self.data.write().await.insert(*k, v.clone());
                DistCmdResult::Add
            }
            DistCmd::Get(k) => {
                let data = self.data.read().await;
                let v = data.get(&k).cloned().unwrap();
                DistCmdResult::Get(v)
            }
            DistCmd::Visit(k, new_tent) => {
                let mut data = self.data.write().await;
                let adj_list = data.get_mut(&k);
                adj_list.unwrap().tent = *new_tent;
                DistCmdResult::Visit
            }
            DistCmd::Relax(k, potential_tent, delta) => {
                let mut data = self.data.write().await;
                let current_tent = data.get_mut(&k).unwrap().tent;
                if potential_tent < &current_tent {
                
                    let idx = (current_tent as f64 / *delta as f64).floor() as i32;
                    DistCmdResult::Relax(idx)
                } else {
                    DistCmdResult::Relax(*k)
                }
            }

            DistCmd::Compare(k, new_tent) => {
                let data = self.data.read().await;
                let v = data.get(&k).cloned().unwrap();
                let current_tent = v.tent;
                if current_tent != *new_tent {
                    DistCmdResult::Compare(true)
                } else {
                    DistCmdResult::Compare(false)
                }
            }

            DistCmd::ReduceMax(k, degree_vec) => {
                let data = self.data.read().await;
                let v = data.get(&k).cloned().unwrap();
                let degree = v.edges.len();
                DistCmdResult::ReduceMax(degree)
            }
        }
    }
}

        
// fn main() {
//     let world = lamellar::LamellarWorldBuilder::new().build();
//     let my_pe = world.my_pe();
//     let num_pes = world.num_pes();
//     world.barrier();
//     let distributed_map = DistHashMap::new(&world, num_pes);
//     let adj_list = AdjList {
//         edges: vec![(1, 0.5), (2, 1.2), (3, 0.8)],
//         tent: 2.5,
//     };

//     for i in 0..10 {
//         // we can ignore the 'unused' result here because we call 'wait_all' below, otherwise to ensure each request completed we could use 'block_on'
//         let _ = distributed_map.add(i, adj_list.clone());
//     };


//     world.wait_all();
//     world.barrier();
//     let map_clone = distributed_map.clone();
//     world.block_on(async move {
//         for i in 0..10 {
//             println!("{}: {:?}", i, map_clone.get(i).await);
//         }
//     });


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

// }