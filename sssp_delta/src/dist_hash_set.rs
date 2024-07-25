use lamellar::active_messaging::prelude::*;
use lamellar::darc::prelude::*;
use serde::{Deserialize, Serialize};
use lamellar::LamellarTeam;
// use std::alloc::LayoutErr;
use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;

// data: <node, (tent, (vertex, weight)>
#[derive(Clone, Debug)]
pub struct DistHashSet {
    pub num_pes: usize,
    pub team: Arc<LamellarTeam>,
    pub data: LocalRwDarc<HashSet<i32>>, //unforunately we can't use generics here due to constraints imposed by ActiveMessages
} 


impl DistHashSet {
    pub fn new(world: &LamellarWorld,  num_pes: usize) -> Self {
        let team = world.team();
        DistHashSet {
            num_pes,
            team: team.clone(),
            data: LocalRwDarc::new(team, HashSet::new()).unwrap(),
        }
    }

    fn get_key_pe(&self, k: i32) -> usize {
        k as usize % self.num_pes
    }

    pub fn get_set(&self, k: i32) -> impl Future<Output = DistCmdResult> {
        let dest_pe = self.get_key_pe(k);
        self.team.exec_am_pe(
            dest_pe,
            DistHashSetOp {
                data: self.data.clone(),
                cmd: DistCmd::Get(k),
            },
        )
    }

    pub fn add_set(&self, k: i32) -> impl Future<Output = DistCmdResult> {
        let dest_pe = self.get_key_pe(k);
        self.team.exec_am_pe(
            dest_pe,
            DistHashSetOp {
                data: self.data.clone(),
                cmd: DistCmd::Add(k),
            },
        )
    }
 

   // consume_all()

    fn async_erase(&self, k: i32) -> impl Future<Output = DistCmdResult> {
        let dest_pe = self.get_key_pe(k);
        self.team.exec_am_pe(
            dest_pe,
            DistHashSetOp {
                data: self.data.clone(),
                cmd: DistCmd::Erase(k),
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
    Add(i32),
    Get(i32),
    Erase(i32),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DistCmdResult {
    Add,
    Get(i32),
    Erase,
}

#[AmData(Debug, Clone)]
struct DistHashSetOp {
    data: LocalRwDarc<HashSet<i32>>, //unforunately we can't use generics here due to constraints imposed by ActiveMessages
    cmd: DistCmd,
}

#[am]
impl LamellarAM for DistHashSetOp {
    async fn exec(self) -> DistCmdResult {
        match &self.cmd {
            DistCmd::Add(k) => {
                self.data.write().await.insert(*k);
                DistCmdResult::Add
            }
            DistCmd::Get(k) => {
                let data = self.data.read().await;
                let v = data.get(k).cloned();
                println!("{:?}", v.unwrap());
                DistCmdResult::Get(v.unwrap())
            }
            DistCmd::Erase(k) => {
                self.data.write().await.remove(k);
                DistCmdResult::Erase
            }

        }
    }
}













// fn main() {
//     let world = lamellar::LamellarWorldBuilder::new().build();
//     let my_pe = world.my_pe();
//     let num_pes = world.num_pes();
//     world.barrier();
//     let distributed_map = DistHashSet::new(&world, num_pes);
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

//     // -store the bucket_indices locally
//     // -get global reduction to get distributed min reduce and do a barrier to make sure this is finished minimum 
//     // -process all things that are part of the active bucket, iterate through everything in the hash map to see if tent(u) is in active bucket and if it is spawn edge relaxations and keep tally of this to spawn heavy relaxations later 
//     // -create a vector that is represented locally to spawn the edge relaxations



 

//     world.barrier();
//     let local_data = world.block_on(distributed_map.data.read());
//     println!(
//         "[{my_pe}] local data: {:?}",
//         local_data
//     );
//     drop(local_data);


//     let n_tent = 5.0;
//     world.block_on(async {
//         if let DistCmdResult::Visit(Some(updated_adj_list)) = distributed_map.visit(9, n_tent).await {
//             println!("{:?}", updated_adj_list);
//         }
//         else {
//             println!("Key does not exist")
//         }
//     });
// distributed_map.async_erase(9, n)
//}