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

    pub fn init_dist_set(&self, k: i32) -> usize {
        k as usize % self.num_pes
    }

    pub fn add_set(&self, k: i32) -> impl Future<Output = DistCmdResult> {
        let dest_pe = k as usize % self.num_pes;
        self.team.exec_am_pe(
            dest_pe,
            DistHashSetOp {
                data: self.data.clone(),
                cmd: DistCmd::Add(k),
            },
        )
    }

    pub async fn empty_set(&self) {
        for dest_pe in 0..self.num_pes {
            self.team.exec_am_pe(
                dest_pe,
                DistHashSetOp {
                    data: self.data.clone(),
                    cmd: DistCmd::Empty(),
                },
            ).await;
        } self.team.wait_all();
    }
  
//    pub fn consume_set(&self, k: i32, t: f64) -> impl Future<Output = DistCmdResult> {
//         let dest_pe = self.get_key_pe(k);
//         self.team.exec_am_pe(
//             dest_pe,
//             DistHashSetOp {
//                 data: self.data.clone(),
//                 cmd: DistCmd::Consume(k, t),
//             },
//         )
//     }

    pub fn erase_set_item(&self, k: i32) -> impl Future<Output = DistCmdResult> {
        let dest_pe = self.init_dist_set(k);
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
    Erase(i32),
    Empty(),
    // Consume(i32, f64),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DistCmdResult {
    Add,
    Erase,
    Empty,
    // Consume,
}

#[AmData(Debug, Clone)]
struct DistHashSetOp {
    data: LocalRwDarc<HashSet<i32>>, //unforunately we can't use generics here due to constraints imposed by ActiveMessages
    cmd: DistCmd,
}

#[lamellar::am]
impl LamellarAM for DistHashSetOp {
    async fn exec(self) -> DistCmdResult {
        match &self.cmd {
            DistCmd::Add(k) => {
                self.data.write().await.insert(*k);
                DistCmdResult::Add
            }
            DistCmd::Erase(k) => {
                self.data.write().await.remove(k);
                DistCmdResult::Erase
            }
            DistCmd::Empty() => {
                self.data.write().await.clear();
                DistCmdResult::Empty
            }

            // DistCmd::Consume(k, tent_val) => {
            //     let mut data = self.data.write().await;
            //     if let Some(adj_list) = data.get(&k).cloned() {
            //         if tent_val != adj_list.tent {
            //             // should update tent value?
            //             self.data.write().await.remove(k);
            //         }
            //         DistCmdResult::Consume
            //     } else {
            //         self.data.write().await.remove(k);
            //         DistCmdResult::Consume 
            //     }
            // }
        }
    }
}