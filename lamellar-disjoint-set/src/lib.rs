use std::collections::HashMap;
use std::future::Future;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;
use lamellar::array::prelude::*;
use lamellar::active_messaging::prelude::*;
use lamellar::darc::prelude::*;
use dist_structs::{Edge, Vertex};

pub struct DisjointSet {
    team: Arc<LamellarTeam>,
    num_pes: usize,
    local_edges: LocalRwDarc<Vec<Edge>>,
    spanning_edges: LocalRwDarc<Vec<Edge>>,
    vertices: AtomicArray<Vertex>,
    ghost_vertices: LocalRwDarc<HashMap<u64, Vertex>>,
}

impl DisjointSet {
    pub fn new(world: &LamellarWorld, max_vertices: usize) -> Self {
        let vertices = AtomicArray::new(world, max_vertices, Distribution::Cyclic);
        let local_edges = LocalRwDarc::new(world, Vec::new()).unwrap();
        let spanning_edges = LocalRwDarc::new(world, Vec::new()).unwrap();
        let ghost_vertices = LocalRwDarc::new(world, HashMap::new()).unwrap();
        Self {
            team: world.team(),
            num_pes: world.num_pes(),
            local_edges,
            spanning_edges,
            vertices,
            ghost_vertices
        }
    }

    fn get_edge_pe(&self, edge: &Edge) -> usize {
        let mut hasher = DefaultHasher::new();
        edge.hash(&mut hasher);
        (hasher.finish() % self.num_pes as u64) as usize
    }

    fn get_vertex_pe(&self, vertex: u64) -> usize {
        self.vertices.pe_and_offset_for_global_index(vertex as usize).unwrap().0
    }

    fn get_vertex_local_index(&self, vertex: u64) -> usize {
        self.vertices.pe_and_offset_for_global_index(vertex as usize).unwrap().1
    }

    pub fn add_edge(&self, edge: Edge) -> impl Future<Output = ()> {
        let edge_pe = self.get_edge_pe(&edge);
        let u_pe = self.get_vertex_pe(edge.0);
        let v_pe = self.get_vertex_pe(edge.1);
        let edges = if u_pe == v_pe && u_pe == edge_pe {self.local_edges.clone()} else {self.spanning_edges.clone()};
        let mut new_ghosts = Vec::new();
        if u_pe != edge_pe {
            new_ghosts.push(edge.0);
        }
        if v_pe != edge_pe {
            new_ghosts.push(edge.1);
        }
        self.team.exec_am_pe(edge_pe, AddEdgeAM {
            edges, edge, new_ghosts, ghost_vertices: self.ghost_vertices.clone()
        })
    }

    pub fn add_new_vertex(&self, v: u64) -> impl Future<Output = ()> {
        self.vertices.store(v as usize, Vertex {parent: v, rank: 0})
    }

    fn process_local(&self) {
        self.team.barrier();

    }

    fn local_union(&self, edge: Edge) -> bool {
        
        todo!()
    }
}

#[AmData(Clone, Debug)]
pub struct AddEdgeAM {
    edges: LocalRwDarc<Vec<Edge>>,
    edge: Edge,
    new_ghosts: Vec<u64>,
    ghost_vertices: LocalRwDarc<HashMap<u64, Vertex>>,
}

#[am]
impl LamellarAM for AddEdgeAM {
    async fn exec(self) {
        let mut ghost_writer = self.ghost_vertices.write().await;
        for ghost in self.new_ghosts.iter() {
            ghost_writer.insert(*ghost, Vertex {parent: *ghost, rank: 0});
        }
        drop(ghost_writer);
        self.edges.write().await.push(self.edge);
    }
}