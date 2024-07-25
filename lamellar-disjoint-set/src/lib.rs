use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;
use lamellar::array::prelude::*;
use lamellar::active_messaging::prelude::*;
use lamellar::darc::prelude::*;
use dist_structs::{Edge, Vertex};

/// Distributed disjoint set data structure with lamellar backend
pub struct DisjointSet {
    team: Arc<LamellarTeam>,
    num_pes: usize,
    local_edges: LocalRwDarc<Vec<Edge>>,
    spanning_edges: LocalRwDarc<Vec<Edge>>,
    vertices: AtomicArray<Vertex>,
    ghost_vertices: LocalRwDarc<HashMap<u64, Vertex>>,
    local_tree: LocalRwDarc<Vec<Edge>>,
    spanning_tree: LocalRwDarc<Vec<Edge>>,
}

impl DisjointSet {
    /// Create a new disjoint set, this is a blocking call for all pes involved
    /// max_vertices is the maximum that this disjoint set can contain, but vertices must still be added individually as well
    pub fn new(world: &LamellarWorld, max_vertices: usize) -> Self {
        let vertices = AtomicArray::new(world, max_vertices, Distribution::Cyclic);
        let local_edges = LocalRwDarc::new(world, Vec::new()).unwrap();
        let spanning_edges = LocalRwDarc::new(world, Vec::new()).unwrap();
        let ghost_vertices = LocalRwDarc::new(world, HashMap::new()).unwrap();
        let local_tree = LocalRwDarc::new(world, Vec::new()).unwrap();
        let spanning_tree = LocalRwDarc::new(world, Vec::new()).unwrap();
        Self {
            team: world.team(),
            num_pes: world.num_pes(),
            local_edges,
            spanning_edges,
            vertices,
            ghost_vertices,
            local_tree,
            spanning_tree
        }
    }

    /// Create a new disjoint set with a given number of vertices already allocated
    /// Assumes that desired vertices are in the range 0..current_vertices
    pub fn new_with_vertices(world: &LamellarWorld, max_vertices: usize, current_vertices: usize) -> Self {
        let temp = Self::new(world, max_vertices);
        if world.my_pe() == 0 {
            for i in 0..current_vertices as u64 {
                let _ = temp.add_new_vertex(i);
            }
        }
        world.wait_all();
        world.barrier();
        temp
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

    /// Adds an edge to the disjoint set, but does not automatically perform
    /// the associated union operation
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
        self.vertices.store(v as usize, Vertex {value: v, parent: v, rank: 0})
    }

    fn process_spanning_edges(&self) {
        let mut spanning_tree = self.team.block_on(self.spanning_tree.write());
        spanning_tree.clear();
        let mut ghost_connections: HashMap<u64, HashSet<u64>> = HashMap::new();
        let my_pe = self.team.my_pe();
        for edge in self.team.block_on(self.spanning_edges.read()).iter() {
            if self.get_vertex_pe(edge.0) == my_pe {
                if ghost_connections.entry(edge.1).or_insert_with(HashSet::new).insert(self.find_local_root(edge.0)) {
                    spanning_tree.push(*edge);
                }
            } else if self.get_vertex_pe(edge.1) == my_pe {
                if ghost_connections.entry(edge.0).or_insert_with(HashSet::new).insert(self.find_local_root(edge.1)) {
                    spanning_tree.push(*edge);
                }
            } else {

            }
        }
    }

    /// perform union operations for all edges local to this processing element
    /// adds required edges to the local spanning forest
    fn process_local_edges(&self) {
        let mut local_tree = self.team.block_on(self.local_tree.write());
        for edge in self.team.block_on(self.local_edges.read()).iter() {
            if self.local_union_splice(edge) {
                local_tree.push(*edge);
            }
        }
    }

    /// perform a union and splice for a given edge, returns true if a root union was performed
    /// edge must be local or there will be problems
    fn local_union_splice(&self, edge: &Edge) -> bool {
        let local_vertices = self.vertices.mut_local_data();
        let mut u = local_vertices.at(self.get_vertex_local_index(edge.0)).load();
        let mut u_parent = local_vertices.at(self.get_vertex_local_index(u.parent)).load();
        let mut v = local_vertices.at(self.get_vertex_local_index(edge.1)).load();
        let mut v_parent = local_vertices.at(self.get_vertex_local_index(v.parent)).load();
        loop {
            if u_parent.rank < v_parent.rank {
                u.parent = v.parent;
                local_vertices.at(self.get_vertex_local_index(u.value)).store(u);
                if u_parent.value == u.value {
                    return true;
                }
                u = u_parent;
                u_parent = local_vertices.at(self.get_vertex_local_index(u.parent)).load();
            } else if v_parent.rank < u_parent.rank {
                v.parent = u.parent;
                local_vertices.at(self.get_vertex_local_index(v.value)).store(v);
                if v_parent.value == v.value {
                    return true;
                }
                v = v_parent;
                v_parent = local_vertices.at(self.get_vertex_local_index(v.parent)).load();
            } else {
                if u_parent.value == v_parent.value {
                    return false;
                }
                if u.value != u_parent.value {
                    u = u_parent;
                    u_parent = local_vertices.at(self.get_vertex_local_index(u.parent)).load();
                } else if v.value != v_parent.value {
                    v = v_parent;
                    v_parent = local_vertices.at(self.get_vertex_local_index(v.parent)).load();
                } else {
                    if u.value > v.value {
                        u.parent = v.value;
                        local_vertices.at(self.get_vertex_local_index(u.value)).store(u);
                    } else {
                        v.parent = u.value;
                        local_vertices.at(self.get_vertex_local_index(v.value)).store(v);
                    }
                    return true;
                }
            }
        }
    }

    /// finds the local root of a given vertex
    /// uses lamellar array operations, could be faster to use local data references instead?
    fn find_local_root(&self, mut vertex: u64) -> u64 {
        let mut parent = self.team.block_on(self.vertices.at(vertex as usize));
        while self.get_vertex_pe(parent.value) == self.team.my_pe() {
            vertex = parent.value;
            parent = self.team.block_on(self.vertices.at(parent.parent as usize));
        }
        vertex
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
            ghost_writer.insert(*ghost, Vertex {value: *ghost, parent: *ghost, rank: 0});
        }
        drop(ghost_writer);
        self.edges.write().await.push(self.edge);
    }
}