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

    /// processes the local and non-local edges in two steps according to the process described
    /// in Manne-Patwary 2010
    /// 
    /// blocking call
    pub fn process_edges(&self) {
        self.team.barrier();
        self.process_local_edges();
        self.team.barrier();
        self.nonlocal_spanning_tree();
        self.team.barrier();
        self.process_nonlocal_edges();
        self.team.barrier();
    }

    /// Adds a disconnected vertex to the disjoint set
    pub fn add_new_vertex(&self, v: u64) -> impl Future<Output = ()> {
        self.vertices.store(v as usize, Vertex {value: v, parent: v, rank: 0})
    }

    pub fn get_spanning_tree(&self) -> Vec<Edge> {
        let local_trees = self.team.exec_am_all(GetEdgeVectorAM{edges: self.local_tree.clone()});
        let nonlocal_edges = self.team.exec_am_all(GetEdgeVectorAM {edges: self.spanning_edges.clone()});
        let mut spanning_tree: Vec<_> = self.team.block_on(local_trees).into_iter().flatten().collect();
        spanning_tree.extend(self.team.block_on(nonlocal_edges).into_iter().flatten());
        spanning_tree
    }

    /// blocking call
    pub fn print_vertices(&self) {
        self.vertices.print();
    }

    /// prune the non-local edges from the list of potentials
    /// 
    /// blocking call
    fn process_nonlocal_edges(&self) {
        let mut potential_edges = Vec::new();
        std::mem::swap(&mut **self.team.block_on(self.spanning_tree.write()), &mut potential_edges);
        self.team.barrier();
        let mut futures = Vec::new();
        let my_pe = self.team.my_pe();
        for edge in potential_edges.iter() {
            let a = if self.get_vertex_pe(edge.0) == my_pe {
                self.vertices.local_data().at(self.get_vertex_local_index(edge.0)).load()
            } else {
                Vertex {value: edge.0, parent: edge.0, rank: 0}
            };
            let b = if self.get_vertex_pe(edge.1) == my_pe {
                self.vertices.local_data().at(self.get_vertex_local_index(edge.1)).load()
            } else {
                Vertex {value: edge.1, parent: edge.1, rank: 0}
            };
            futures.push(self.team.exec_am_pe(self.get_vertex_pe(edge.0), FindUnionAM {edge: *edge, a, b, vertices: self.vertices.clone()}));
        }
        for (edge, future) in potential_edges.into_iter().zip(futures) {
            if self.team.block_on(future) {
                self.team.block_on(self.spanning_tree.write()).push(edge);
            }
        }
    }

    /// empty the list of non-local edges for the spanning tree, then calculate
    /// which of those edges are potentially in the global spanning tree
    fn nonlocal_spanning_tree(&self) {
        let mut spanning_tree = self.team.block_on(self.spanning_tree.write());
        spanning_tree.clear();
        let mut ghost_connections: HashMap<u64, HashSet<u64>> = HashMap::new();
        let my_pe = self.team.my_pe();
        for edge in self.team.block_on(self.spanning_edges.read()).iter() {
            if self.get_vertex_pe(edge.0) == my_pe {
                if ghost_connections.entry(edge.1).or_default().insert(self.find_local_root(edge.0)) {
                    spanning_tree.push(*edge);
                }
            } else if self.get_vertex_pe(edge.1) == my_pe {
                if ghost_connections.entry(edge.0).or_default().insert(self.find_local_root(edge.1)) {
                    spanning_tree.push(*edge);
                }
            } else {
                let mut u_conns = ghost_connections.remove(&edge.0).unwrap_or_default();
                let mut v_conns = ghost_connections.remove(&edge.1).unwrap_or_default();
                if u_conns.is_disjoint(&v_conns) {
                    u_conns = u_conns.union(&v_conns).copied().collect();
                    v_conns = u_conns.clone();
                    ghost_connections.insert(edge.0, u_conns);
                    ghost_connections.insert(edge.1, v_conns);
                    spanning_tree.push(*edge);
                }
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
    /// 
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
    /// 
    /// uses lamellar array operations, could be faster to use local data references instead?
    fn find_local_root(&self, mut vertex: u64) -> u64 {
        let mut parent = self.team.block_on(self.vertices.at(vertex as usize));
        let my_pe = self.team.my_pe();
        while self.get_vertex_pe(parent.value) == my_pe {
            vertex = parent.value;
            parent = self.team.block_on(self.vertices.at(parent.parent as usize));
            if vertex == parent.value {
                break;
            }
        }
        vertex
    }
}

/// Active message to add an edge to the graph
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

/// Active message for the Find-Union operation described in Manne-Patwari 2010
#[AmData(Clone, Debug)]
pub struct FindUnionAM {
    edge: Edge,
    a: Vertex,
    b: Vertex,
    vertices: AtomicArray<Vertex>,
}

#[am]
impl LamellarAm for FindUnionAM {
    async fn exec(self) -> bool {
        let mut r = true;

        // these operations could be performed on the local data instead of using the lamellar
        // array interface, which may provide better performance (and avoids awaiting the results)
        let mut a = find_local_root(&self.vertices, self.a).await;
        let mut b = find_local_root(&self.vertices, self.b).await;

        // loop becomes relevant if a or b change during the execution of the function (due to parallel nature of the code)
        loop {
            if a.value == b.value {
                // if a and b are the same, then the edge is not needed in the global spanning tree
                r = false;
                break;
            } else if a.parent == a.value && a.rank < b.rank && get_vertex_pe(&self.vertices, a.value) == lamellar::current_pe {
                // if a is a global root, and of lower rank than b, then b becomes the parent of a
                let new_a = Vertex {parent: b.value, .. a};
                if self.vertices.mut_local_data().at(get_vertex_local_index(&self.vertices, a.value)).compare_exchange(a, new_a).is_ok() {
                    break;
                }
            } else if b.parent == b.value && b.rank < a.rank && get_vertex_pe(&self.vertices, b.value) == lamellar::current_pe {
                // inverse of above, if b is a global root and of lower rank than a, a becomes the parent of b
                let new_b = Vertex {parent: a.value, .. b};
                if self.vertices.mut_local_data().at(get_vertex_local_index(&self.vertices, b.value)).compare_exchange(b, new_b).is_ok() {
                    break;
                }
            } else if a.parent == a.value && b.parent == b.value && get_vertex_pe(&self.vertices, a.value) == lamellar::current_pe {
                // if both a and b are global roots, and of the same rank, and a is local to the calling pe, check which of a and b has a lower value
                // if b has a lower value, swap a and b so that a has the lower value
                if b.value < a.value {
                    std::mem::swap(&mut a, &mut b);
                    // if the swapped a value is not local to the calling pe, go back through the loop
                    if get_vertex_pe(&self.vertices, a.value) != lamellar::current_pe {
                        continue;
                    }
                }
                // if the swapped value is still local, try to swap a for an updated version (with the parent being b)
                let new_a = Vertex {parent: b.value, .. a};
                if self.vertices.mut_local_data().at(get_vertex_local_index(&self.vertices, a.value)).compare_exchange(a, new_a).is_ok() {
                    // if that swap is made, increase the rank of b by 1, and potentially its parents if it has ceased to be a global root
                    let _ = self.vertices.team().exec_am_pe(get_vertex_pe(&self.vertices, b.value), IncreaseRankAM {vertices: self.vertices.clone(), v: b});
                    break;
                }
            } else {
                // this branch is reached if either of a or b is not a global root
                // if b is not a global root, swap a and b
                if b.parent != b.value {
                    std::mem::swap(&mut a, &mut b);
                }
                // perform a Find-Union operation on the vertices a and b, on the processing element owning the parent of a
                // the result here (whether the edge should be included) becomes the result of that operation
                r = self.vertices.team().exec_am_pe(get_vertex_pe(&self.vertices, a.parent), FindUnionAM {edge: self.edge, a, b, vertices: self.vertices.clone()}).await;
                break;
            }

            // moreso than above, these should likely be changed to operate on the local data of the vertices array
            a = find_local_root(&self.vertices, a).await;
            b = find_local_root(&self.vertices, b).await;
        }
        r
    }
}

/// Active message used to maintain monotonically increasing ranks in the pointer graph
#[AmData(Clone, Debug)]
pub struct IncreaseRankAM {
    vertices: AtomicArray<Vertex>,
    v: Vertex,
}

#[am]
impl LamellarAM for IncreaseRankAM {
    async fn exec(self) {
        let v_index = get_vertex_local_index(&self.vertices, self.v.value);
        let v_elem = self.vertices.local_data().at(v_index);
        let mut v = self.v;
        loop {
            let new_v = Vertex {rank: self.v.rank + 1, .. v};
            match v_elem.compare_exchange(v, new_v) {
                Ok(_) => break,
                Err(new_v) => v = new_v
            }
        }
        if v.parent != v.value {
            let _ = self.vertices.team().exec_am_pe(get_vertex_pe(&self.vertices, v.parent), IncreaseRankAM {vertices: self.vertices.clone(), v: self.vertices.at(v.parent as usize).await});
        }
    }
}

/// Active message used for path compression
#[AmData(Clone, Debug)]
pub struct PathCompressionAM {
    root: Vertex,
    target: Vertex,
    vertices: AtomicArray<Vertex>,
}

#[am]
impl LamellarAM for PathCompressionAM {
    async fn exec(self) {
        let mut target;
        loop {
            // find the local root of the target vertex
            target = find_local_root(&self.vertices, self.target).await;
            // if it is still valid for the local root to point to the new root, make that change
            if target.rank < self.root.rank {
                let updated = Vertex {parent: self.root.value, .. target};
                if self.vertices.mut_local_data().at(get_vertex_local_index(&self.vertices, target.value)).compare_exchange(target, updated).is_ok() {
                    break;
                }
            }
        }
        // if we have not reached a global root, continue to the next PE
        if target.parent != target.value {
            let _ = self.vertices.team().exec_am_pe(get_vertex_pe(&self.vertices, target.parent), PathCompressionAM {root: self.root, target, vertices: self.vertices.clone()});
        }
    }
}

#[AmData(Clone, Debug)]
pub struct GetEdgeVectorAM {
    edges: LocalRwDarc<Vec<Edge>>
}

#[am]
impl LamellarAM for GetEdgeVectorAM {
    async fn exec(self) -> Vec<Edge> {
        Vec::clone(&self.edges.team().block_on(self.edges.read()))
    }
}

fn get_vertex_pe<T>(vertices: &T, vertex: u64) -> usize where T: LamellarArray<Vertex> {
    vertices.pe_and_offset_for_global_index(vertex as usize).unwrap().0
}

fn get_vertex_local_index<T>(vertices: &T, vertex: u64) -> usize where T: LamellarArray<Vertex> {
    vertices.pe_and_offset_for_global_index(vertex as usize).unwrap().1
}


/// asynchronous function to find the local root of a given vertex
/// 
/// requires a reference to the array of vertices and the vertex to find the root of
/// 
/// if the parent of vertex is not local to the calling pe, will return the original vertex unchanged
async fn find_local_root<T>(vertices: &T, mut vertex: Vertex) -> Vertex where T: LamellarArray<Vertex> + LamellarArrayGet<Vertex> {
    let my_pe = vertices.team_rt().my_pe();
    while get_vertex_pe(vertices, vertex.parent) == my_pe {
        vertex = vertices.at(vertex.parent as usize).await;
        if vertex.value == vertex.parent {
            break;
        }
    }
    vertex
}