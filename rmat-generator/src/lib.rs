use rand::{distributions::Distribution, thread_rng, RngCore, SeedableRng};
use dist_structs::Edge;

pub trait CloneSeedableRng: Clone + RngCore + SeedableRng {}
impl<T: Clone + RngCore + SeedableRng> CloneSeedableRng for T {}

#[derive(Clone, Debug)]
pub struct RMATGraph <T> {
    pub order: usize,
    fuzz: f64,
    seed: u64,
    gen: T,
    edge_count: usize,
    partition: [f64; 4],
    directed: bool,
}

impl<T> RMATGraph <T> where T: CloneSeedableRng{
    pub fn new(order: usize, fuzz: f64, seed: Option<u64>, edge_count: usize, partition: [f64; 4], directed: bool) -> Self {
        // if the seed is not given, choose a random one
        let seed = seed.unwrap_or_else(|| thread_rng().next_u64());
        Self {order, fuzz, seed, gen: T::seed_from_u64(seed), edge_count, partition, directed}
    }

    pub fn reset_gen(&mut self) {
        // set the generator back to the starting state
        self.gen = T::seed_from_u64(self.seed);
    }

    pub fn generate_edge(&mut self) -> Edge {
        // cribbed from ygm rmat generator
        let mut a = self.partition[0];
        let mut b = self.partition[1];
        let mut c = self.partition[2];
        let mut d = self.partition[3];
        let mut step = 1 << (self.order - 1);
        let mut u = 0;
        let mut v = 0;
        let distribution = rand::distributions::Uniform::new(0.0, 1.0);
        for _ in 0..self.order {
            let p = distribution.sample(&mut self.gen);
            if p < a {}
            else if p < a + b {
                u += step;
            } else if p < a + b + c {
                v += step;
            } else {
                u += step;
                v += step;
            }
            step >>= 1;

            // add noise at each step, then normalize
            a *= 1. - self.fuzz + 2. * self.fuzz * distribution.sample(&mut self.gen);
            b *= 1. - self.fuzz + 2. * self.fuzz * distribution.sample(&mut self.gen);
            c *= 1. - self.fuzz + 2. * self.fuzz * distribution.sample(&mut self.gen);
            d *= 1. - self.fuzz + 2. * self.fuzz * distribution.sample(&mut self.gen);
            let s = a + b + c + d;
            a /= s;
            b /= s;
            c /= s;
            // ensure the probabilities add to 1
            d = 1. - a - b - c;
        }
        if !self.directed {
            return Edge(usize::min(u, v).try_into().unwrap(), usize::max(u, v).try_into().unwrap());
        }
        Edge(u.try_into().unwrap(), v.try_into().unwrap())
    }

    pub fn iter(&self) -> RMATIter<T> {
        // return a non-consuming iterator over the edges this graph will generate
        let mut self_clone = self.clone();
        self_clone.reset_gen();
        RMATIter {graph: self_clone, next_edge: None, count: 0}
    }
}

impl<T> IntoIterator for RMATGraph<T> where T: CloneSeedableRng {
    type Item = Edge;
    type IntoIter = RMATIter<T>;
    fn into_iter(mut self) -> Self::IntoIter {
        self.reset_gen();
        RMATIter {graph: self, next_edge: None, count: 0}
    }
}

pub struct RMATIter<T> {
    graph: RMATGraph<T>,
    next_edge: Option<Edge>,
    count: usize
}

impl<T: CloneSeedableRng> Iterator for RMATIter <T> {
    type Item = Edge;
    fn next(&mut self) -> Option<Self::Item> {
        if self.count >= self.graph.edge_count {
            return None;
        }
        self.count += 1;
        if let Some(e) = self.next_edge {
            self.next_edge = None;
            Some(e)
        } else if self.graph.directed {
            Some(self.graph.generate_edge())
        } else {
            let Edge(u, v) = self.graph.generate_edge();
            self.next_edge = Some(Edge(v, u));
            Some(Edge(u, v))
        }
    }
}