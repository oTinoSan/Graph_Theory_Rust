use rand::{distributions::Distribution, rngs::SmallRng, thread_rng, RngCore, SeedableRng};

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct Edge(pub usize, pub usize);

#[derive(Clone, Debug)]
pub struct RMATGraph {
    pub order: usize,
    fuzz: f64,
    seed: u64,
    gen: SmallRng,
    edge_count: usize,
    partition: [f64; 4],
    directed: bool,
}

impl RMATGraph {
    pub fn new(order: usize, fuzz: f64, seed: Option<u64>, edge_count: usize, partition: [f64; 4], directed: bool) -> Self {
        let seed = seed.unwrap_or_else(|| thread_rng().next_u64());
        Self {order, fuzz, seed, gen: SmallRng::seed_from_u64(seed), edge_count, partition, directed}
    }

    pub fn generate_edge(&mut self) -> Edge {
        let mut a = self.partition[0];
        let mut b = self.partition[1];
        let mut c = self.partition[2];
        let mut d = self.partition[3];
        let mut step = 1 << (self.order - 1);
        let mut u = 0;
        let mut v = 0;
        let distribution = rand::distributions::Uniform::new(0.0, 1.0);
        for _ in 0..self.order {
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
        }
        if !self.directed {
            return Edge(usize::min(u, v), usize::max(u, v));
        }
        Edge(u, v)
    }
}

impl IntoIterator for RMATGraph {
    type Item = Edge;
    type IntoIter = RMATIter;
    fn into_iter(mut self) -> Self::IntoIter {
        self.gen = SmallRng::seed_from_u64(self.seed);
        RMATIter {graph: self, next_edge: None, count: 0}
    }
}

pub struct RMATIter {
    graph: RMATGraph,
    next_edge: Option<Edge>,
    count: usize
}

impl Iterator for RMATIter {
    type Item = Edge;
    fn next(&mut self) -> Option<Self::Item> {
        if self.count > self.graph.edge_count {
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