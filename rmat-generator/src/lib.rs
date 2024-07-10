use rand::{distributions::Distribution, thread_rng};

#[derive(Copy, Clone, Debug)]
pub struct Edge(pub usize, pub usize);

#[derive(Clone, Debug)]
pub struct RMATGraph {
    pub order: usize,
    fuzz: f64,
    partition: [f64; 4],
    directed: bool,
    pub edges: Vec<Edge>,
}

impl RMATGraph {
    pub fn new(order: usize, fuzz: f64, partition: [f64; 4], directed: bool) -> Self {
        Self {order, fuzz, partition, directed, edges: vec![]}
    }

    pub fn generate_edge(&mut self) -> Edge {
        let mut a = self.partition[0];
        let mut b = self.partition[1];
        let mut c = self.partition[2];
        let mut d = self.partition[3];
        let mut step = 1 << (self.order - 1);
        let mut u = 0;
        let mut v = 0;
        let distribution = rand::distributions::Uniform::new_inclusive(0.0, 1.0);
        for _ in 0..self.order {
            // add noise at each step, then normalize
            a *= 1. - self.fuzz + 2. * self.fuzz * distribution.sample(&mut thread_rng());
            b *= 1. - self.fuzz + 2. * self.fuzz * distribution.sample(&mut thread_rng());
            c *= 1. - self.fuzz + 2. * self.fuzz * distribution.sample(&mut thread_rng());
            d *= 1. - self.fuzz + 2. * self.fuzz * distribution.sample(&mut thread_rng());

            let s = a + b + c + d;
            a /= s;
            b /= s;
            c /= s;
            // ensure the probabilities add to 1
            d = 1. - a - b - c;

            let p = distribution.sample(&mut thread_rng());
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
        self.edges.push(Edge(u, v));
        if !self.directed {
            self.edges.push(Edge(v, u));
            return Edge(usize::min(u, v), usize::max(u, v));
        }
        Edge(u, v)
    }
}