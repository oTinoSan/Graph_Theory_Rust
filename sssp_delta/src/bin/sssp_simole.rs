use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};

#[derive(Clone, Eq, PartialEq)]
struct State {
    cost: usize,
    position: usize,
}

// The priority queue depends on `Ord`.
// Explicitly implement the trait so the queue becomes a min-heap
// instead of a max-heap.
impl Ord for State {
    fn cmp(&self, other: &Self) -> Ordering {
        other.cost.cmp(&self.cost)
    }
}

impl PartialOrd for State {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn dijkstra(graph: &HashMap<usize, Vec<(usize, usize)>>, start: usize) -> HashMap<usize, usize> {
    let mut dist: HashMap<usize, usize> = HashMap::new();
    let mut heap = BinaryHeap::new();

    // We're at `start`, with a zero cost
    dist.insert(start, 0);
    heap.push(State {
        cost: 0,
        position: start,
    });

    while let Some(State { cost, position }) = heap.pop() {
        // Important as we may have already found a better way
        if cost > *dist.get(&position).unwrap_or(&usize::MAX) {
            continue;
        }

        if let Some(edges) = graph.get(&position) {
            for &(next_position, next_cost) in edges {
                let next = State {
                    cost: cost + next_cost,
                    position: next_position,
                };

                if next.cost < *dist.get(&next_position).unwrap_or(&usize::MAX) {
                    heap.push(next);
                    dist.insert(next_position, next.cost);
                }
            }
        }
    }

    dist
}

fn main() {
    let mut graph: HashMap<usize, Vec<(usize, usize)>> = HashMap::new();

    // Example graph
    graph.insert(0, vec![(1, 2), (2, 4)]);
    graph.insert(1, vec![(2, 1), (3, 6)]);
    graph.insert(2, vec![(3, 3)]);
    graph.insert(3, vec![]);

    let start = 0;
    let distances = dijkstra(&graph, start);

    println!("Distances from node {}: {:?}", start, distances);
}
