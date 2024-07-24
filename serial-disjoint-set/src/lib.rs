use std::{cell::RefCell, collections::HashMap, hash::Hash, rc::{Rc, Weak}};

#[derive(Clone, Debug)]
struct Vertex<T> {
    value: T,
    parent: Weak<RefCell<Vertex<T>>>,
    rank: usize
}

impl<T> Vertex<T> {
    fn new_rc(value: T) -> Rc<RefCell<Self>> {
        Rc::new_cyclic(|weak| {
            RefCell::new(Self {value, parent: weak.clone(), rank: 0})
        })
    }
}

impl<T> PartialEq for Vertex<T> where T: PartialEq {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

#[derive(Clone, Debug)]
pub struct DisjointSet<T> {
    data: HashMap<T, Rc<RefCell<Vertex<T>>>>
}

impl<T> DisjointSet<T> where T: Eq + PartialEq + Hash + Copy + PartialOrd {
    pub fn new() -> Self {
        Self {data: HashMap::new()}
    }

    pub fn add(&mut self, vertex: T) {
        self.data.insert(vertex, Vertex::new_rc(vertex));
    }

    pub fn find(&self, vertex: T) -> Option<T> {
        if let Some(vertex_cell) = self.data.get(&vertex) {
            let parent = vertex_cell.borrow().parent.upgrade().unwrap().borrow().value;
            if parent == vertex {
                return Some(parent);
            } else {
                return self.find(parent);
            }
        }
        None
    }

    pub fn union_by_rank(&self, a: T, b: T) {
        if let (Some(a_root), Some(b_root)) = (self.find(a), self.find(b)) {
            let a_root_rc = self.data.get(&a_root).unwrap();
            let b_root_rc = self.data.get(&b_root).unwrap();
            let mut a_root = a_root_rc.borrow_mut();
            let mut b_root = b_root_rc.borrow_mut();
            if a_root.value != b_root.value {
                if a_root.rank > b_root.rank {
                    b_root.parent = a_root.parent.clone();
                } else if b_root.rank > a_root.rank {
                    a_root.parent = b_root.parent.clone();
                } else {
                    if a_root.value < b_root.value {
                        b_root.parent = a_root.parent.clone();
                    } else {
                        a_root.parent = b_root.parent.clone();
                    }
                }
            }
        }
    }

    pub fn interleaved_find(&self, x: T, y: T) -> bool {
        let mut x_rc = self.data.get(&x).unwrap().to_owned();
        let mut y_rc = self.data.get(&y).unwrap().to_owned();
        let mut x_rank = x_rc.borrow().rank;
        let mut y_rank = y_rc.borrow().rank;
        loop {
            if x_rank < y_rank {
                let x_parent = x_rc.borrow().parent.upgrade().unwrap();
                if x_parent == x_rc {
                    return false;
                }
                x_rc = x_parent;
            } else {
                let y_parent = y_rc.borrow().parent.upgrade().unwrap();
                if y_parent == y_rc {
                    return false;
                }
                y_rc = y_parent;
            }

            x_rank = x_rc.borrow().rank;
            y_rank = y_rc.borrow().rank;

            if x_rank == y_rank {
                if x_rc.borrow().value == y_rc.borrow().value {
                    return true
                } else {
                    return false
                }
            }
        }
    }

    pub fn union_splice(&self, a: T, b: T) {
        if let (Some(a_rc), Some(b_rc)) = (self.data.get(&a), self.data.get(&b)) {
            let mut a_rc = a_rc.clone();
            let mut b_rc = b_rc.clone();
            let mut a_parent = a_rc.borrow().parent.upgrade().unwrap();
            let mut b_parent = b_rc.borrow().parent.upgrade().unwrap();
            while a_parent != b_parent {
                if a_parent.borrow().rank < b_parent.borrow().rank {
                    a_rc.borrow_mut().parent = Rc::<RefCell<Vertex<T>>>::downgrade(&b_parent);
                    if a_rc == a_parent {
                        break;
                    }
                    a_rc = a_parent;
                    a_parent = a_rc.borrow().parent.upgrade().unwrap();
                } else if b_parent.borrow().rank < a_parent.borrow().rank{
                    b_rc.borrow_mut().parent = Rc::<RefCell<Vertex<T>>>::downgrade(&a_parent);
                    if b_rc == b_parent {
                        break
                    }
                    b_rc = b_parent;
                    b_parent = b_rc.borrow().parent.upgrade().unwrap();
                } else {
                    if a_rc != a_parent {
                        a_rc = a_parent;
                        a_parent = a_rc.borrow().parent.upgrade().unwrap();
                    } else if b_rc != b_parent {
                        b_rc = b_parent;
                        b_parent = b_rc.borrow().parent.upgrade().unwrap();
                    } else {
                        a_rc.borrow_mut().parent = Rc::<RefCell<Vertex<T>>>::downgrade(&b_rc);
                        b_rc.borrow_mut().rank += 1;
                        break;
                    }
                }
            }
        }
    }
}