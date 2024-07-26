use serial_disjoint_set::DisjointSet;

fn main() {
    let mut set = DisjointSet::new();
    for i in 0..10 {
        set.add(i);
    }

    set.union_splice(0, 1);
    set.union_splice(2, 3);
    println!("{:?}", set.find(2));
    set.union_splice(0, 3);
    println!("{:?}", set.interleaved_find(0, 8));
}