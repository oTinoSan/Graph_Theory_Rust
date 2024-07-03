use lamellar::array::prelude::*;

pub fn lamellar_main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    let node_count = 10;

    let old_parents = UnsafeArray::<u64>::new(&world, node_count, Distribution::Block);
    unsafe{
        let _ = old_parents.dist_iter().enumerate().map(|(i, x)| *x = i as u64);
    }
    old_parents.barrier();
    
    old_parents.print();
}