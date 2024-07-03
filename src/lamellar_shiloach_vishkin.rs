use lamellar::array::prelude::*;

pub fn lamellar_main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    let node_count = 10;

    let old_parents = UnsafeArray::<u64>::new(&world, node_count, Distribution::Block);
    unsafe{
        let _ = old_parents.dist_iter_mut().enumerate().for_each(|(i, x)| *x = i as u64);
    }

    old_parents.wait_all();

    world.barrier();

    let new_parents = old_parents.clone();

    unsafe {
        let _ = new_parents.dist_iter_mut().for_each(|i| *i *= 2);
    }
    new_parents.wait_all();

    new_parents.print();

    old_parents.print();
}