use lamellar::active_messaging::prelude::*;

#[AmData(Debug, Clone)] // `AmData` is a macro used in place of `derive`
struct HelloWorld {
    //the "input data" we are sending with our active message
    my_pe: usize, // "pe" is processing element == a node
}

#[lamellar::am] // at a highlevel registers this LamellarAM implemenatation with the runtime for remote execution
impl LamellarAM for HelloWorld {
    async fn exec(&self) {
        println!(
            "Hello pe {:?} of {:?}, I'm pe {:?}",
            lamellar::current_pe,
            lamellar::num_pes,
            self.my_pe
        );
    }
}

pub fn main() {
    let mut world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    let am = HelloWorld { my_pe: my_pe };
    for pe in 0..num_pes {
        world.exec_am_pe(pe, am.clone()); // explicitly launch on each PE
    }
    world.wait_all(); // wait for all active messages to finish
    world.barrier(); // synchronize with other PEs
    let request = world.exec_am_all(am.clone()); //also possible to execute on every PE with a single call
    world.block_on(request); //both exec_am_all and exec_am_pe return futures that can be used to wait for completion and access any returned result
}
