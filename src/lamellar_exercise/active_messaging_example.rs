use lamellar::active_messaging::prelude::*;

#[AmData(Debug,Clone)]
struct HelloWorld {
   original_pe: usize, //this will contain the ID of the PE this data originated from
}

#[lamellar::am]
 impl LamellarAM for HelloWorld {
     async fn exec(self) {
         println!(
             "Hello World, I'm from PE {:?}",
             self.original_pe,
         );
     }
 }

 pub fn message_launch (){
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    //Send a Hello World Active Message to all pes
    let request = world.exec_am_all(
        HelloWorld {
            original_pe: my_pe,
        }
    );
    //wait for the request to complete
    world.block_on(request);
}