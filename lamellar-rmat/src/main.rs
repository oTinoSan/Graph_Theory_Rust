use lamellar_rmat::DistRMAT;
use rand::rngs::SmallRng;
use lamellar::{darc::Darc, ActiveMessaging};
use std::io::prelude::*;

#[lamellar::AmData(Clone)]
struct Yoink {
    gen: Darc<DistRMAT<SmallRng>>
}

#[lamellar::am]
impl LamellarAm for Yoink {
    async fn exec(&self) -> Vec<dist_structs::Edge> {
        self.gen.iter().collect::<Vec<_>>()
    }
}


fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();

    let gen = DistRMAT::<SmallRng>::new(&world, 18, 0.1, Some(0), 7000000, [0.57, 0.19, 0.19, 0.05], false);
    let gen = Darc::new(&world, gen).unwrap();
    if my_pe == 0 {
        let start = std::time::Instant::now();
        let edges: Vec<_> = world.block_on(world.exec_am_all(Yoink {gen})).into_iter().flatten().collect();
        println!("Found {} total edges at pe {}", edges.len(), my_pe);
        println!("Took {:?}", start.elapsed());
        let f = std::fs::File::options().write(true).truncate(true).create(true).open("graph.json").unwrap();
        let mut writer = std::io::BufWriter::new(f);
        serde_json::to_writer_pretty(&mut writer, &edges).unwrap();
        writer.flush();
    }
}