use clap::Parser;
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

#[derive(Parser, Debug)]
#[command(name="DistRMAT")]
#[command(about="Generates an RMAT graph with the requested order and number of edges", long_about=None)]
struct Cli {
    order: usize,
    edge_factor: usize,
    #[arg(short, long, default_value_t=0.1)]
    fuzz: f64,
    #[arg(short, long)]
    seed: Option<u64>,
    #[arg(short, long)]
    directed: bool,
    #[arg(short, long, num_args=4)]
    partition: Option<Vec<f64>>,
    #[arg(short, long)]
    split_output: bool,
    #[arg(short, long)]
    filename: Option<String>
}

fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();

    let cli = Cli::parse();
    let partition = cli.partition.unwrap_or(vec![0.57, 0.19, 0.19, 0.05]);
    let partition = [partition[0], partition[1], partition[2], partition[3]];
    let filename = cli.filename.unwrap_or_else(|| format!("scale{}_ef{}", cli.order, cli.edge_factor));

    let gen = DistRMAT::<SmallRng>::new(&world, cli.order, cli.fuzz, cli.seed, 2_usize.pow(cli.order as u32) * cli.edge_factor, partition, cli.directed);
    let gen = Darc::new(&world, gen).unwrap();
    if cli.split_output {
        let filename = filename + &format!("_{}", my_pe);
        let local_edges: Vec<_> = gen.iter().collect();
        let f = std::fs::File::options().write(true).truncate(true).create(true).open(filename + ".json").unwrap();
        let mut writer = std::io::BufWriter::new(f);
        serde_json::to_writer_pretty(&mut writer, &local_edges).unwrap();
        writer.flush().unwrap();
    } else {
        if my_pe == 0 {
            let start = std::time::Instant::now();
            let edges: Vec<_> = world.block_on(world.exec_am_all(Yoink {gen})).into_iter().flatten().collect();
            println!("Found {} total edges at pe {}", edges.len(), my_pe);
            println!("Took {:?}", start.elapsed());
            let f = std::fs::File::options().write(true).truncate(true).create(true).open("graph.json").unwrap();
            let mut writer = std::io::BufWriter::new(f);
            serde_json::to_writer_pretty(&mut writer, &edges).unwrap();
            writer.flush().unwrap();
        }
    }
}