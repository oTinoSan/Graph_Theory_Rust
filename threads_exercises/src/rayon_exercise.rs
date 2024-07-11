use rayon::prelude::*;
use std::sync::mpsc;

pub fn rayon_tutorial() {
    //par_iter method to parallize a loop
    let data = vec![1, 2, 3, 4, 5];
    data.par_iter().for_each(|x| println!("{}", x));

    // use the map method to apply an func to each element in parallel
    let result = data.par_iter().map(|x| x * 2).collect::<Vec<_>>();

    // provide parallel operations on mut data structures
    let mut data = vec![1, 2, 3, 4, 5];
    data.par_iter_mut().for_each(|x| *x *= 2);

    // use spawn method to create a new thread
    rayon::spawn(|| {
        println!("Running in a new thread!");
    });

    // using sync::mpsc to communicate between threads
    let (tx, rx) = mpsc::channel();
  
    rayon::spawn(move || {
      tx.send(1).unwrap();
    });

    println!("Received: {}", rx.recv().unwrap());

    // crossbean, Mutex, RwLock, and Atomic
}