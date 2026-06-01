#![allow(dead_code)]
#![allow(unused_imports)]

// V1 contracts intentionally include protocol states not all exercised by the MVP benchmark.
mod bench;
mod cli;
mod contracts;
mod model;
mod repo;
mod runtime;
mod tools;
mod util;

#[cfg(test)]
mod tests;

fn main() {
    cli::main_entry();
}
