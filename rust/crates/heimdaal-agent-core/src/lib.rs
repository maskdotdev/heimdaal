#![allow(dead_code)]
#![allow(unused_imports)]

// V1 contracts intentionally include protocol states not all exercised by the MVP benchmark.
pub mod cli;
pub mod reviewer;

pub(crate) mod bench;
pub(crate) mod concurrent;
pub(crate) mod contracts;
pub(crate) mod model;
pub(crate) mod repo;
pub(crate) mod runtime;
pub(crate) mod tools;
pub(crate) mod util;

#[cfg(test)]
mod tests;
