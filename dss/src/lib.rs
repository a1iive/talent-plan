#![feature(integer_atomics)]
#![deny(clippy::all)]
// You need to remove these two allows.
#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(non_snake_case)]

#[allow(unused_imports)]
#[macro_use]
extern crate log;
#[macro_use]
extern crate prost_derive;

mod kvraft;
mod raft;
