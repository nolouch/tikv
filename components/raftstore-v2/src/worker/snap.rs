use std::{
    fmt,
    time::Duration,
};
use engine_traits::KvEngine;
use slog::KV;
use tikv_util::worker::{Runnable, RunnableWithTimer};
use yatp::{
    ThreadPool,
    task::future::TaskCell,
};
/// Region snapshot related task
#[derive(Debug)]
pub enum SnapshotTask<S> {
    Gen { 
        region_id: u64,
        kv_snap: S,
    },
}

impl<S> fmt::Display for SnapshotTask<S> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            SnapshotTask::Gen { region_id, .. } => write!(f, "Snapshot gen for {}", region_id),
        }
    }
}


pub struct Runner<EK: KvEngine>{
    ctx: SnapContext<EK>,
  //  pool: ThreadPool<TaskCell>,
}

impl<EK: KvEngine> Runner<EK> {
    pub fn new(
        engine: EK,
    ) -> Self {
        Self{
            ctx: SnapContext { engine }
        }
    }
}


impl<EK: KvEngine> Runnable for Runner<EK> {
    type Task = SnapshotTask<EK::Snapshot>;
    fn run(&mut self, task: Self::Task) {
        unimplemented!()
    }
    fn shutdown(&mut self) {
        unimplemented!()
    }
}



impl<EK: KvEngine> RunnableWithTimer for Runner<EK> {
    fn on_timeout(&mut self){
        unimplemented!()
    }
    fn get_interval(&self) -> Duration{
        unimplemented!()
    }
}


#[derive(Clone)]
struct SnapContext<EK>
where
    EK: KvEngine,
{
    engine: EK,
}