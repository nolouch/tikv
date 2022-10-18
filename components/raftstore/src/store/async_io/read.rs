// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt,
    marker::PhantomData,
    sync::{atomic::AtomicBool, mpsc::SyncSender, Arc},
};

use engine_traits::{KvEngine, RaftEngine};
use fail::fail_point;
use file_system::{IoType, WithIoType};
use kvproto::raft_serverpb::RegionLocalState;
use raft::{eraftpb::Snapshot as RaftSnapshot, GetEntriesContext};
use tikv_util::{box_try, info, time::Instant, worker::Runnable};

use crate::store::{RaftlogFetchResult, SnapManager, MAX_INIT_ENTRY_COUNT};

pub enum ReadTask<EK> {
    PeerStorage {
        region_id: u64,
        context: GetEntriesContext,
        low: u64,
        high: u64,
        max_size: usize,
        tried_cnt: usize,
        term: u64,
    },

    // GenTabletSnapshot is used to generate tablet snapshot.
    GenTabletSnapshot {
        region_id: u64,
        tablet: EK,
        region_state: RegionLocalState,
        last_applied_term: u64,
        last_applied_index: u64,
        canceled: Arc<AtomicBool>,
        notifier: SyncSender<RaftSnapshot>,
        for_balance: bool,
    },
}

impl<EK> fmt::Display for ReadTask<EK> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReadTask::PeerStorage {
                region_id,
                context,
                low,
                high,
                max_size,
                tried_cnt,
                term,
            } => write!(
                f,
                "Fetch Raft Logs [region: {}, low: {}, high: {}, max_size: {}] for sending with context {:?}, tried: {}, term: {}",
                region_id, low, high, max_size, context, tried_cnt, term,
            ),
            ReadTask::GenTabletSnapshot { region_id, .. } => {
                write!(f, "Snapshot gen for {}", region_id)
            }
        }
    }
}

#[derive(Debug)]
pub struct FetchedLogs {
    pub context: GetEntriesContext,
    pub logs: Box<RaftlogFetchResult>,
}

/// A router for receiving fetched result.
pub trait LogFetchedNotifier: Send {
    fn notify(&self, region_id: u64, fetched: FetchedLogs);
}

pub struct ReadRunner<EK, ER, N>
where
    EK: KvEngine,
    ER: RaftEngine,
    EK: KvEngine,
    N: LogFetchedNotifier,
{
    notifier: N,
    raft_engine: ER,
    phantom: PhantomData<EK>,
}

impl<EK, ER, N> ReadRunner<EK, ER, N> {
    #[inline]
    pub fn set_snap_mgr(&mut self, mgr: SnapManager) {
        self.mgr = Some(mgr);
    }

    #[inline]
    fn snap_mgr(&self) -> &SnapManager {
        self.mgr.as_ref().unwrap()
    }

    fn handle_gen(
        &self,
        region_id: u64,
        tablet_index: u64,
        region_state: RegionLocalState,
        last_applied_term: u64,
        last_applied_index: u64,
        canceled: Arc<AtomicBool>,
        notifier: SyncSender<RaftSnapshot>,
        for_balance: bool,
    ) {
        if canceled.load(Ordering::Relaxed) {
            info!("generate snap is canceled"; "region_id" => region_id);
            return;
        }

        let start = Instant::now();
        let _io_type_guard = WithIoType::new(if for_balance {
            IoType::LoadBalance
        } else {
            IoType::Replication
        });

        let snap = box_try!(do_tablet_snapshot(
            &self.raft_engine,
            self.mgr().clone(),
            self.tablet_factory().clone(),
            region_id,
            tablet_index,
            for_balance,
            canceled,
        ));

        // Only enable the fail point when the region id is equal to 1, which is
        // the id of bootstrapped region in tests.

        if let Err(e) = notifier.try_send(snap) {
            info!(
                "failed to notify snap result, leadership may have changed, ignore error";
                "region_id" => region_id,
                "err" => %e,
            );
        }
    }
}

impl<EK, ER, N> Runnable for ReadRunner<EK, ER, N>
where
    EK: KvEngine,
    ER: RaftEngine,
    EK: KvEngine,
    N: LogFetchedNotifier,
{
    fn run(&mut self, task: ReadTask<EK>) {
        match task {
            ReadTask::PeerStorage {
                region_id,
                low,
                high,
                max_size,
                context,
                tried_cnt,
                term,
            } => {
                Vec::with_capacity(std::cmp::min((high - low) as usize, MAX_INIT_ENTRY_COUNT));
                let res = self.raft_engine.fetch_entries_to(
                    region_id,
                    low,
                    high,
                    Some(max_size),
                    &mut ents,
                );

                let hit_size_limit = res
                    .as_ref()
                    .map(|c| (*c as u64) != high - low)
                    .unwrap_or(false);
                fail_point!("worker_async_fetch_raft_log");
                self.notifier.notify(
                    region_id,
                    FetchedLogs {
                        context,
                        logs: Box::new(RaftlogFetchResult {
                            ents: res.map(|_| ents).map_err(|e| e.into()),
                            low,
                            max_size: max_size as u64,
                            hit_size_limit,
                            tried_cnt,
                            term,
                        }),
                    },
                );
            }
            ReadTask::GenTabletSnapshot {
                region_id,
                tablet_index,
                region_state,
                last_applied_term,
                last_applied_index,
                canceled,
                notifier,
                for_balance,
            } => self.handle_gen(
                region_id,
                tablet_index,
                region_state,
                last_applied_term,
                last_applied_index,
                canceled,
                notifier,
                for_balance,
            ),
        }
    }
}

pub fn do_tablet_snapshot<EK, ER>(
    raft_engine: &ER,
    mgr: SnapManager,
    tablet_factory: Arc<dyn TabletFactory<EK>>,
    region_id: u64,
    tablet_suffix: u64,
    for_balance: bool,
    canceled: Arc<AtomicBool>,
) -> raft::Result<Snapshot>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    debug!(
        "begin to generate a snapshot";
        "region_id" => region_id,
    );

    // TODO: new way to do flow control.
    if mgr.stats().sending_count >= 32 {
        canceled.store(true, Ordering::Relaxed);
        return Err(storage_error(format!(
            "{} too many sending snapshots",
            region_id
        )));
    }

    info!(
        "generating snapshot";
        "region_id" => region_id,
        "tablet_suffix" => tablet_suffix,
    );

    let path = mgr.get_temp_path_for_build(region_id);
    defer!({
        if path.exists() {
            fs::remove_dir_all(&path).unwrap();
        }
    });
    match tablet_factory.open_tablet(region_id, Some(tablet_suffix), OpenOptions::default()) {
        // TODO: flush is not necessary if atomic flush is enabled.
        Ok(tablet) => {
            println!("path {:?}", &path.as_path().to_str());
            tablet.checkpoint_to(slice::from_ref(&path), 0).unwrap()
        }
        Err(e) => {
            return Err(storage_error(format!(
                "{} {} don't exist anymore, error {}",
                region_id, tablet_suffix, e
            )));
        }
    }
    let mut tablet = match tablet_factory.open_tablet_raw(
        Path::new(&path),
        region_id,
        tablet_suffix,
        OpenOptions::default().set_read_only(true),
    ) {
        Ok(t) => t,
        Err(e) => {
            return Err(storage_error(format!(
                "failed to open tablet at {}, error {}",
                path.display(),
                e
            )));
        }
    };

    let msg = tablet
        .get_msg_cf(CF_RAFT, &keys::apply_state_key(region_id))
        .map_err(into_other::<_, raft::Error>)?;
    let apply_state: RaftApplyState = match msg {
        None => {
            return Err(storage_error(format!(
                "could not load raft state of region {}",
                region_id
            )));
        }
        Some(state) => state,
    };

    let apply_index = apply_state.get_applied_index();
    let apply_term = if apply_index == apply_state.get_truncated_state().get_index() {
        apply_state.get_truncated_state().get_term()
    } else {
        match raft_engine.get_entry(region_id, apply_index) {
            Ok(Some(e)) => e.term,
            Ok(None) => {
                return Err(storage_error(format!(
                    "could not load entry at {}",
                    apply_index
                )));
            }
            Err(e) => return Err(into_other::<_, raft::Error>(e)),
        }
    };

    let key = SnapKey::new(region_id, apply_term, apply_index);
    let final_path = mgr.get_final_name_for_build(&key);

    let reused = tablet_factory.exists_raw(&final_path);
    if reused {
        drop(tablet);
        tablet = match tablet_factory.open_tablet_raw(
            &final_path,
            region_id,
            tablet_suffix,
            OpenOptions::default().set_read_only(true),
        ) {
            Ok(t) => t,
            Err(e) => {
                return Err(storage_error(format!(
                    "failed to open tablet at {}, error {}",
                    final_path.display(),
                    e
                )));
            }
        };
    }

    mgr.register(key.clone(), SnapEntry::Generating);
    defer!(mgr.deregister(&key, &SnapEntry::Generating));

    let state: RegionLocalState = tablet
        .get_msg_cf(CF_RAFT, &keys::region_state_key(key.region_id))
        .and_then(|res| match res {
            None => Err(box_err!("region {} could not find region info", region_id)),
            Some(state) => Ok(state),
        })
        .map_err(into_other::<_, raft::Error>)?;

    if state.get_state() != PeerState::Normal {
        return Err(storage_error(format!(
            "snap job for {} seems stale, skip.",
            region_id
        )));
    }

    let mut snapshot = Snapshot::default();

    // Set snapshot metadata.
    snapshot.mut_metadata().set_index(key.idx);
    snapshot.mut_metadata().set_term(key.term);

    let conf_state = util::conf_state_from_region(state.get_region());
    snapshot.mut_metadata().set_conf_state(conf_state);

    // Set snapshot data.
    let mut snap_data = RaftSnapshotData::default();
    snap_data.set_region(state.get_region().clone());
    snap_data.mut_meta().set_for_balance(for_balance);
    let v = snap_data.write_to_bytes()?;
    snapshot.set_data(v.into());

    drop(tablet);
    if !reused {
        fs::rename(&path, &final_path).unwrap();
    }

    Ok(snapshot)
}
