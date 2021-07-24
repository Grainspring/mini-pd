use super::storage::{self, address_key, valid_data_key};
use super::{Command, InvokeContext, Msg, RaftClient, Res, RockStorage};
use crate::{r, Config, Result};
use crossbeam::channel::{self, Receiver, RecvTimeoutError, Sender};
use futures::channel::mpsc;
use futures_timer::Delay;
use raft::eraftpb::{Entry, Message};
use raft::{prelude::*, INVALID_ID};
use rocksdb::{ReadOptions, SeekKey, Writable, WriteBatch, DB};
use slog::{info, o, Logger};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::convert::TryInto;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use yatp::task::future::TaskCell;
use yatp::Remote;

const SYNC_INTERVAL: Duration = Duration::from_micros(100);

struct Proposal {
    index: u64,
    term: u64,
    notifier: mpsc::Sender<Res>,
}

struct ReadRequest {
    notifier: mpsc::Sender<Res>,
    start: Instant,
}

pub struct Fsm {
    node: RawNode<RockStorage>,
    receiver: Receiver<Msg>,
    sender: Sender<Msg>,
    pool: Remote<TaskCell>,
    db: Arc<DB>,
    logger: Logger,
    has_ready: bool,
    abort: bool,
    processed_msg_cnt: usize,
    write_batch: WriteBatch,
    unsynced_data_size: u64,
    persisted_messages: Vec<Vec<Message>>,
    last_ready_number: u64,
    last_sync_time: Instant,
    raft_client: RaftClient,
    proposal_queue: VecDeque<Proposal>,
    // ReadIndex don't have order, and can easily lose, use `HashMap`
    // for simplicity.
    read_states: HashMap<u64, ReadRequest>,
    read_queue: BTreeMap<u64, Vec<mpsc::Sender<Res>>>,
    role_observer: Vec<mpsc::Sender<Res>>,
}

impl Fsm {
    pub fn new(
        config: &Config,
        raft_client: RaftClient,
        logger: &Logger,
        pool: Remote<TaskCell>,
    ) -> Result<Fsm> {
        if !storage::exists(&config.data_dir) {
            if config.initial_peers.contains(&config.my_id) {
                super::bootstrap(
                    &config.data_dir,
                    raft_client.address_map(),
                    &config.initial_peers,
                    config.my_id,
                )?;
            } else {
                super::bootstrap(
                    &config.data_dir,
                    raft_client.address_map(),
                    &[],
                    config.my_id,
                )?;
            }
            info!(
                logger,
                "bootstrapped data dir at {}",
                config.data_dir.display()
            );
        }
        let storage = RockStorage::open(&config.data_dir, config.my_id)?;
        let db = storage.db();
        let cfg = raft::Config {
            id: storage.id(),
            applied: storage.applied(),
            pre_vote: true,
            election_tick: config.raft_election_ticks,
            heartbeat_tick: config.raft_heartbeat_ticks,
            ..Default::default()
        };
        let node = RawNode::new(&cfg, storage, logger)?;
        let logger = logger.new(o! {"fsm_id" => node.store().id()});
        let (tx, rx) = channel::bounded(4096);
        let mut fsm = Fsm {
            node,
            receiver: rx,
            sender: tx,
            logger,
            db,
            raft_client,
            pool,
            has_ready: false,
            abort: false,
            processed_msg_cnt: 0,
            write_batch: WriteBatch::with_capacity(4096),
            unsynced_data_size: 0,
            persisted_messages: Vec::with_capacity(4096),
            last_ready_number: 0,
            last_sync_time: Instant::now(),
            proposal_queue: VecDeque::with_capacity(4096),
            read_states: HashMap::with_capacity(4097),
            read_queue: BTreeMap::new(),
            role_observer: vec![],
        };
        fsm.on_start();
        Ok(fsm)
    }

    fn on_start(&mut self) {
        self.load_address_map();
        self.schedule_tick();

        if self.node.store().singleton() {
            let _ = self.node.campaign();
        }
    }

    fn load_address_map(&self) {
        let mut opt = ReadOptions::default();
        opt.set_iterate_upper_bound(address_key(u64::MAX).to_vec());
        opt.fill_cache(false);
        let map = self.raft_client.address_map();
        let mut address = map.lock();
        let db = self.node.store().db();
        let mut iter = db.iter_opt(opt);
        if iter.seek(SeekKey::Key(&address_key(0))).unwrap() {
            let id = u64::from_be_bytes(iter.key()[1..].try_into().unwrap());
            let addr = String::from_utf8(iter.value().to_vec()).unwrap();
            address.insert(id, addr);
        }
    }

    pub fn sender(&self) -> Sender<Msg> {
        self.sender.clone()
    }

    pub fn id(&self) -> u64 {
        self.node.store().id()
    }

    fn schedule_tick(&mut self) {
        let sender = self.sender.clone();
        self.pool.spawn(async move {
            Delay::new(Duration::from_millis(200)).await;
            // TODO: use unbounded channel instead.
            sender.try_send(Msg::Tick).unwrap_or_else(|e| {
                panic!("failed to send tick: {:?}", e);
            })
        });
    }

    fn get_notifier(&mut self, index: u64, term: u64) -> Option<mpsc::Sender<Res>> {
        loop {
            let front = match self.proposal_queue.front() {
                Some(p) => p,
                None => return None,
            };
            if front.term < term {
                self.proposal_queue.pop_front();
                continue;
            } else if front.term > term || front.index > index {
                return None;
            }
            assert_eq!(front.index, index, "{}", term);
            return self.proposal_queue.pop_front().map(|p| p.notifier);
        }
    }

    fn process(&mut self, start: Instant, msg: Msg) {
        match msg {
            Msg::Command { cmd, notifier } => {
                let (context, data) = cmd.into_proposal();
                match self.node.propose(context, data) {
                    Ok(()) => {
                        self.has_ready = true;
                        if let Some(notifier) = notifier {
                            self.proposal_queue.push_back(Proposal {
                                term: self.node.raft.term,
                                index: self.node.raft.raft_log.last_index(),
                                notifier,
                            })
                        }
                    }
                    Err(e) => {
                        let err_msg = format!("failed to make proposal: {}", e);
                        info!(self.logger, "{}", err_msg);
                        if let Some(mut notifier) = notifier {
                            let _ = notifier.try_send(Res::Fail(err_msg));
                        }
                    }
                }
            }
            Msg::Snapshot(notifier) => {
                // This is not technically safe, should use uuid.
                let state: u64 = rand::random();
                self.node.read_index(state.to_ne_bytes().to_vec());
                self.read_states
                    .insert(state, ReadRequest { notifier, start });
            }
            Msg::WaitTillLeader(mut notifier) => {
                let leader_id = self.node.raft.leader_id;
                if leader_id != 0 {
                    let _ = notifier.try_send(Res::Leader(leader_id));
                } else {
                    self.role_observer.push(notifier);
                }
            }
            Msg::RaftMessage(msg) => {
                if let Err(e) = self.node.step(msg) {
                    info!(self.logger, "failed to step message {}", e);
                } else {
                    self.has_ready = true;
                }
            }
            Msg::Tick => {
                self.has_ready |= self.node.tick();
                self.schedule_tick();
            }
            Msg::Stop => self.abort = true,
        }
    }

    fn handle_committed_entries(&mut self, entries: Vec<Entry>) -> u64 {
        let applied_index = entries.last().unwrap().get_index();
        for mut entry in entries {
            let context = entry.take_context();
            let data = entry.take_data();
            let index = entry.get_index();
            let term = entry.get_term();
            let res = match Command::from_proposal(context, data) {
                None => Res::Success,
                Some(Command::Put { key, value }) => {
                    if valid_data_key(&key) {
                        if let Err(e) = self.write_batch.put(&*key, &*value) {
                            panic!("unable to apply command: {:?}", e);
                        }
                        Res::Success
                    } else {
                        Res::Fail(format!("invalid key {:?}", key))
                    }
                }
                Some(Command::UpdateAddress { id, address }) => {
                    if let Err(e) = self.write_batch.put(&address_key(id), address.as_bytes()) {
                        panic!("unable to write address at {}: {}", index, e);
                    }
                    self.raft_client.address_map().lock().insert(id, address);
                    Res::Success
                }
            };
            if let Some(mut notifier) = self.get_notifier(index, term) {
                let _ = notifier.try_send(res);
            }
        }
        applied_index
    }

    fn send_messages(&mut self, msgs: Vec<Message>) {
        for msg in msgs {
            info!(self.logger, "sending {:?}", msg);
            self.raft_client.send(msg);
        }
    }

    fn process_read(&mut self, read_states: Vec<ReadState>) {
        for read in read_states {
            let id = u64::from_ne_bytes(read.request_ctx.try_into().unwrap());
            if let Some(mut req) = self.read_states.remove(&id) {
                if read.index <= self.node.store().applied() {
                    let _ = req
                        .notifier
                        .try_send(Res::Snapshot(self.node.store().rock_snapshot()));
                }
                self.read_queue
                    .entry(read.index)
                    .or_default()
                    .push(req.notifier);
            }
        }
    }

    fn clean_stale_read_req(&mut self, start: Instant) {
        self.read_states.retain(|_, v| {
            if start
                .checked_duration_since(v.start)
                .map_or(false, |d| d > Duration::from_secs(10))
            {
                let _ = v.notifier.try_send(Res::Fail("timeout".to_owned()));
                false
            } else {
                true
            }
        });
    }

    fn notify_role_changed(&mut self) {
        if self.node.raft.leader_id != INVALID_ID && !self.role_observer.is_empty() {
            for mut ob in self.role_observer.drain(..) {
                let _ = ob.try_send(Res::Leader(self.node.raft.leader_id));
            }
        }
    }

    fn process_ready(&mut self, start: Instant) -> Result<()> {
        let mut sync_log = false;
        if self.has_ready && self.node.has_ready() {
            let mut ready = self.node.ready();
            self.notify_role_changed();
            let applied_index = if !ready.committed_entries().is_empty() {
                Some(self.handle_committed_entries(ready.take_committed_entries()))
            } else {
                None
            };
            if !ready.messages().is_empty() {
                self.send_messages(ready.take_messages());
            }
            if !ready.read_states().is_empty() {
                self.process_read(ready.take_read_states());
            }
            self.clean_stale_read_req(start);
            self.last_ready_number = ready.number();
            let mut context = InvokeContext::new(self.node.store());
            if let Some(index) = applied_index {
                context.apply_state.set_applied_index(index);
            }
            self.node
                .mut_store()
                .process_ready(&mut context, &mut ready, &mut self.write_batch)?;
            if !self.write_batch.is_empty() {
                sync_log |= ready.must_sync();
                self.unsynced_data_size += self.write_batch.data_size() as u64;
                r!(self.db.write(&self.write_batch));
            }
            self.node.mut_store().post_ready(context);
            if !ready.persisted_messages().is_empty() {
                // Actually we don't have to check persisted_messages as raft-rs is
                // expected to tolerate with out of order messages.
                if self.write_batch.is_empty() && self.persisted_messages.is_empty() {
                    self.send_messages(ready.take_persisted_messages());
                } else {
                    self.persisted_messages
                        .push(ready.take_persisted_messages());
                }
            }
            self.write_batch.clear();
            self.node.advance_append_async(ready);
        }
        self.has_ready = false;
        if self.unsynced_data_size >= 512 * 1024
            || self.unsynced_data_size > 0
                && start
                    .checked_duration_since(self.last_sync_time)
                    .map_or(false, |d| d >= SYNC_INTERVAL)
        {
            sync_log = true;
        }
        if sync_log {
            r!(self.db.sync_wal());
            let after_sync = Instant::now();
            if let Some(d) = after_sync.checked_duration_since(start) {
                if d >= Duration::from_millis(100) {
                    info!(self.logger, "syncing WAL takes {:?}", d);
                }
            }
            self.last_sync_time = after_sync;
            self.unsynced_data_size = 0;
            self.node.on_persist_ready(self.last_ready_number);
            self.has_ready = true;
            let mut persisted_messages = std::mem::take(&mut self.persisted_messages);
            for msgs in persisted_messages.drain(..) {
                self.send_messages(msgs);
            }
            self.persisted_messages = persisted_messages;
        }
        Ok(())
    }

    fn suggest_timeout(&self) -> Option<Duration> {
        // Need sync for unsynced data or may need to handle committed entries
        // if sync has just happened.
        if self.unsynced_data_size > 0 || self.has_ready {
            Some(
                match Instant::now().checked_duration_since(self.last_sync_time) {
                    Some(dur) if dur < SYNC_INTERVAL => SYNC_INTERVAL - dur,
                    _ => Duration::new(0, 0),
                },
            )
        } else {
            None
        }
    }

    pub fn poll(&mut self) -> Result<()> {
        let mut timeout = None;
        loop {
            let mut msg = if let Some(dur) = timeout {
                match self.receiver.recv_timeout(dur) {
                    Ok(msg) => Some(msg),
                    Err(RecvTimeoutError::Timeout) => None,
                    Err(RecvTimeoutError::Disconnected) => return Ok(()),
                }
            } else {
                match self.receiver.recv() {
                    Ok(msg) => Some(msg),
                    Err(_) => return Ok(()),
                }
            };
            let start = Instant::now();
            while let Some(m) = msg {
                self.process(start, m);
                if self.abort {
                    return Ok(());
                }
                if self.processed_msg_cnt >= 4096 {
                    if self.has_ready {
                        break;
                    }
                    self.processed_msg_cnt = 0;
                }
                msg = self.receiver.try_recv().ok();
            }
            self.process_ready(start)?;
            timeout = self.suggest_timeout();
        }
    }
}
