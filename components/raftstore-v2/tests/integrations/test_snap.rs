// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::thread;

use futures::executor::block_on;
use kvproto::{
    raft_cmdpb::{CmdType, RaftCmdRequest, Request},
    raft_serverpb::RaftMessage,
};
use raft::eraftpb::MessageType;
use raftstore::store::{util::new_peer, INIT_EPOCH_CONF_VER, INIT_EPOCH_VER};
use raftstore_v2::router::PeerMsg;

use crate::Cluster;

/// Test basic write flow.
#[test]
fn test_basic_generate_snapshot() {
    test_util::init_log_for_test();
    let cluster = Cluster::with_node_count(2);

    let router = cluster.router(0);
    let mut raft_msg = RaftMessage::default();
    raft_msg.set_region_id(2);
    raft_msg.set_to_peer(new_peer(1, 3));
    //  raft_msg.set_
    let epoch = raft_msg.mut_region_epoch();
    epoch.set_version(INIT_EPOCH_VER);
    epoch.set_conf_ver(INIT_EPOCH_CONF_VER);

    let raft_message = raft_msg.mut_message();
    raft_message.set_msg_type(raft::prelude::MessageType::MsgAppendResponse);
    raft_message.set_from(3);
    raft_message.set_term(5);

    let peer_msg = PeerMsg::RaftMessage(Box::new(raft_msg));
    router.send(2, peer_msg).unwrap();
    println!("Hidden output");
    thread::sleep(std::time::Duration::from_secs(3));
}
