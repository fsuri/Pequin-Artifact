/**
 * Copyright 2018 VMware
 * Copyright 2018 Ted Yin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <iostream>
#include <cstring>
#include <cassert>
#include <algorithm>
#include <random>
#include <unistd.h>
#include <signal.h>
#include <map>

#include "salticidae/stream.h"
#include "salticidae/util.h"
#include "salticidae/network.h"
#include "salticidae/msg.h"

#include "hotstuff/promise.hpp"
#include "hotstuff/type.h"
#include "hotstuff/entity.h"
#include "hotstuff/util.h"
#include "hotstuff/client.h"
#include "hotstuff/hotstuff.h"
#include "hotstuff/liveness.h"

using salticidae::MsgNetwork;
using salticidae::ClientNetwork;
using salticidae::ElapsedTime;
using salticidae::Config;
using salticidae::_1;
using salticidae::_2;
using salticidae::static_pointer_cast;
using salticidae::trim_all;
using salticidae::split;

using hotstuff::TimerEvent;
using hotstuff::EventContext;
using hotstuff::NetAddr;
using hotstuff::HotStuffError;
using hotstuff::CommandDummy;
using hotstuff::Finality;
using hotstuff::command_t;
using hotstuff::uint256_t;
using hotstuff::opcode_t;
using hotstuff::bytearray_t;
using hotstuff::DataStream;
using hotstuff::ReplicaID;
using hotstuff::MsgReqCmd;
using hotstuff::MsgRespCmd;
using hotstuff::get_hash;
using hotstuff::promise_t;

using HotStuff = hotstuff::HotStuffSecp256k1;

class HotStuffApp: public HotStuff {
    double stat_period;
    double impeach_timeout;
    EventContext ec;
    EventContext req_ec;
    EventContext resp_ec;
    /** Network messaging between a replica and its client. */
    ClientNetwork<opcode_t> cn;
    /** Timer object to schedule a periodic printing of system statistics */
    TimerEvent ev_stat_timer;
    /** Timer object to monitor the progress for simple impeachment */
    TimerEvent impeach_timer;
    /** The listen address for client RPC */
    NetAddr clisten_addr;

    std::set<uint256_t> hashed_digests;

    std::unordered_map<const uint256_t, promise_t> unconfirmed;

    using conn_t = ClientNetwork<opcode_t>::conn_t;
    using resp_queue_t = salticidae::MPSCQueueEventDriven<std::pair<Finality, NetAddr>>;

    /* for the dedicated thread sending responses to the clients */
    std::thread req_thread;
    std::thread resp_thread;
    resp_queue_t resp_queue;
    salticidae::BoxObj<salticidae::ThreadCall> resp_tcall;
    salticidae::BoxObj<salticidae::ThreadCall> req_tcall;

    void client_request_cmd_handler(MsgReqCmd &&, const conn_t &);

    static command_t parse_cmd(DataStream &s) {
        auto cmd = new CommandDummy();
        s >> *cmd;
        return cmd;
    }

    void reset_imp_timer() {
        impeach_timer.del();
        impeach_timer.add(impeach_timeout);
    }

    bool has_smr_cb = false;
    //std::function<void(uint256_t cmd_hash, uint32_t seqnum)> smr_cb;
    std::function<void(const std::string&, uint32_t seqnum)> smr_cb;
    std::map<uint256_t, std::pair<Finality, std::string>> digest_map;
    uint32_t last_block = 0;
    uint32_t last_seq = 0;

    void state_machine_execute(const Finality &fin) override {
        reset_imp_timer();
#ifndef HOTSTUFF_ENABLE_BENCHMARK
        HOTSTUFF_LOG_INFO("replicated %s", std::string(fin).c_str());
#endif

        //std::cerr << "State machine execute" << std::endl;
        return;

        assert(fin.cmd_height >= 1);
        //Assert fin come in order
        if(fin.cmd_height < last_block){ //Note. Don't check ==, multiple fin from same block might be invoked
            // std::cerr << "smr execute called out of order" << std::endl;  //TODO: This is not necessarily true at follower replicas... But it shouldn't impact anything since we are running "fakeSMR"
            assert(false);
        }
        last_block = fin.cmd_height;

     
        //TODO: Dynamically compute seq number.
        uint32_t seqnum = (fin.cmd_height - 1) * blk_size + fin.cmd_idx; //TODO: make u64_t everywhere
        //assert(blk_size == 1);
        //std::cerr << "Execute height " << fin.cmd_height << " idx: " << fin.cmd_idx << ". Seq no:  " << seqnum << std::endl;

        // auto itr = digest_map.find(fin.cmd_hash);
        // assert(itr != digest_map.end()); //FIXME: Using SMR callback still requires everyone to call propose to register the map...
        //                                 // TODO: make callback use cmd_hash. At replica level translate digest into cmd_hash (and back)
        std::string dig; 
        //Call pre-registered callback
        //fin.cmd_hash
        if(has_smr_cb) smr_cb(dig, seqnum);
    }

    std::unordered_set<conn_t> client_conns;
    void print_stat() const;

    public:
    HotStuffApp(uint32_t blk_size, uint32_t blk_timeout,
                double stat_period,
                double impeach_timeout,
                ReplicaID idx,
                const bytearray_t &raw_privkey,
                NetAddr plisten_addr,
                NetAddr clisten_addr,
                hotstuff::pacemaker_bt pmaker,
                const EventContext &ec,
                size_t nworker,
                const Net::Config &repnet_config,
                const ClientNetwork<opcode_t>::Config &clinet_config);

    void start(const std::vector<std::tuple<NetAddr, bytearray_t, bytearray_t>> &reps);
    void interface_entry();
    
    void stop();

    inline void register_smr_callback(std::function<void(const std::string&, uint32_t seqnum)> cb){ 
        bool has_smr_cb = true;
        smr_cb = cb;
    }

    void interface_propose(const string &hash,  std::function<void(const std::string&, uint32_t seqnum)> cb);
};

std::pair<std::string, std::string> split_ip_port_cport(const std::string &s) {
    auto ret = trim_all(split(s, ";"));
    if (ret.size() != 2)
        throw std::invalid_argument("invalid cport format");
    return std::make_pair(ret[0], ret[1]);
}

salticidae::BoxObj<HotStuffApp> hotstuff_papp = nullptr;


HotStuffApp::HotStuffApp(uint32_t blk_size, uint32_t blk_timeout,
                        double stat_period,
                        double impeach_timeout,
                        ReplicaID idx,
                        const bytearray_t &raw_privkey,
                        NetAddr plisten_addr,
                        NetAddr clisten_addr,
                        hotstuff::pacemaker_bt pmaker,
                        const EventContext &ec,
                        size_t nworker,
                        const Net::Config &repnet_config,
                        const ClientNetwork<opcode_t>::Config &clinet_config):
    HotStuff(blk_size, blk_timeout, 0, 0, idx, raw_privkey,
            plisten_addr, std::move(pmaker), ec, nworker, repnet_config),
    stat_period(stat_period),
    impeach_timeout(impeach_timeout),
    ec(ec),
    cn(req_ec, clinet_config),
    clisten_addr(clisten_addr) {

    /* prepare the thread used for sending back confirmations */
    resp_tcall = new salticidae::ThreadCall(resp_ec);
    req_tcall = new salticidae::ThreadCall(req_ec);
    resp_queue.reg_handler(resp_ec, [this](resp_queue_t &q) {
        std::pair<Finality, NetAddr> p;
        while (q.try_dequeue(p))
        {
            try {
                cn.send_msg(MsgRespCmd(std::move(p.first)), p.second);
            } catch (std::exception &err) {
                HOTSTUFF_LOG_WARN("unable to send to the client: %s", err.what());
            }
        }
        return false;
    });

    /* register the handlers for msg from clients */
    cn.reg_handler(salticidae::generic_bind(&HotStuffApp::client_request_cmd_handler, this, _1, _2));
    cn.start();
    cn.listen(clisten_addr);
}

void HotStuffApp::client_request_cmd_handler(MsgReqCmd &&msg, const conn_t &conn) {

    const NetAddr addr = conn->get_addr();
    auto cmd = parse_cmd(msg.serialized);
    const auto &cmd_hash = cmd->get_hash();
    HOTSTUFF_LOG_DEBUG("processing %s", std::string(*cmd).c_str());
    exec_command(cmd_hash, [this, addr](Finality fin) {
        resp_queue.enqueue(std::make_pair(fin, addr));
    });
}



std::string string_to_hex(const std::string& input)
{
    static const char hex_digits[] = "0123456789ABCDEF";

    std::string output;
    output.reserve(input.length() * 2);
    for (unsigned char c : input)
    {
        output.push_back(hex_digits[c >> 4]);
        output.push_back(hex_digits[c & 15]);
    }
    return output;
}

static std::mutex seq_mutex;
void HotStuffApp::interface_propose(const string &hash,  std::function<void(const std::string&, uint32_t seqnum)> cb) {
    uint256_t cmd_hash((const uint8_t *)hash.c_str());

    //std::cerr << "Propose hash " << hash << " as hex: " << string_to_hex(hash) << " --> cmd hash: " <<  cmd_hash.to_hex() << std::endl; //TODO: ideally print cmd_hash, but it has no ToString function

    if (!this->hashed_digests.insert(cmd_hash).second){
        std::cerr << "Panic:  duplicate hash per Hotstuff requests with hash "<<hash << std::endl;
        abort;
        exit(1);
    }

    exec_command(cmd_hash, [this, hash, cb](Finality fin) {
        //std::cerr << "exec cmd: Core: " << sched_getcpu() << std::endl;
        seq_mutex.lock();
            assert(fin.cmd_height >= 1);

            // if(fin.cmd_height < last_block){ //Note. Don't check ==, multiple fin from same block might be invoked
            //     std::cerr << "smr execute called out of order" << std::endl;
            //     assert(false);
            // }
            last_block = fin.cmd_height;
         
            uint32_t seqnum = last_seq++; //return current seq_num, and increment.

        //    uint32_t seqnum = (fin.cmd_height - 1) * blk_size + fin.cmd_idx;
        //    assert(blk_size == 1);
        // std::cerr << "Execute height " << fin.cmd_height << " idx: " << fin.cmd_idx << ". Seq no:  " << seqnum << std::endl;
        seq_mutex.unlock();
            cb(hash, seqnum);
    });
}


void HotStuffApp::start(const std::vector<std::tuple<NetAddr, bytearray_t, bytearray_t>> &reps) {
    ev_stat_timer = TimerEvent(ec, [this](TimerEvent &) {
        HotStuff::print_stat();
        HotStuffApp::print_stat();
        //HotStuffCore::prune(100);
        ev_stat_timer.add(stat_period);
    });
    ev_stat_timer.add(stat_period);
    impeach_timer = TimerEvent(ec, [this](TimerEvent &) {
        if (get_decision_waiting().size())
            get_pace_maker()->impeach();
        reset_imp_timer();
    });

    impeach_timer.add(impeach_timeout);
    HOTSTUFF_LOG_INFO("** starting the system with parameters **");
    HOTSTUFF_LOG_INFO("blk_size = %lu", blk_size);
    HOTSTUFF_LOG_INFO("conns = %lu", HotStuff::size());
    HOTSTUFF_LOG_INFO("** starting the event loop...");
    HotStuff::start(reps);
}

void HotStuffApp::interface_entry() {
    // cn.reg_conn_handler([this](const salticidae::ConnPool::conn_t &_conn, bool connected) {
    //     auto conn = salticidae::static_pointer_cast<conn_t::type>(_conn);
    //     if (connected)
    //         client_conns.insert(conn);
    //     else
    //         client_conns.erase(conn);
    //     return true;
    // });
    req_thread = std::thread([this]() { req_ec.dispatch(); });
    resp_thread = std::thread([this]() { resp_ec.dispatch(); });
    /* enter the event main loop */
    ec.dispatch();
}

void HotStuffApp::stop() {
    hotstuff_papp->req_tcall->async_call([this](salticidae::ThreadCall::Handle &) {
        req_ec.stop();
    });
    hotstuff_papp->resp_tcall->async_call([this](salticidae::ThreadCall::Handle &) {
        resp_ec.stop();
    });

    req_thread.join();
    resp_thread.join();
    ec.stop();
}

void HotStuffApp::print_stat() const {
#ifdef HOTSTUFF_MSG_STAT
    HOTSTUFF_LOG_INFO("--- client msg. (10s) ---");
    size_t _nsent = 0;
    size_t _nrecv = 0;
    for (const auto &conn: client_conns)
    {
        if (conn == nullptr) continue;
        size_t ns = conn->get_nsent();
        size_t nr = conn->get_nrecv();
        size_t nsb = conn->get_nsentb();
        size_t nrb = conn->get_nrecvb();
        conn->clear_msgstat();
        HOTSTUFF_LOG_INFO("%s: %u(%u), %u(%u)",
            std::string(conn->get_addr()).c_str(), ns, nsb, nr, nrb);
        _nsent += ns;
        _nrecv += nr;
    }
    HOTSTUFF_LOG_INFO("--- end client msg. ---");
#endif
}
