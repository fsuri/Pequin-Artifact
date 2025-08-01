// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * simtransport.h:
 *   simulated message-passing interface for testing use
 *
 * Copyright 2013-2015 Irene Zhang <iyzhang@cs.washington.edu>
 *                     Naveen Kr. Sharma <naveenks@cs.washington.edu>
 *                     Dan R. K. Ports  <drkp@cs.washington.edu>
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 **********************************************************************/

#ifndef _LIB_SIMTRANSPORT_H_
#define _LIB_SIMTRANSPORT_H_

#include "lib/transport.h"
#include "lib/transportcommon.h"

#include <deque>
#include <map>
#include <functional>

class SimulatedTransportAddress : public TransportAddress
{
public:
    SimulatedTransportAddress * clone() const;
    SimulatedTransportAddress(int addr);
    int GetAddr() const;
    bool operator==(const SimulatedTransportAddress &other) const;
    inline bool operator!=(const SimulatedTransportAddress &other) const
    {
        return !(*this == other);
    }
private:

    int addr;
    friend class SimulatedTransport;
};

class SimulatedTransport :
    public TransportCommon<SimulatedTransportAddress>
{
    typedef std::function<bool (TransportReceiver*, int,
                                TransportReceiver*, int,
                                Message &, uint64_t &delay)> filter_t;
public:
    SimulatedTransport();
    ~SimulatedTransport();
    void Register(TransportReceiver *receiver,
                  const transport::Configuration &config,
                  int replicaIdx);
    void Register(TransportReceiver *receiver,
                  const transport::Configuration &config,
                  int groupIdx,
                  int replicaIdx);
    void Run();
    void AddFilter(int id, filter_t filter);
    void RemoveFilter(int id);
    int Timer(uint64_t ms, timer_callback_t cb);
    bool CancelTimer(int id);
    void CancelAllTimers();
    void Stop() override;
    virtual void Close(TransportReceiver *receiver) override;

    void DispatchTP(std::function<void*()> f, std::function<void(void*)> cb);
    void DispatchTP_local(std::function<void*()> f, std::function<void(void*)> cb);
    void DispatchTP_noCB(std::function<void*()> f);
    void DispatchTP_noCB_ptr(std::function<void*()> *f);
    void DispatchTP_main(std::function<void*()> f);
    void IssueCB(std::function<void(void*)> cb, void* arg);
    void IssueCB_main(std::function<void(void*)> cb, void* arg);
     //Indexed Threadpool
    void AddIndexedThreads(int num_threads); 
    void DispatchIndexedTP(uint64_t id, std::function<void *()> f, std::function<void(void *)> cb);
    void DispatchIndexedTP_noCB(uint64_t id, std::function<void *()> f); 


protected:
    bool SendMessageInternal(TransportReceiver *src,
                             const SimulatedTransportAddress &dstAddr,
                             const Message &m,
                             bool multicast);

    bool SendMessageToReplica(TransportReceiver *src,
                             int groupIdx,
                             int replicaIdx,
                             const Message &m) override;

    SimulatedTransportAddress
    LookupAddress(const transport::Configuration &cfg, int idx);
    SimulatedTransportAddress
    LookupAddress(const transport::Configuration &cfg, int groupIdx, int idx);
    const SimulatedTransportAddress *
    LookupMulticastAddress(const transport::Configuration *cfg);
    const SimulatedTransportAddress *
    LookupFCAddress(const transport::Configuration *cfg) override;
    bool SendMessageInternal(TransportReceiver *src,
                             const SimulatedTransportAddress &dstAddr,
                             const Message &m) override;

private:
    struct QueuedMessage {
        int dst;
        int src;
        string type;
        string msg;
        inline QueuedMessage(int dst, int src,
                             const string &type, const string &msg) :
            dst(dst), src(src), type(type), msg(msg) { }
    };
    struct PendingTimer {
        uint64_t when;
        int id;
        timer_callback_t cb;
    };

    std::deque<QueuedMessage> queue;
    std::map<int,TransportReceiver *> endpoints;
    int lastAddr;
//    std::map<int,int> replicas;
    std::map<int,int> replicaIdxs;
    std::map<int, std::map<int,int>> g_replicaIdxs;
    std::multimap<int,filter_t> filters;
    std::multimap<uint64_t, PendingTimer> timers;
    int lastTimerId;
    uint64_t vtime;
    bool processTimers;
    int fcAddress;
    bool running;

    bool _SendMessageInternal(TransportReceiver *src,
                              const SimulatedTransportAddress &dstAddr,
                              const Message &m,
                              const multistamp_t &stamp);
};

#endif  // _LIB_SIMTRANSPORT_H_
