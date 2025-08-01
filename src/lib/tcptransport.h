// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * tcptransport.h:
 *   message-passing network interface that uses UDP message delivery
 *   and libasync
 *
 * Copyright 2013 Dan R. K. Ports  <drkp@cs.washington.edu>
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

#ifndef _LIB_TCPTRANSPORT_H_
#define _LIB_TCPTRANSPORT_H_

#include "lib/configuration.h"
#include "lib/transport.h"
#include "lib/transportcommon.h"
#include "lib/latency.h"
#include "lib/threadpool.h"
//#include "lib/threadpool.cc"

#include <event2/event.h>
#include <event2/buffer.h>
#include <event2/bufferevent.h>

#include <map>
#include <unordered_map>
#include <list>
#include <random>
#include <mutex>
#include <shared_mutex>
#include <netinet/in.h>

class TCPTransportAddress : public TransportAddress
{
public:
    virtual TCPTransportAddress * clone() const;
    virtual ~TCPTransportAddress() {}
    sockaddr_in addr;
    TCPTransportAddress(const sockaddr_in &addr);
private:    

    friend class TCPTransport;
    friend bool operator==(const TCPTransportAddress &a,
                           const TCPTransportAddress &b);
    friend bool operator!=(const TCPTransportAddress &a,
                           const TCPTransportAddress &b);
    friend bool operator<(const TCPTransportAddress &a,
                          const TCPTransportAddress &b);
};

class TCPTransport : public TransportCommon<TCPTransportAddress>
{
public:
    TCPTransport(double dropRate = 0.0, double reogrderRate = 0.0,
                    int dscp = 0, bool handleSignals = true,
                     int process_id = 0, int total_processes = 1,
                     bool hyperthreading = true, bool server = true, int mode = 0, bool optimize_tpool_for_dev_machine = false);
    virtual ~TCPTransport();
    virtual void Register(TransportReceiver *receiver,
                  const transport::Configuration &config,
                  int groupIdx,
                  int replicaIdx) override;
    virtual bool OrderedMulticast(TransportReceiver *src,
        const std::vector<int> &groups, const Message &m) override;

    virtual void Run() override;
    virtual void Stop() override;
    virtual void Close(TransportReceiver *receiver) override;
    virtual int Timer(uint64_t ms, timer_callback_t cb) override;
    virtual int TimerMicro(uint64_t us, timer_callback_t cb) override;
    virtual bool CancelTimer(int id) override;
    virtual void CancelAllTimers() override;
    //virtual void Flush() override;

    void DispatchTP(std::function<void*()> f, std::function<void(void*)> cb);
    void DispatchTP_local(std::function<void*()> f, std::function<void(void*)> cb);
    void DispatchTP_noCB(std::function<void*()> f);
    void DispatchTP_noCB_ptr(std::function<void*()> *f);
    void DispatchTP_main(std::function<void*()> f);
    void IssueCB(std::function<void(void*)> cb, void* arg);
    void IssueCB_main(std::function<void(void*)> cb, void* arg);
    void CancelLoadBonus() override;
    //Indexed Threadpool
    void AddIndexedThreads(int num_threads); 
    void DispatchIndexedTP(uint64_t id, std::function<void *()> f, std::function<void(void *)> cb);
    void DispatchIndexedTP_noCB(uint64_t id, std::function<void *()> f); 

    TCPTransportAddress
    LookupAddress(const transport::Configuration &cfg,
                  int replicaIdx);

    virtual TCPTransportAddress
    LookupAddress(const transport::Configuration &config,
                  int groupIdx,
                  int replicaIdx) override;

    TCPTransportAddress
    LookupAddress(const transport::ReplicaAddress &addr);


private:
    int TimerInternal(struct timeval &tv, timer_callback_t cb);
    std::shared_mutex mtx;
    std::shared_mutex timer_mtx;
    struct TCPTransportTimerInfo
    {
        TCPTransport *transport;
        timer_callback_t cb;
        event *ev;
        int id;
    };
    struct TCPTransportTCPListener
    {
        TCPTransport *transport;
        TransportReceiver *receiver;
        int acceptFd;
        int groupIdx;
        int replicaIdx;
        event *acceptEvent;
        std::list<struct bufferevent *> connectionEvents;
    };
    event_base *libeventBase;
    std::vector<event *> listenerEvents;
    std::vector<event *> signalEvents;
    std::map<int, TransportReceiver*> receivers; // fd -> receiver
    std::map<TransportReceiver*, int> fds; // receiver -> fd
    int lastTimerId;
    std::map<int, TCPTransportTimerInfo *> timers;
    std::list<TCPTransportTCPListener *> tcpListeners;
    std::map<std::pair<TCPTransportAddress, TransportReceiver *>, struct bufferevent *> tcpOutgoing;
    std::map<struct bufferevent *, std::pair<TCPTransportAddress, TransportReceiver *>> tcpAddresses;
    Latency_t sockWriteLat;
    ThreadPool tp;

    bool stopped;

    virtual bool SendMessageInternal(TransportReceiver *src,
                             const TCPTransportAddress &dst,
                             const Message &m) override;
    virtual const TCPTransportAddress *
    LookupMulticastAddress(const transport::Configuration*config) override {
      return nullptr;
    };

    virtual const TCPTransportAddress *
    LookupFCAddress(const transport::Configuration *cfg) override {
      return nullptr;
    }

    void ConnectTCP(const std::pair<TCPTransportAddress, TransportReceiver *> &dstSrc);
    void OnTimer(TCPTransportTimerInfo *info);
    static void TimerCallback(evutil_socket_t fd,
                              short what, void *arg);
    static void LogCallback(int severity, const char *msg);
    static void FatalCallback(int err);
    static void SignalCallback(evutil_socket_t fd,
                               short what, void *arg);
    static void TCPAcceptCallback(evutil_socket_t fd, short what,
                                  void *arg);
    static void TCPReadableCallback(struct bufferevent *bev,
                                    void *arg);
    static void TCPEventCallback(struct bufferevent *bev,
                                 short what, void *arg);
    static void TCPIncomingEventCallback(struct bufferevent *bev,
                                         short what, void *arg);
    static void TCPOutgoingEventCallback(struct bufferevent *bev,
                                         short what, void *arg);
};

#endif  // _LIB_TCPTRANSPORT_H_
