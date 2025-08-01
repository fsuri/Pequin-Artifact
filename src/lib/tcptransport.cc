// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * tcptransport.cc:
 *   message-passing network interface that uses TCP message delivery
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

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/message.h"
#include "lib/tcptransport.h"

#include <google/protobuf/message.h>
#include <event2/thread.h>
#include <event2/bufferevent_struct.h>

#include <cstdio>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <signal.h>
#include <utility>
//#include "lib/threadpool.cc"

const size_t MAX_TCP_SIZE = 100; // XXX
const uint32_t MAGIC = 0x06121983;
const int SOCKET_BUF_SIZE = 1048576;

using std::pair;

TCPTransportAddress::TCPTransportAddress(const sockaddr_in &addr)
    : addr(addr)
{
    memset((void *)addr.sin_zero, 0, sizeof(addr.sin_zero));
}

TCPTransportAddress *
TCPTransportAddress::clone() const
{
    TCPTransportAddress *c = new TCPTransportAddress(*this);
    return c;
}

bool operator==(const TCPTransportAddress &a, const TCPTransportAddress &b)
{
    return (memcmp(&a.addr, &b.addr, sizeof(a.addr)) == 0);
}

bool operator!=(const TCPTransportAddress &a, const TCPTransportAddress &b)
{
    return !(a == b);
}

bool operator<(const TCPTransportAddress &a, const TCPTransportAddress &b)
{
    return (memcmp(&a.addr, &b.addr, sizeof(a.addr)) < 0);
}

TCPTransportAddress
TCPTransport::LookupAddress(const transport::ReplicaAddress &addr)
{
        int res;
        struct addrinfo hints;
        memset(&hints, 0, sizeof(hints));
        hints.ai_family   = AF_INET;
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_protocol = 0;
        hints.ai_flags    = 0;
        struct addrinfo *ai;
        if ((res = getaddrinfo(addr.host.c_str(), addr.port.c_str(),
                               &hints, &ai))) {
            Panic("Failed to resolve %s:%s: %s",
                  addr.host.c_str(), addr.port.c_str(), gai_strerror(res));
        }
        if (ai->ai_addr->sa_family != AF_INET) {
            Panic("getaddrinfo returned a non IPv4 address");
        }
        TCPTransportAddress out =
            TCPTransportAddress(*((sockaddr_in *)ai->ai_addr));
        freeaddrinfo(ai);
        return out;
}

TCPTransportAddress
TCPTransport::LookupAddress(const transport::Configuration &config,
                            int idx)
{
  return LookupAddress(config, 0, idx);
}

TCPTransportAddress
TCPTransport::LookupAddress(const transport::Configuration &config,
                            int groupIdx,
                            int replicaIdx)
{
    const transport::ReplicaAddress &addr = config.replica(groupIdx,
                                                           replicaIdx);
    return LookupAddress(addr);
}

static void
BindToPort(int fd, const string &host, const string &port)
{
    struct sockaddr_in sin;

    // look up its hostname and port number (which
    // might be a service name)
    struct addrinfo hints;
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = 0;
    hints.ai_flags    = AI_PASSIVE;
    struct addrinfo *ai;
    int res;
    if ((res = getaddrinfo(host.c_str(),
			   port.c_str(),
                           &hints, &ai))) {
        Panic("Failed to resolve host/port %s:%s: %s",
              host.c_str(), port.c_str(), gai_strerror(res));
    }
    UW_ASSERT(ai->ai_family == AF_INET);
    UW_ASSERT(ai->ai_socktype == SOCK_STREAM);
    if (ai->ai_addr->sa_family != AF_INET) {
        Panic("getaddrinfo returned a non IPv4 address");
    }
    sin = *(sockaddr_in *)ai->ai_addr;

    freeaddrinfo(ai);

    Debug("Binding to %s %d TCP", inet_ntoa(sin.sin_addr), htons(sin.sin_port));

    if (bind(fd, (sockaddr *)&sin, sizeof(sin)) < 0) {
        PPanic("Failed to bind to socket: %s:%d", inet_ntoa(sin.sin_addr),
            htons(sin.sin_port));
    }
}

TCPTransport::TCPTransport(double dropRate, double reorderRate,
			   int dscp, bool handleSignals, int process_id, int total_processes, bool hyperthreading, bool server, int mode, bool optimize_tpool_for_dev_machine)
{
    tp.start(process_id, total_processes, hyperthreading, server, mode, optimize_tpool_for_dev_machine);

    lastTimerId = 0;

    // Set up libevent
    evthread_use_pthreads();
    event_set_log_callback(LogCallback);
    event_set_fatal_callback(FatalCallback);
    
    // auto cfg = event_config_new();
    // // // event_config_init(cfg);
    // event_config_set_flag(cfg, EVENT_BASE_FLAG_PRECISE_TIMER);
    // // event_config_set_flag(cfg, EVENT_BASE_FLAG_EPOLL_DISALLOW_TIMERFD); //This flag is only supported on master branch of libevent, not on stable release.
    // // //libeventBase = event_base_new_with_config(cfg);
    // if ((libeventBase = event_base_new_with_config(cfg)) == NULL){
    //     Panic("Failed to create an event base");
    // }
    // event_config_free(cfg);


    libeventBase = event_base_new();
    //tp2.emplace(); this works?
    //tp = new ThreadPool(); //change tp to *
    evthread_make_base_notifiable(libeventBase);

    // Set up signal handler
    if (handleSignals) {
        signalEvents.push_back(evsignal_new(libeventBase, SIGTERM,
                                            SignalCallback, this));
        signalEvents.push_back(evsignal_new(libeventBase, SIGINT,
                                            SignalCallback, this));
        signalEvents.push_back(evsignal_new(libeventBase, SIGPIPE,
                                            [](int fd, short what, void* arg){}, this));

        for (event *x : signalEvents) {
            event_add(x, NULL);
        }
    }
    _Latency_Init(&sockWriteLat, "sock_write");
}

TCPTransport::~TCPTransport()
{
  // Old version
    // // XXX Shut down libevent?
    // event_base_free(libeventBase);
    // // for (auto kv : timers) {
    // //     delete kv.second;
    // // }
    // Latency_Dump(&sockWriteLat);
    mtx.lock();
    for (auto itr = tcpOutgoing.begin(); itr != tcpOutgoing.end(); ) {
      bufferevent_free(itr->second);
      tcpAddresses.erase(itr->second);
      //TCPTransportTCPListener* info = nullptr;
      //bufferevent_getcb(itr->second, nullptr, nullptr, nullptr,
      //    (void **) &info);
      //if (info != nullptr) {
      //  delete info;
      //}
      itr = tcpOutgoing.erase(itr);
    }
  // for (auto kv : timers) {
  //     delete kv.second;
  // }
    Latency_Dump(&sockWriteLat);
    for (const auto info : tcpListeners) {
      delete info;
    }
    mtx.unlock();
    // XXX Shut down libevent?
    event_base_free(libeventBase);
}

void TCPTransport::ConnectTCP(const std::pair<TCPTransportAddress, TransportReceiver *> &dstSrc) {
  Debug("Opening new TCP connection to %s:%d", inet_ntoa(dstSrc.first.addr.sin_addr),
        htons(dstSrc.first.addr.sin_port));

    // Create socket
    int fd;
    if ((fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        PPanic("Failed to create socket for outgoing TCP connection");
    }

    // Put it in non-blocking mode
    if (fcntl(fd, F_SETFL, O_NONBLOCK, 1)) {
        PWarning("Failed to set O_NONBLOCK on outgoing TCP socket");
    }

    // Set TCP_NODELAY
    int n = 1;
    if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (char *)&n, sizeof(n)) < 0) {
      PWarning("Failedt to set TCP_NODELAY on TCP listening socket");
    }

    n = SOCKET_BUF_SIZE;
    if (setsockopt(fd, SOL_SOCKET, SO_RCVBUF, (char *)&n, sizeof(n)) < 0) {
      PWarning("Failed to set SO_RCVBUF on socket");
    }

    if (setsockopt(fd, SOL_SOCKET, SO_SNDBUF, (char *)&n, sizeof(n)) < 0) {
      PWarning("Failed to set SO_SNDBUF on socket");
    }


    TCPTransportTCPListener *info = new TCPTransportTCPListener();
    info->transport = this;
    info->acceptFd = 0;
    info->receiver = dstSrc.second;
    info->replicaIdx = -1;
    info->acceptEvent = NULL;

    tcpListeners.push_back(info);

    struct bufferevent *bev =
        bufferevent_socket_new(libeventBase, fd,
                               BEV_OPT_CLOSE_ON_FREE | BEV_OPT_THREADSAFE);

    //mtx.lock();
    tcpOutgoing[dstSrc] = bev;
    tcpAddresses.insert(pair<struct bufferevent*, pair<TCPTransportAddress, TransportReceiver*>>(bev, dstSrc));
    //mtx.unlock();

    bufferevent_setcb(bev, TCPReadableCallback, NULL, TCPOutgoingEventCallback, info);
    if (bufferevent_socket_connect(bev, (struct sockaddr *)&(dstSrc.first.addr), sizeof(dstSrc.first.addr)) < 0) {
        bufferevent_free(bev);

        //mtx.lock();
        tcpOutgoing.erase(dstSrc);
        tcpAddresses.erase(bev);
        //mtx.unlock();

        Warning("Failed to connect to server via TCP");
        return;
    }

    if (bufferevent_enable(bev, EV_READ|EV_WRITE) < 0) {
        Panic("Failed to enable bufferevent");
    }

    // Tell the receiver its address
    struct sockaddr_in sin;
    socklen_t sinsize = sizeof(sin);
    if (getsockname(fd, (sockaddr *) &sin, &sinsize) < 0) {
        PPanic("Failed to get socket name");
    }
    
    if (dstSrc.second->GetAddress() == nullptr) {
      TCPTransportAddress *addr = new TCPTransportAddress(sin);
      dstSrc.second->SetAddress(addr);
    }


    // Debug("Opened TCP connection to %s:%d",
	  // inet_ntoa(dstSrc.first.addr.sin_addr), htons(dstSrc.first.addr.sin_port));
    Debug("Opened TCP connection to %s:%d from %s:%d",
	  inet_ntoa(dstSrc.first.addr.sin_addr), htons(dstSrc.first.addr.sin_port),
	  inet_ntoa(sin.sin_addr), htons(sin.sin_port));
}

void
TCPTransport::Register(TransportReceiver *receiver,
                       const transport::Configuration &config,
                       int groupIdx, int replicaIdx)
{
    UW_ASSERT(replicaIdx < config.n);
    struct sockaddr_in sin;

    //const transport::Configuration *canonicalConfig =
    RegisterConfiguration(receiver, config, groupIdx, replicaIdx);

    // Clients don't need to accept TCP connections
    if (replicaIdx == -1) {
	return;
    }

    // Create socket
    int fd;
    if ((fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        PPanic("Failed to create socket to accept TCP connections");
    }

    // Put it in non-blocking mode
    if (fcntl(fd, F_SETFL, O_NONBLOCK, 1)) {
        PWarning("Failed to set O_NONBLOCK");
    }

    // Set SO_REUSEADDR
    int n = 1;
    if (setsockopt(fd, SOL_SOCKET,
                   SO_REUSEADDR, (char *)&n, sizeof(n)) < 0) {
        PWarning("Failed to set SO_REUSEADDR on TCP listening socket");
    }

    // Set TCP_NODELAY
    n = 1;
    if (setsockopt(fd, IPPROTO_TCP,
                   TCP_NODELAY, (char *)&n, sizeof(n)) < 0) {
        PWarning("Failed to set TCP_NODELAY on TCP listening socket");
    }

    n = SOCKET_BUF_SIZE;
    if (setsockopt(fd, SOL_SOCKET, SO_RCVBUF, (char *)&n, sizeof(n)) < 0) {
      PWarning("Failed to set SO_RCVBUF on socket");
    }

    if (setsockopt(fd, SOL_SOCKET, SO_SNDBUF, (char *)&n, sizeof(n)) < 0) {
      PWarning("Failed to set SO_SNDBUF on socket");
    }


    // Registering a replica. Bind socket to the designated
    // host/port
    const string &host = config.replica(groupIdx, replicaIdx).host;
    const string &port = config.replica(groupIdx, replicaIdx).port;
    BindToPort(fd, host, port);

    // Listen for connections
    if (listen(fd, 5) < 0) {
        PPanic("Failed to listen for TCP connections");
    }

    // Create event to accept connections
    TCPTransportTCPListener *info = new TCPTransportTCPListener();
    info->transport = this;
    info->acceptFd = fd;
    info->receiver = receiver;
    info->replicaIdx = replicaIdx;
    info->acceptEvent = event_new(libeventBase,
                                  fd,
                                  EV_READ | EV_PERSIST,
                                  TCPAcceptCallback,
                                  (void *)info);
    event_add(info->acceptEvent, NULL);
    tcpListeners.push_back(info);

    // Tell the receiver its address
    socklen_t sinsize = sizeof(sin);
    if (getsockname(fd, (sockaddr *) &sin, &sinsize) < 0) {
        PPanic("Failed to get socket name");
    }
    TCPTransportAddress *addr = new TCPTransportAddress(sin);
    receiver->SetAddress(addr);

    // Update mappings
    receivers[fd] = receiver;
    fds[receiver] = fd;

    Debug("Accepting connections on TCP port %hu", ntohs(sin.sin_port));
}


bool TCPTransport::OrderedMulticast(TransportReceiver *src,
    const std::vector<int> &groups, const Message &m) {
  Panic("Not implemented :(.");
}



bool
TCPTransport::SendMessageInternal(TransportReceiver *src,
                                  const TCPTransportAddress &dst,
                                  const Message &m)
{

    Debug("Sending %s message over TCP to %s:%d",
        m.GetTypeName().c_str(), inet_ntoa(dst.addr.sin_addr),
        htons(dst.addr.sin_port));
    auto dstSrc = std::make_pair(dst, src);

    mtx.lock();
    auto kv = tcpOutgoing.find(dstSrc);
    // See if we have a connection open
    if (kv == tcpOutgoing.end()) {
        ConnectTCP(dstSrc);
        kv = tcpOutgoing.find(dstSrc);
    }

    struct bufferevent *ev = kv->second;
    mtx.unlock();

    UW_ASSERT(ev != NULL);

    // Serialize message
    string data;
    UW_ASSERT(m.SerializeToString(&data));
    string type = m.GetTypeName();
    size_t typeLen = type.length();
    size_t dataLen = data.length();
    size_t totalLen = (typeLen + sizeof(typeLen) +
                       dataLen + sizeof(dataLen) +
                       sizeof(totalLen) +
                       sizeof(uint32_t));

    Debug("Message is %lu total bytes", totalLen);

    char buf[totalLen];
    char *ptr = buf;

    *((uint32_t *) ptr) = MAGIC;
    ptr += sizeof(uint32_t);
    UW_ASSERT((size_t)(ptr-buf) < totalLen);

    *((size_t *) ptr) = totalLen;
    ptr += sizeof(size_t);
    UW_ASSERT((size_t)(ptr-buf) < totalLen);

    *((size_t *) ptr) = typeLen;
    ptr += sizeof(size_t);
    UW_ASSERT((size_t)(ptr-buf) < totalLen);

    UW_ASSERT((size_t)(ptr+typeLen-buf) < totalLen);
    memcpy(ptr, type.c_str(), typeLen);
    ptr += typeLen;
    *((size_t *) ptr) = dataLen;
    ptr += sizeof(size_t);

    UW_ASSERT((size_t)(ptr-buf) < totalLen);
    UW_ASSERT((size_t)(ptr+dataLen-buf) == totalLen);
    memcpy(ptr, data.c_str(), dataLen);
    ptr += dataLen;

    //mtx.lock();
    //evbuffer_lock(ev);
    if (bufferevent_write(ev, buf, totalLen) < 0) {
        Warning("Failed to write to TCP buffer");
        fprintf(stderr, "tcp write failed\n");
        //evbuffer_unlock(ev);
        //mtx.unlock();
        return false;
    }
    //evbuffer_unlock(ev);
    //mtx.unlock();
  

    /*Latency_Start(&sockWriteLat);
    if (write(ev->ev_write.ev_fd, buf, totalLen) < 0) {
      Warning("Failed to write to TCP buffer");
      return false;
    }
    Latency_End(&sockWriteLat);*/
    return true;
}

// void TCPTransport::Flush() {
//   event_base_loop(libeventBase, EVLOOP_NONBLOCK);
// }

void
TCPTransport::Run()
{
    //stopped = false;
    int ret = event_base_dispatch(libeventBase);
    Debug("event_base_dispatch returned %d.", ret);
}

void
TCPTransport::Stop()
{
  // TODO: cleaning up TCP connections needs to be done better
  // - We want to close connections from client side when we kill clients so that
  //   server doesn't see many connections in TIME_WAIT and run out of file descriptors
  // - This is mainly a problem if the client is still running long after it should have
  //   finished (due to abort loops)

 //XXX old version
  // if (!stopped) {
  //
  //   auto stopFn = [this](){
  //     if (!stopped) {
  //       stopped = true;
  //       mtx.lock();
  //       for (auto itr = tcpOutgoing.begin(); itr != tcpOutgoing.end(); ) {
  //         tcpAddresses.erase(itr->second);
  //         bufferevent_free(itr->second);
  //         itr = tcpOutgoing.erase(itr);
  //       }
  //       event_base_dump_events(libeventBase, stderr);
  //       event_base_loopbreak(libeventBase);
  //       tp.stop();
  //       //delete tp;
  //       mtx.unlock();
  //     }
  //   };
  //   if (immediately) {
  //     stopFn();
  //   } else {
  //     Timer(500, stopFn);
  //   }
  // }

  // mtx.lock();
  // for (auto itr = tcpOutgoing.begin(); itr != tcpOutgoing.end(); ) {
  //   bufferevent_free(itr->second);
  //   tcpAddresses.erase(itr->second);
  //   itr = tcpOutgoing.erase(itr);
  // }

  tp.stop();
  event_base_dump_events(libeventBase, stderr);

  //mtx.unlock();
}

void TCPTransport::Close(TransportReceiver *receiver) {
  mtx.lock();
  for (auto itr = tcpOutgoing.begin(); itr != tcpOutgoing.end(); ++itr) {
    if (itr->first.second == receiver) {
      bufferevent_free(itr->second);
      tcpOutgoing.erase(itr);
      tcpAddresses.erase(itr->second);
      break;
    }
  }
  mtx.unlock();
}



int TCPTransport::Timer(uint64_t ms, timer_callback_t cb) {
  struct timeval tv;
  tv.tv_sec = ms/1000;
  tv.tv_usec = (ms % 1000) * 1000;

  return TimerInternal(tv, cb);
}

int TCPTransport::TimerMicro(uint64_t us, timer_callback_t cb) {
  struct timeval tv;
  tv.tv_sec = us / 1000000UL;
  tv.tv_usec = us % 1000000UL;

  return TimerInternal(tv, cb);
}

int TCPTransport::TimerInternal(struct timeval &tv, timer_callback_t cb) {
  std::unique_lock<std::shared_mutex> lck(timer_mtx);

  TCPTransportTimerInfo *info = new TCPTransportTimerInfo();

  ++lastTimerId;

  info->transport = this;
  info->id = lastTimerId;
  info->cb = cb;
  info->ev = event_new(libeventBase, -1, 0, TimerCallback, info);

  timers[info->id] = info;

  event_add(info->ev, &tv);

  return info->id;
}

bool
TCPTransport::CancelTimer(int id)
{
    std::unique_lock<std::shared_mutex> lck(timer_mtx);
    TCPTransportTimerInfo *info = timers[id];

    if (info == NULL) {
        return false;
    }

    timers.erase(info->id);
    event_del(info->ev);
    event_free(info->ev);
    delete info;

    return true;
}

void
TCPTransport::CancelAllTimers()
{
    timer_mtx.lock();
    while (!timers.empty()) {
        auto kv = timers.begin();
        int id = kv->first;
        timer_mtx.unlock();
        CancelTimer(id);
        timer_mtx.lock();
    }
    timer_mtx.unlock();
}

void
TCPTransport::OnTimer(TCPTransportTimerInfo *info)
{
    {
	    std::unique_lock<std::shared_mutex> lck(timer_mtx);

	    timers.erase(info->id);
	    event_del(info->ev);
	    event_free(info->ev);
    }

    info->cb();

    delete info;
}

void
TCPTransport::TimerCallback(evutil_socket_t fd, short what, void *arg)
{
    TCPTransport::TCPTransportTimerInfo *info = (TCPTransport::TCPTransportTimerInfo *)arg;

    UW_ASSERT(what & EV_TIMEOUT);

    info->transport->OnTimer(info);
}

void TCPTransport::DispatchTP(std::function<void*()> f, std::function<void(void*)> cb)  {
  tp.dispatch(std::move(f), std::move(cb), libeventBase);
}

void TCPTransport::DispatchTP_local(std::function<void*()> f, std::function<void(void*)> cb)  {
  tp.dispatch_local(std::move(f), std::move(cb));
}

void TCPTransport::DispatchTP_noCB(std::function<void*()> f) {
  tp.detach(std::move(f));
}
void TCPTransport::DispatchTP_noCB_ptr(std::function<void*()> *f) {
  tp.detach_ptr(f);
}
void TCPTransport::DispatchTP_main(std::function<void*()> f) {
  tp.detach_main(std::move(f));
}
void TCPTransport::IssueCB(std::function<void(void*)> cb, void* arg){
  //std::lock_guard<std::mutex> lck(mtx);
  tp.issueCallback(std::move(cb), arg, libeventBase);
}
void TCPTransport::IssueCB_main(std::function<void(void*)> cb, void* arg){
  tp.issueMainThreadCallback(std::move(cb), arg);
}

void TCPTransport::CancelLoadBonus(){
  tp.cancel_load_threads();
}

void TCPTransport::AddIndexedThreads(int num_threads){
  tp.add_n_indexed(num_threads);
}
void TCPTransport::DispatchIndexedTP(uint64_t id, std::function<void *()> f, std::function<void(void *)> cb){
  tp.dispatch_indexed(id, std::move(f), std::move(cb), libeventBase);
}
void TCPTransport::DispatchIndexedTP_noCB(uint64_t id, std::function<void *()> f){
  tp.detach_indexed(id, std::move(f));
}



void
TCPTransport::LogCallback(int severity, const char *msg)
{
    Message_Type msgType;
    switch (severity) {
    case _EVENT_LOG_DEBUG:
        msgType = MSG_DEBUG;
        break;
    case _EVENT_LOG_MSG:
        msgType = MSG_NOTICE;
        break;
    case _EVENT_LOG_WARN:
        msgType = MSG_WARNING;
        break;
    case _EVENT_LOG_ERR:
        msgType = MSG_WARNING;
        break;
    default:
        NOT_REACHABLE();
    }

    _Message(msgType, "libevent", 0, NULL, "%s", msg);
}

void
TCPTransport::FatalCallback(int err)
{
    Panic("Fatal libevent error: %d", err);
}

void
TCPTransport::SignalCallback(evutil_socket_t fd, short what, void *arg)
{
    Debug("Terminating on SIGTERM/SIGINT");
    TCPTransport *transport = (TCPTransport *)arg;
    event_base_loopbreak(transport->libeventBase);
}

void
TCPTransport::TCPAcceptCallback(evutil_socket_t fd, short what, void *arg)
{
    TCPTransportTCPListener *info = (TCPTransportTCPListener *)arg;
    TCPTransport *transport = info->transport;

    if (what & EV_READ) {
        int newfd;
        struct sockaddr_in sin;
        socklen_t sinLength = sizeof(sin);
        struct bufferevent *bev;

        // Accept a connection
        if ((newfd = accept(fd, (struct sockaddr *)&sin,
                            &sinLength)) < 0) {
            PWarning("Failed to accept incoming TCP connection");
            return;
        }

        // Put it in non-blocking mode
        if (fcntl(newfd, F_SETFL, O_NONBLOCK, 1)) {
            PWarning("Failed to set O_NONBLOCK");
        }

            // Set TCP_NODELAY
        int n = 1;
        if (setsockopt(newfd, IPPROTO_TCP,
                       TCP_NODELAY, (char *)&n, sizeof(n)) < 0) {
            PWarning("Failed to set TCP_NODELAY on TCP listening socket");
        }

        // Create a buffered event
        bev = bufferevent_socket_new(transport->libeventBase, newfd,
                                     BEV_OPT_CLOSE_ON_FREE | BEV_OPT_THREADSAFE);
        bufferevent_setcb(bev, TCPReadableCallback, NULL,
                          TCPIncomingEventCallback, info);
        if (bufferevent_enable(bev, EV_READ|EV_WRITE) < 0) {
            Panic("Failed to enable bufferevent");
        }
    info->connectionEvents.push_back(bev);
	TCPTransportAddress client = TCPTransportAddress(sin);

  transport->mtx.lock();
  auto dstSrc = std::make_pair(client, info->receiver);
	transport->tcpOutgoing[dstSrc] = bev;
	transport->tcpAddresses.insert(pair<struct bufferevent*,
        pair<TCPTransportAddress, TransportReceiver *>>(bev, dstSrc));
  transport->mtx.unlock();

    Debug("Opened incoming TCP connection from %s:%d",
               inet_ntoa(sin.sin_addr), htons(sin.sin_port));
    }
}

void
TCPTransport::TCPReadableCallback(struct bufferevent *bev, void *arg)
{
    TCPTransportTCPListener *info = (TCPTransportTCPListener *)arg;
    TCPTransport *transport = info->transport;
    struct evbuffer *evbuf = bufferevent_get_input(bev);

    while (evbuffer_get_length(evbuf) > 0) {
        uint32_t *magic;
        magic = (uint32_t *)evbuffer_pullup(evbuf, sizeof(*magic));
        if (magic == NULL) {
            return;
        }
        UW_ASSERT(*magic == MAGIC);

        size_t *sz;
        unsigned char *x = evbuffer_pullup(evbuf, sizeof(*magic) + sizeof(*sz));

        sz = (size_t *) (x + sizeof(*magic));
        if (x == NULL) {
            return;
        }
        size_t totalSize = *sz;
        UW_ASSERT(totalSize < 1073741826);

        if (evbuffer_get_length(evbuf) < totalSize) {
            //Debug("Don't have %ld bytes for a message yet, only %ld",
            //      totalSize, evbuffer_get_length(evbuf));
            return;
        }
        // Debug("Receiving %ld byte message", totalSize);

        char buf[totalSize];
        size_t copied = evbuffer_remove(evbuf, buf, totalSize);
        UW_ASSERT(copied == totalSize);

        // Parse message
        char *ptr = buf + sizeof(*sz) + sizeof(*magic);

        size_t typeLen = *((size_t *)ptr);
        ptr += sizeof(size_t);
        UW_ASSERT((size_t)(ptr-buf) < totalSize);

        UW_ASSERT((size_t)(ptr+typeLen-buf) < totalSize);
        string msgType(ptr, typeLen);
        ptr += typeLen;

        size_t msgLen = *((size_t *)ptr);
        ptr += sizeof(size_t);
        UW_ASSERT((size_t)(ptr-buf) < totalSize);

        UW_ASSERT((size_t)(ptr+msgLen-buf) <= totalSize);
        string msg(ptr, msgLen);
        ptr += msgLen;

        transport->mtx.lock_shared();
        auto addr = transport->tcpAddresses.find(bev);
        TCPTransportAddress &ad = addr->second.first; //Note: if address was removed from map, ref could still be in "use" by server
        // transport->mtx.unlock();
        // UW_ASSERT(addr != transport->tcpAddresses.end());
        //
        // // Dispatch
        //
        // info->receiver->ReceiveMessage(ad, msgType, msg, nullptr);
        // Debug("Done processing large %s message", msgType.c_str());

        if (addr == transport->tcpAddresses.end()) {
         Warning("Received message for closed connection.");
         transport->mtx.unlock_shared();
       } else {
         // Dispatch
         transport->mtx.unlock_shared();
         Debug("Received %lu bytes %s message.", totalSize, msgType.c_str());
         info->receiver->ReceiveMessage(ad, msgType, msg, nullptr);
         // Debug("Done processing large %s message", msgType.c_str());
       }
    }
}


void
TCPTransport::TCPIncomingEventCallback(struct bufferevent *bev,
                                       short what, void *arg)
{
    if (what & BEV_EVENT_ERROR) {
      fprintf(stderr,"tcp incoming error\n");
        Debug("Error on incoming TCP connection: %s\n",
                evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR()));
        bufferevent_free(bev);
        return;
    } else if (what & BEV_EVENT_ERROR) {
      fprintf(stderr,"tcp incoming eof\n");
        Debug("EOF on incoming TCP connection\n");
        bufferevent_free(bev);
        return;
    }
}

//Note: If ever to make client multithreaded, add mutexes here. (same for ConnectTCP)
void
TCPTransport::TCPOutgoingEventCallback(struct bufferevent *bev,
                                       short what, void *arg)
{
    TCPTransportTCPListener *info = (TCPTransportTCPListener *)arg;
    TCPTransport *transport = info->transport;
    transport->mtx.lock_shared();
    auto it = transport->tcpAddresses.find(bev);

    UW_ASSERT(it != transport->tcpAddresses.end());
    TCPTransportAddress addr = it->second.first;
    transport->mtx.unlock_shared();

    if (what & BEV_EVENT_CONNECTED) {
        Debug("Established outgoing TCP connection to server [g:%d][r:%d]", info->groupIdx, info->replicaIdx);
    } else if (what & BEV_EVENT_ERROR) {
        Warning("Error on outgoing TCP connection to server [g:%d][r:%d]: %s",
                info->groupIdx, info->replicaIdx,
                evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR()));
        bufferevent_free(bev);

        transport->mtx.lock();
        auto it2 = transport->tcpOutgoing.find(std::make_pair(addr, info->receiver));
        transport->tcpOutgoing.erase(it2);
        transport->tcpAddresses.erase(bev);
        transport->mtx.unlock();

        return;
    } else if (what & BEV_EVENT_EOF) {
        Warning("EOF on outgoing TCP connection to server");
        bufferevent_free(bev);

        transport->mtx.lock();
        auto it2 = transport->tcpOutgoing.find(std::make_pair(addr, info->receiver));
        transport->tcpOutgoing.erase(it2);
        transport->tcpAddresses.erase(bev);
        transport->mtx.unlock();

        return;
    }
}
