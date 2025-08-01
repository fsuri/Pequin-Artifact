/**
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

#ifndef _HOTSTUFF_WORKER_H
#define _HOTSTUFF_WORKER_H

#include <thread>
#include <unordered_map>
#include <unistd.h>

#include "salticidae/event.h"
#include "hotstuff/util.h"

namespace hotstuff {

  extern int hotstuff_core_offset;

class VeriTask {
    friend class VeriPool;
    bool result;
    public:
    virtual bool verify() = 0;
    virtual ~VeriTask() = default;
};

using salticidae::ThreadCall;
using veritask_ut = BoxObj<VeriTask>;
using mpmc_queue_t = salticidae::MPMCQueueEventDriven<VeriTask *>;
using mpsc_queue_t = salticidae::MPSCQueueEventDriven<VeriTask *>;

class VeriPool {
    mpmc_queue_t in_queue;
    mpsc_queue_t out_queue;

    struct Worker {
        std::thread handle;
        EventContext ec;
        BoxObj<ThreadCall> tcall;
    };

    std::vector<Worker> workers;
    std::unordered_map<VeriTask *, std::pair<veritask_ut, promise_t>> pms;

    public:
    VeriPool(EventContext ec, size_t nworker, size_t burst_size = 128) {
        out_queue.reg_handler(ec, [this, burst_size](mpsc_queue_t &q) {
            
            // struct timespec ts_start;
            // clock_gettime(CLOCK_MONOTONIC, &ts_start);
            // uint64_t microseconds_start = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;
            // fprintf(stderr, "[CPU:%d]: finished task at [%lu]\n", sched_getcpu(), microseconds_start);

            size_t cnt = burst_size;
            VeriTask *task;
            while (q.try_dequeue(task))
            {
                auto it = pms.find(task);
                it->second.second.resolve(task->result);
                pms.erase(it);
                if (!--cnt) return true;
            }
            return false;
        });

        workers.resize(nworker);
        for (size_t i = 0; i < nworker; i++)
        {
            in_queue.reg_handler(workers[i].ec, [this, burst_size, i](mpmc_queue_t &q) {
              // cpu_set_t cpuset;
              // CPU_ZERO(&cpuset);
              // int k = 0;
              // if(i!=0) k = i + 1;
              // CPU_SET(hotstuff_core_offset + k, &cpuset);
              // int rc = pthread_setaffinity_np(pthread_self(),
              //                                 sizeof(cpu_set_t), &cpuset);
                size_t cnt = burst_size;
                VeriTask *task;
                while (q.try_dequeue(task))
                {
                    //fprintf(stderr, "[CPU:%d]: Working on Verification\n", sched_getcpu());
                    HOTSTUFF_LOG_DEBUG("%lx working on %u", std::this_thread::get_id(), (uintptr_t)task);
                    task->result = task->verify();
                    out_queue.enqueue(task);
                    if (!--cnt) return true;
                }
                return false;
            });
        }
        for (auto &w: workers)
        {
            w.tcall = new ThreadCall(w.ec);
            w.handle = std::thread([ec=w.ec]() { ec.dispatch(); });
        }
    }

    ~VeriPool() {
        for (auto &w: workers)
            w.tcall->async_call([ec=w.ec](ThreadCall::Handle &) {
                ec.stop();
            });
        for (auto &w: workers)
            w.handle.join();
    }

    promise_t verify(veritask_ut &&task) {
      
        
        // struct timespec ts_start;
        // clock_gettime(CLOCK_MONOTONIC, &ts_start);
        // uint64_t microseconds_start = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;
        // fprintf(stderr, "[CPU:%d]: add task at [%lu]\n", sched_getcpu(), microseconds_start);

        auto ptr = task.get();
        auto ret = pms.insert(std::make_pair(ptr,
                std::make_pair(std::move(task), promise_t([](promise_t &){}))));
        assert(ret.second);
        in_queue.enqueue(ptr);
        return ret.first->second.second;
    }
};

}

#endif
