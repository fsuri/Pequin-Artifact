/***********************************************************************
 *
 * Copyright 2021 Florian Suri-Payer <fsp@cs.cornell.edu>
 *                Matthew Burke <matthelb@cs.cornell.edu>
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
#include "lib/threadpool.h"

#include <thread>
#include <sched.h>
#include <utility>
#include <iostream>

//TODO: make is so that all but the first core are used.
ThreadPool::ThreadPool() {

}

void ThreadPool::start(int process_id, int total_processes, bool hyperthreading, bool server, int mode){
  //printf("starting threadpool \n");
  //could pre-allocate some Events and EventInfos for a Hotstart
  if(server){
        fprintf(stderr, "starting server threadpool\n");
        fprintf(stderr, "process_id: %d, total_processes: %d \n", process_id, total_processes);
    //TODO: add config param for hyperthreading
    //bool hyperthreading = true;
    int num_cpus = 8;//std::thread::hardware_concurrency(); ///(2-hyperthreading);
        fprintf(stderr, "Total Num_cpus on server: %d \n", num_cpus);
    num_cpus /= total_processes;
        fprintf(stderr, "Num_cpus used for replica #%d: %d \n", process_id, num_cpus);
    int offset = process_id * num_cpus;
    uint32_t num_threads = (uint32_t) std::max(1, num_cpus);
    // Currently: First CPU = MainThread.
    running = true;

    int num_core_for_hotstuff;
    if (total_processes <= 2) {
        num_core_for_hotstuff = 1;
    } else {
        num_core_for_hotstuff = 0;
    }
    uint32_t start = 1;
    uint32_t end = num_threads;
    if(mode == 0){ //Indicus
       //Use defaults. First core is messagine (inactive in threadpool), second is Main Logic Thread, remainder are workers (crypto/reads/asynchronous handling)
    } 
    else if (mode == 1){ //TxHotstuff
      end = end - num_core_for_hotstuff; //use last core for Hotstuff only
    }
    else if(mode == 2){ //TxBFTSmart
      start = 0; // use all cores
    }
    else Panic("No valid system defined");
       
    Debug("Main Process running on CPU %d.", sched_getcpu());
    for (uint32_t i = start; i < end; i++) {    
        std::thread *t;
        //Mainthread
        if(i==1){
            t = new std::thread([this, i] {
                    while (true) {
                        std::function<void*()> job;
                        
                        Debug("Main Thread %d running on CPU %d", i, sched_getcpu());
                        main_thread_request_list.wait_dequeue(job);
                           
                        if (!running) {
                            break;
                        }
                        job();
                    }
                });
        }
        //Cryptothread
        else{
            t = new std::thread([this, i] {
                    while (true) {
                        std::pair<std::function<void*()>, EventInfo*> job;
                      
                        Debug("Worker Thread %d running on CPU %d", i, sched_getcpu());
                        worker_thread_request_list.wait_dequeue(job);
                           
                        Debug("popped job on CPU %d.", i);
                        if (!running) {
                            break;
                        }
                        if(job.second){
                            job.second->r = job.first();
                            // This _should_ be thread safe
                            event_active(job.second->ev, 0, 0);
                        }
                        else{
                            job.first();
                        }

                    }
                });
        }
        
        // Create a cpu_set_t object representing a set of CPUs. Clear it and mark
        // only CPU i as set.
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(i+offset, &cpuset);
        if(i+offset > 7) return; //XXX This is a hack to support the non-crypto experiment that does not actually use multiple cores 
        std::cerr << "THREADPOOL SETUP: Trying to pin thread to core: " << i << " + " << offset << std::endl;
        int rc = pthread_setaffinity_np(t->native_handle(),
                                        sizeof(cpu_set_t), &cpuset);
        if (rc != 0) {
            Panic("Error calling pthread_setaffinity_np: %d", rc);
        }
        threads.push_back(t);
        t->detach();
    }
  //}
  } else{
      fprintf(stderr, "starting client threadpool\n");
      int num_cpus = std::thread::hardware_concurrency(); ///(2-hyperthreading);
      fprintf(stderr, "Num_cpus: %d \n", num_cpus);
      num_cpus /= total_processes;
      num_cpus = 8; //XXX change back to dynamic
      //int offset = process_id * num_cpus;
      Debug("num cpus %d", num_cpus);
      uint32_t num_threads = (uint32_t) std::max(1, num_cpus);
      running = true;
      for (uint32_t i = 0; i < num_threads; i++) {
          std::thread *t;
          t = new std::thread([this, i] {
                  while (true) {
                      std::pair<std::function<void*()>, EventInfo*> job;
                      
                      Debug("Thread %d running on CPU %d.", i, sched_getcpu());
                      worker_thread_request_list.wait_dequeue(job);
                         
                      if (!running) {
                          break;
                      }
          
                      if(job.second){
                          job.second->r = job.first();
                          event_active(job.second->ev, 0, 0);
                      }
                      else{
                          job.first();
                      }
                  }
              });
          cpu_set_t cpuset;
          CPU_ZERO(&cpuset);
          CPU_SET(i, &cpuset);
          int rc = pthread_setaffinity_np(t->native_handle(),
                                          sizeof(cpu_set_t), &cpuset);
          if (rc != 0) {
              Panic("Error calling pthread_setaffinity_np: %d", rc);
          }
          Debug("MainThread running on CPU %d.", sched_getcpu());
          threads.push_back(t);
          t->detach();
      }
  }
}

ThreadPool::~ThreadPool()
{
  stop();

}

void ThreadPool::stop() {
  running = false;
  
 // for(auto t: threads){
 //    t->join();
 //    delete t;
 // }
}


void ThreadPool::EventCallback(evutil_socket_t fd, short what, void *arg) {
  // we want to run the callback in the main event loop
  EventInfo* info = (EventInfo*) arg;
  info->cb(info->r);

  info->tp->FreeEvent(info->ev);
  info->tp->FreeEventInfo(info);
}


void ThreadPool::dispatch(std::function<void*()> f, std::function<void(void*)> cb, event_base* libeventBase) {
  
  EventInfo* info = GetUnusedEventInfo();
  info->cb = std::move(cb);
  info->ev = GetUnusedEvent(libeventBase, info);
  event_add(info->ev, NULL);

  worker_thread_request_list.enqueue(std::make_pair(std::move(f), info));
}

void* ThreadPool::combiner(std::function<void*()> f, std::function<void(void*)> cb){
  cb(f());
  return nullptr;
}

void ThreadPool::dispatch_local(std::function<void*()> f, std::function<void(void*)> cb){
  EventInfo* info = nullptr;
  auto combination = [f = std::move(f), cb = std::move(cb)](){cb(f()); return nullptr;};
  
  worker_thread_request_list.enqueue(std::make_pair(std::move(combination), info));
}

void ThreadPool::detatch(std::function<void*()> f){
  EventInfo* info = nullptr;
  
  worker_thread_request_list.enqueue(std::make_pair(std::move(f), info));

}

void ThreadPool::detatch_ptr(std::function<void*()> *f){
  EventInfo* info = nullptr;
  
  worker_thread_request_list.enqueue(std::make_pair(std::move(*f), info));

}

void ThreadPool::detatch_main(std::function<void*()> f){
  EventInfo* info = nullptr;

  main_thread_request_list.enqueue(std::move(f));
}

////////////////////////////////
//requires transport object to call this... (add to the verifyObj)
//could alternatively use:
// transport->Timer(0, f)   // expects a timer_callback_t though, which is a void(void) typedef
//could make f purely void, if I refactored a bunch
//lazy solution:
// transport->Timer(0, [](){f(new bool(true));})
void ThreadPool::issueCallback(std::function<void(void*)> cb, void* arg, event_base* libeventBase){
  EventInfo* info = GetUnusedEventInfo(); //new EventInfo(this);
  info->cb = std::move(cb);
  info->r = arg;
  //info->ev = event_new(libeventBase, -1, 0, ThreadPool::EventCallback, info);
  info->ev = GetUnusedEvent(libeventBase, info);
  event_add(info->ev, NULL);
  event_active(info->ev, 0, 0);
}

void ThreadPool::issueMainThreadCallback(std::function<void(void*)> cb, void* arg){

  auto f = [cb, arg](){
    cb(arg);
    return (void*) true;
  };
  main_thread_request_list.enqueue(std::move(f));
}

////////////////////////////////////////


ThreadPool::EventInfo* ThreadPool::GetUnusedEventInfo() {
  std::unique_lock<std::mutex> lock(EventInfoMutex);
  EventInfo *info;
  if (eventInfos.size() > 0) {
    info = eventInfos.back();
    eventInfos.pop_back();
  } else {
    info = new EventInfo(this);
  }
  return info;
}

void ThreadPool::FreeEventInfo(EventInfo *info) {
  std::unique_lock<std::mutex> lock(EventInfoMutex);
  eventInfos.push_back(info);
}

event* ThreadPool::GetUnusedEvent(event_base* libeventBase, EventInfo* info) {
  std::unique_lock<std::mutex> lock(EventMutex);
  event* event;
  if (events.size() > 0) {
    event = events.back();
    events.pop_back();
    event_assign(event, libeventBase, -1, 0, ThreadPool::EventCallback, info);
  } else {
    event = event_new(libeventBase, -1, 0, ThreadPool::EventCallback, info);
  }
  return event;
}

void ThreadPool::FreeEvent(event* event) {
  std::unique_lock<std::mutex> lock(EventMutex);
  event_del(event);
  events.push_back(event);
}
