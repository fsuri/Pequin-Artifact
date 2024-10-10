// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * common/truetime.cc:
 *   A simulated TrueTime module
 *
 **********************************************************************/

#include "store/common/truetime.h"
#include<iostream>

TrueTime::TrueTime() {
    simError = 0;
    simSkew = 0;
}

TrueTime::TrueTime(uint64_t skew, uint64_t errorBound)
{
    simError = errorBound;
    if (skew == 0) {
        simSkew = 0;
    } else {
        struct timeval t1;
        gettimeofday(&t1, NULL);
        srand(t1.tv_sec + t1.tv_usec);
        uint64_t r = rand();
        simSkew = (r % skew) - (skew / 2);
    }

    Debug("TrueTime variance: skew=%lu error=%lu", simSkew, simError);
}    

uint64_t
TrueTime::GetTime()
{
    struct timeval now;
    uint64_t timestamp;

    gettimeofday(&now, NULL);

    now.tv_usec += simSkew;
    if (now.tv_usec > 999999) {
        now.tv_usec -= 1000000;
        now.tv_sec++;
    } else if (now.tv_usec < 0) {
        now.tv_usec += 1000000;
        now.tv_sec--;
    }

    //timestamp = ((uint64_t)now.tv_sec << 32) | (uint64_t) (now.tv_usec);
    //32 bit for seconds suffices until 2038
    //timestamp = ((uint64_t)now.tv_sec << 20) | ((uint64_t) now.tv_usec); // shifting 20 suffices, since u_sec < 2^20
    timestamp = ((uint64_t)now.tv_sec << 32) | ((uint64_t) now.tv_usec << 12); //leave space for client ids

    // //FIXME: REMOVE TEST: Isolate ids again and compare:
    // //top 32 bits
    // uint64_t ts_top = (uint64_t)now.tv_sec << 32;
    // uint64_t ts_bot = (uint64_t) now.tv_usec << 12;
    // uint64_t top_mask = 0xFFFFFFFF00000000;
    // uint64_t bot_mask = 0x00000000FFFFFFFF;
    // uint64_t top = timestamp & top_mask;  //(((uint64_t) 1 << 32 - 1) << 32);  //top 32 bits (wipe bottom)  
    // uint64_t bot = timestamp & bot_mask;  //(((uint64_t) 1 << 20 - 1));  //bottom 20 bits               
    // if(ts_top != top || ts_bot != bot) Panic("ts_top: %lx different than merged_top: %lx; OR: ts_bot: %lx different than merged_bot: %lx", ts_top, top, ts_bot, bot);

    Debug("Time: [%lu s][%lu us] -> TS: %lu", now.tv_sec,now.tv_usec,timestamp);
    //fprintf(stderr, "Time: %lu %lu %lu \n", now.tv_sec,now.tv_usec,timestamp);

    return timestamp;
}

void
TrueTime::GetTimeAndError(uint64_t &time, uint64_t &error)
{
   time = GetTime();
   error = simError;
}

uint64_t TrueTime::MStoTS(const uint64_t &time_milis){
    uint64_t second_comp = time_milis / 1000;
    uint64_t milisecond_remain = time_milis % 1000;
    uint64_t microsecond_comp =  milisecond_remain * 1000;
  
    uint64_t ts = (second_comp << 32) | (microsecond_comp << 12);
    return ts;
}

uint64_t TrueTime::TStoMS(const uint64_t &time_stamp)
{
    uint64_t second_comp = time_stamp >> 32;
    uint64_t microsecond_comp = (time_stamp - (second_comp << 32)) >> 12;

    uint64_t ms = second_comp * 1000 + microsecond_comp / 1000;
    return ms;
}