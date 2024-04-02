#ifndef _HOTSTUFF_INTERFACE_H_
#define _HOTSTUFF_INTERFACE_H_

#include <string>
#include <functional>
#include <filesystem>

#include "local_config_dir.h"
#include "remote_config_dir.h"
using std::string;

namespace hotstuffstore {
    class IndicusInterface {
        typedef std::function<void(const std::string&, uint32_t seqnum)> hotstuff_exec_callback;

        int shardId;
        int replicaId;
        int cpuId;
        bool local_config;

        void initialize(int argc, char** argv);

    public:
        IndicusInterface(int shardId, int replicaId, int cpuId, bool local_config=false);

        void propose(const std::string& hash, hotstuff_exec_callback execb);
    };
}

#endif /* _HOTSTUFF_APP_H_ */
