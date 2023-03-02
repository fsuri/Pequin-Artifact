#ifndef _HOTSTUFF_INTERFACE_H_
#define _HOTSTUFF_INTERFACE_H_

#include <string>
#include <functional>
#include "remote_config_dir.h"
using std::string;

namespace hotstuffstore {
    class IndicusInterface {
        typedef std::function<void(const std::string&, uint32_t seqnum)> hotstuff_exec_callback;
        //const std::string config_dir_base = "/home/yunhao/florian/BFT-DB/src/store/hotstuffstore/libhotstuff/conf-indicus/";

        // on CloudLab
        //const std::string config_dir_base = "/users/fs435/config/";
        const std::string config_dir_base = REMOTE_CONFIG_DIR;

        int shardId;
        int replicaId;
        int cpuId;

        void initialize(int argc, char** argv);

    public:
        IndicusInterface(int shardId, int replicaId, int cpuId);

        void propose(const std::string& hash, hotstuff_exec_callback execb);
    };
}

#endif /* _HOTSTUFF_APP_H_ */
