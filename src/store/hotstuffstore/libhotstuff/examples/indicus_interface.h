#ifndef _HOTSTUFF_INTERFACE_H_
#define _HOTSTUFF_INTERFACE_H_

#include <string>
#include <functional>
#include "local_config_dir.h"
#include "remote_config_dir.h"
// #define local_config
using std::string;

namespace hotstuffstore {
    class IndicusInterface {
        typedef std::function<void(const std::string&, uint32_t seqnum)> hotstuff_exec_callback;
        //const std::string config_dir_base = "/home/yunhao/florian/BFT-DB/src/store/hotstuffstore/libhotstuff/conf-indicus/";
        // const std::string config_dir_base = "/root/Pequin-Artifact/src/scripts/config/";

        // on CloudLab
        // #if defined(local_config)
        const std::string config_dir_base = LOCAL_CONFIG_DIR;
        // #else
            // const std::string config_dir_base = REMOTE_CONFIG_DIR;
        // #endif


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
