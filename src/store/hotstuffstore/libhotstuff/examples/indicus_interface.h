#ifndef _HOTSTUFF_INTERFACE_H_
#define _HOTSTUFF_INTERFACE_H_

#include <string>
#include <functional>
#include <filesystem>

// #include "../salticidae/include/salticidae/endian.h"
// #include "../salticidae/include/salticidae/type.h"
// #include "../salticidae/include/salticidae/stream.h"
#include "local_config_dir.h"
#include "remote_config_dir.h"
using std::string;

namespace hotstuffstore {
    class IndicusInterface {
       // typedef std::function<void(salticidae::uint256_t cmd_hash, uint32_t seqnum)> smr_callback;
        typedef std::function<void(const std::string&, uint32_t seqnum)> smr_callback; //Alternative: Turn cmd_hash back into digest? Might not be possible.
        typedef std::function<void(const std::string&, uint32_t seqnum)> hotstuff_exec_callback;

        int shardId;
        int replicaId;
        int cpuId;
        bool local_config;

        void initialize(int argc, char** argv);

    public:
        IndicusInterface(int shardId, int replicaId, int cpuId, bool local_config=true);

        void register_smr_callback(smr_callback smr_cb);

        void propose(const std::string& hash, hotstuff_exec_callback execb);
    };
}

#endif /* _HOTSTUFF_APP_H_ */
