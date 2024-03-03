#ifndef _QUERY_EXECUTOR_H_
#define _QUERY_EXECUTOR_H_

#include "store/common/backend/versionstore_safe.h"
#include "store/common/backend/sql_engine/table_kv_encoder.h"
#include <vector> 
#include <string>
#include <unordered_map>

namespace toystore {
namespace executor {

// is there a way to read from a templated class without also creating a template?
template<class T, class V>
class QueryExecutor {
private: 
    std::unordered_map<std::string, std::string> table_to_key;
public: 
    QueryExecutor();
    ~QueryExecutor();

    using Write = std::tuple<int, T, V>; // index key, timestamp, value

    /** Register table with query executor */
    void addTable(const std::string &table_name, const std::string &primary_key_col);
    /** Returns values in table_name with index value in range [idx_low, idx_high]  */
    std::vector<V> scan(VersionedKVStore<T, V> &store, const std::string &table_name, int idx_low, int idx_high);
    /** Inserts values into store under table*/
    void insert(VersionedKVStore<T, V> &store, const std::string &table_name, std::vector<Write> &vals);
};

template<class T, class V>
QueryExecutor<T, V>::QueryExecutor() {}

template<class T, class V>
QueryExecutor<T, V>::~QueryExecutor() {}

template<class T, class V>
void QueryExecutor<T, V>::addTable(const std::string &table_name, const std::string &primary_key_col) {
    std::vector<std::string> cols {primary_key_col};
    table_to_key[table_name] = EncodeTableRow(table_name, cols);
}

template<class T, class V>
std::vector<V> QueryExecutor<T,V>::scan(VersionedKVStore<T, V> &store, const std::string &table_name, int low, int high) {
    std::string base = table_to_key[table_name]
    std::vector<V> ret;
    for (int idx = low; idx <= high; idx++) {
        std::pair<T, V> res;
        std::string key = base + std::to_string(idx);
        if (store.get(key, res)) ret.push_back(res.second);
    }
    return ret;
}

template<class T, class V>
void QueryExecutor<T, V>::insert(VersionedKVStore<T, V> &store, const std::string &table_name, std::vector<QueryExecutor<T, V>::Write> &vals) {
    std::string base table_to_key[table_name];
    for (auto v : vals) {
        std::string key = base + std::to_string(v[0]);
        store.put(key, v[1], v[2]);
    }
}
}
}
#endif  /* _QUERY_EXECUTOR_H_ */
