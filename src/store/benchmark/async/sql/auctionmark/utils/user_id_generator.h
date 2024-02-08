#ifndef AUCTIONMARK_USER_ID_GENERATOR_H
#define AUCTIONMARK_USER_ID_GENERATOR_H

#include <iostream>
#include <vector>
#include <map>
#include <algorithm>
#include <iterator>
#include "store/benchmark/async/sql/auctionmark/utils/user_id.h"
#include <boost/histogram.hpp>
#include <optional>

using axes_t = std::tuple<
    boost::histogram::axis::integer<>,
    boost::histogram::axis::integer<>
>;
using hist_t = boost::histogram::histogram<axes_t>;

namespace auctionmark {

class UserIdGenerator {
public:
    UserIdGenerator(const hist_t& users_per_item_count, int num_clients, int client_id = -1);

    long get_total_users() const;
    void set_current_item_count(int size);
    int get_current_position() const;
    std::optional<UserId> seek_to_position(int position);
    bool check_client(const UserId& user_id) const;
    bool has_next();
    UserId next();
    std::string to_string() const;

private:
    std::vector<int> users_per_item_counts;
    int num_clients;
    int client_id;
    int min_item_count;
    int max_item_count;
    long total_users;
    std::optional<UserId> _next;
    int current_item_count;
    int current_offset;
    int current_position;

    std::optional<UserId> find_next_user_id();
};

} // namespace auctionmark

#endif // AUCTIONMARK_USER_ID_GENERATOR_H