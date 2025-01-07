#include "store/benchmark/async/sql/auctionmark/utils/user_id_generator.h"

namespace auctionmark {

//UserIdGenerator::UserIdGenerator(const std::vector<int> &users_per_item_count, int num_clients, int client_id)
UserIdGenerator::UserIdGenerator(const std::map<int, int> &users_per_item_count, int num_clients, int client_id)
    : num_clients(num_clients), client_id(client_id), min_item_count(0), 
    max_item_count(0), total_users(0), _next(std::nullopt), current_item_count(-1), current_offset(0), 
    current_position(0) { //, users_per_item_counts(users_per_item_count) {

    if (num_clients <= 0) {
      throw std::invalid_argument("num_clients must be more than 0: " + std::to_string(num_clients));
    }
    if (client_id != -1 && client_id < 0) {
      throw std::invalid_argument("client_id must be more than or equal to 0: " + std::to_string(client_id));
    }

    // // TODO: Validate that this is the correct axis
    max_item_count = users_per_item_count.rbegin()->first;    //static_cast<int>(*users_per_item_count.axis(0).end());
    users_per_item_counts.resize(max_item_count + 2);
    for (int i = 0; i < users_per_item_counts.size(); i++) {
      try {
        users_per_item_counts[i] = users_per_item_count.at(i);
      }
      catch(...){
        users_per_item_counts[i] = 0;
      }
      
    }

    // TODO: Validate that this is the correct axis
    min_item_count = users_per_item_count.begin()->first;//static_cast<int>(*users_per_item_count.axis(0).begin());

    total_users = 0;
    for (const auto& count : users_per_item_counts) {
      total_users += count;
    }

    set_current_item_count(min_item_count);


    fprintf(stderr, "UserIDGenerator. min_item_count: %d. max_item_count: %d, num_clients: %d. Total users: %d\n", min_item_count, max_item_count, num_clients, total_users);
}

long UserIdGenerator::get_total_users() const {
    return total_users;
}

void UserIdGenerator::set_current_item_count(int size) {
    current_position = 0;
    for (int i = 0; i < size; i++) {
      current_position += users_per_item_counts[i];
    }
   
    current_item_count = size;
    current_offset = users_per_item_counts[current_item_count];
    //std::cerr << "current position: " << current_position << ". curr item_count: " << current_item_count << std::endl;
}

int UserIdGenerator::get_current_position() const {
    return current_position;
}

std::optional<UserId> UserIdGenerator::seek_to_position(int position) {
    
    std::optional<UserId> user_id;

    current_position = 0;
    current_item_count = 0;
    while (true) {
      int num_users = users_per_item_counts[current_item_count];

      //std::cerr << "loop sum: " << (current_position + num_users) << std::endl;
      if (current_position + num_users > position) {
        _next = std::nullopt;
        current_offset = num_users - (position - current_position);
        // std::cerr << "num users: " << num_users << std::endl;
        // std::cerr << "position: " << position << std::endl;
        // std::cerr << "current_position: " << current_position << std::endl;
        // std::cerr << "current_offset " << current_offset << std::endl;
        current_position = position;
        user_id = next();
        break;
      } else {
        current_position += num_users;
      }
      current_item_count++;
    }
    return std::move(user_id);
}

//returns true if the given UserID should be processed by the given client id
bool UserIdGenerator::check_client(const UserId& user_id) const {
    if (client_id == -1) {
      return true;
    }

    int tmp_count = 0;
    int tmp_position = 0;
    while (tmp_count <= max_item_count) {
      int num_users = users_per_item_counts[tmp_count];
      if (tmp_count == user_id.get_item_count()) {
        tmp_position += (num_users - user_id.get_offset()) + 1;
        break;
      }
      tmp_position += num_users;
      tmp_count++;
    }
    return tmp_position % num_clients == client_id;
}

bool UserIdGenerator::has_next() {
    if (!_next.has_value()) {
      _next = find_next_user_id();
    }
    return _next.has_value();
}

std::optional<UserId> UserIdGenerator::next() {
    if (!_next.has_value()) {
      _next = find_next_user_id();
    }
    auto ret = _next;
    _next = std::nullopt;
    return ret;
}

std::string UserIdGenerator::to_string() const {
    std::map<std::string, std::string> m;
    m["num_clients"] = std::to_string(num_clients);
    m["client_id"] = std::to_string(client_id);
    m["min_item_count"] = std::to_string(min_item_count);
    m["max_item_count"] = std::to_string(max_item_count);
    m["total_users"] = std::to_string(total_users);
    m["current_item_count"] = std::to_string(current_item_count);
    m["current_offset"] = std::to_string(current_offset);
    m["current_position"] = std::to_string(current_position);
    m["_next"] = _next.has_value() ? "not null" : "null";
    std::string users_per_item_countstr = "[Length:" + std::to_string(users_per_item_counts.size()) + "] => [";
    for (const auto& count : users_per_item_counts) {
      users_per_item_countstr += std::to_string(count) + ", ";
    }
    users_per_item_countstr += "]";
    m["users_per_item_count"] = users_per_item_countstr;
    std::string result;
    for (const auto& entry : m) {
      result += entry.first + ": " + entry.second + "\n";
    }
    return result;
}

std::optional<UserId> UserIdGenerator::find_next_user_id() {
    int found = -1;
    //std::cerr << "find next user. curr_item_cnt: " << current_item_count << ". max_item count: " << max_item_count << std::endl; 
    
    while (current_item_count <= max_item_count) {
      //std::cerr << "curr_item_cnt: " << current_item_count << std::endl;
      while (current_offset > 0) {
        //std::cerr << "curr_offset " << current_offset << std::endl;
        int next_ctr = current_offset--;
        current_position++;

         //std::cerr << "curr pos: " << current_position << ". num_clients: " << num_clients << " . client id: " << client_id <<std::endl;

        // If we weren't given a client_id, then we'll generate UserIds
        if (client_id  == -1) {
            found = next_ctr;
            break;
        }
        // Otherwise we have to spin through and find one for our client
        else if (current_position % num_clients == client_id) {
            //fprintf(stderr, "curr pos: %d. num_clients: %d. client_id: %d\n", current_position, num_clients, client_id);
            found = next_ctr;
            break;
        }
      }
      if (found != -1) {
        break;
      }
      current_item_count++;
      current_offset = users_per_item_counts[current_item_count];
      // std::cerr << "new item: " << current_item_count << std::endl;
      // std::cerr << "new offset: " << current_offset << std::endl;
    }

    // std::cerr << "current_item_count: " <<current_item_count << std::endl;
    // std::cerr << "found: " << found << std::endl;
    if (found == -1) {
      assert(client_id != -1); //should never happen for generation
      return std::nullopt;
    }

    return std::make_optional<UserId>(UserId(current_item_count, found));
}

} // namespace auctionmark