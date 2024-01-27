#include <iostream>
#include <vector>
#include <map>
#include <algorithm>
#include <iterator>
#include "user_id.h"
#include <boost/histogram.hpp>
#include <optional>

class UserIdGenerator {
public:
  UserIdGenerator(const boost::histogram::histogram<long>& users_per_item_count, int numClients, int clientId = -1)
    : numClients(numClients), clientId(clientId), usersPerItemCounts(), minItemCount(0), maxItemCount(0), totalUsers(0), _next(nullptr), currentItemCount(-1), currentOffset(0), currentPosition(0) {

    if (numClients <= 0) {
      throw std::invalid_argument("numClients must be more than 0: " + std::to_string(numClients));
    }
    if (clientId != -1 && clientId < 0) {
      throw std::invalid_argument("clientId must be more than or equal to 0: " + std::to_string(clientId));
    }

    maxItemCount = static_cast<int>(users_per_item_count.axis().upper(0));
    usersPerItemCounts.resize(maxItemCount + 2);
    for (int i = 0; i < usersPerItemCounts.size(); i++) {
      usersPerItemCounts[i] = users_per_item_count.at(i);
    }

    minItemCount = static_cast<int>(users_per_item_count.axis().lower(0));

    totalUsers = 0;
    for (const auto& count : usersPerItemCounts) {
      totalUsers += count;
    }

    setCurrentItemCount(minItemCount);
  }

  long getTotalUsers() const {
    return totalUsers;
  }

  void setCurrentItemCount(int size) {
    currentPosition = 0;
    for (int i = 0; i < size; i++) {
      currentPosition += usersPerItemCounts[i];
    }
    currentItemCount = size;
    currentOffset = usersPerItemCounts[currentItemCount];
  }

  int getCurrentPosition() const {
    return currentPosition;
  }

  std::optional<UserId> seekToPosition(int position) {
    std::optional<UserId> user_id;

    currentPosition = 0;
    currentItemCount = 0;
    while (true) {
      int num_users = usersPerItemCounts[currentItemCount];

      if (currentPosition + num_users > position) {
        _next = nullptr;
        currentOffset = num_users - (position - currentPosition);
        currentPosition = position;
        user_id = next();
        break;
      } else {
        currentPosition += num_users;
      }
      currentItemCount++;
    }
    return user_id;
  }

  bool checkClient(const UserId& user_id) const {
    if (clientId == -1) {
      return true;
    }

    int tmp_count = 0;
    int tmp_position = 0;
    while (tmp_count <= maxItemCount) {
      int num_users = usersPerItemCounts[tmp_count];
      if (tmp_count == user_id.getItemCount()) {
        tmp_position += (num_users - user_id.getOffset()) + 1;
        break;
      }
      tmp_position += num_users;
      tmp_count++;
    }
    return tmp_position % numClients == clientId;
  }

  bool hasNext() {
    if (_next == nullptr) {
      _next = findNextUserId();
    }
    return _next != nullptr;
  }

  UserId next() {
    if (_next == nullptr) {
      _next = findNextUserId();
    }
    UserId ret = *_next;
    _next = nullptr;
    return ret;
  }

  void remove() {
    throw std::logic_error("Cannot call remove!!");
  }

  std::string toString() const {
    std::map<std::string, std::string> m;
    m["numClients"] = std::to_string(numClients);
    m["clientId"] = std::to_string(clientId);
    m["minItemCount"] = std::to_string(minItemCount);
    m["maxItemCount"] = std::to_string(maxItemCount);
    m["totalUsers"] = std::to_string(totalUsers);
    m["currentItemCount"] = std::to_string(currentItemCount);
    m["currentOffset"] = std::to_string(currentOffset);
    m["currentPosition"] = std::to_string(currentPosition);
    m["_next"] = _next != nullptr ? "not null" : "null";
    std::string usersPerItemCountStr = "[Length:" + std::to_string(usersPerItemCounts.size()) + "] => [";
    for (const auto& count : usersPerItemCounts) {
      usersPerItemCountStr += std::to_string(count) + ", ";
    }
    usersPerItemCountStr += "]";
    m["users_per_item_count"] = usersPerItemCountStr;
    std::string result;
    for (const auto& entry : m) {
      result += entry.first + ": " + entry.second + "\n";
    }
    return result;
  }

private:
  std::vector<int> usersPerItemCounts;
  int numClients;
  int clientId;
  int minItemCount;
  int maxItemCount;
  long totalUsers;
  std::vector<int>::iterator _next;
  int currentItemCount;
  int currentOffset;
  int currentPosition;

  std::vector<int>::iterator findNextUserId() {
    auto it = usersPerItemCounts.begin();
    std::advance(it, currentItemCount);
    while (it != usersPerItemCounts.end()) {
      while (currentOffset > 0) {
        int nextCtr = currentOffset--;
        currentPosition++;

        if (clientId == -1) {
          return it;
        } else if (currentPosition % numClients == clientId) {
          return it;
        }
      }
      it++;
      currentItemCount++;
      currentOffset = *it;
    }
    return usersPerItemCounts.end();
  }
};
