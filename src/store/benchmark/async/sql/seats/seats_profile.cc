#include <fstream>
#include <unordered_map> 
const char* PROFILE_LOC = "./profiles/profile.txt";

void writeProfile(std::unordered_map<int, std::pair<int,int>>& flight_info) {
  std::ofstream profile (PROFILE_LOC);

  for (auto it = flight_info.begin(); it != flight_info.end(); it++) {
    int f_id = it->first;
    int ap_o_id = it->second.first;
    int ap_i_id = it->second.second; 

    auto f_ap_conn_entry = std::to_string(f_id) + ':' + std::to_string(ap_o_id) + '-' + std::to_string(ap_i_id) + '\n';
    profile.write(f_ap_conn_entry.c_str(), f_ap_conn_entry.size());
  }
}

void readProfile(std::unordered_map<int, std::pair<int,int>>& flight_info) {
    std::ifstream profile (PROFILE_LOC);

}