
#include "store/benchmark/async/sql/seats/seats_util.h"

namespace seats_sql {

void skipCSVHeader(std::ifstream& iostr){
  //Skip header
  std::string columns;
  getline(iostr, columns); 
}

std::vector<std::string> readCSVRow(std::ifstream& iostr) {
  std::vector<std::string> res;

  std::string line; 
  std::getline(iostr, line);
  std::stringstream lineStream(line);
  std::string cell; 

  //std::cerr << std::endl << line << std::endl;
  std::string combination_cell = "";

  //TODO: Do not parse on entries that are of form "dallas, Texas". Those should be treated as one cell.
        //TODO: If split cell, but cell has only open quotes, then it must be part of a second word. Don't push back cell, until we get next cell, and then combine them...
      //Alternatively: Edit CSV to remove all such entries.

  while (std::getline(lineStream, cell, ',')) {
    // if (cell[0] == '"') cell = cell.substr(1);
    // if (cell[cell.size() - 1] == '"') cell = cell.substr(0, cell.size() - 1);
    bool add_to_res = true;
    if(cell.empty()){} //do nothing
    else if(cell[0] == '"' && cell[cell.size() - 1] != '"'){
      add_to_res = false;
    }
    else if(cell[0] != '"' && cell[cell.size() - 1] == '"'){
      cell = combination_cell + ";" + cell;
      combination_cell = "";
      //std::cerr << "combined: " << cell << std::endl;
    }

    //std::cerr << "cell before erase: " << cell << std::endl;
    cell.erase(std::remove(cell.begin(), cell.end(), '\"' ),cell.end()); //remove enclosing quotes
    cell.erase(std::remove(cell.begin(), cell.end(), '\'' ),cell.end()); //remove any internal single quotes (TODO: if one wants them, then they must be doubled)
    //std::cerr << "cell after erase: " << cell << std::endl;

    if(add_to_res) res.push_back(cell);
    else{
      combination_cell += cell;
     // std::cerr << "first part: " << combination_cell << std::endl;
    }
  }

  return res;
}


std::pair<std::string, std::string> convertAPConnToAirports(std::string apconn) {
  std::stringstream ss(apconn);
  std::pair<std::string, std::string> ret; 
  std::string temp; 
  getline(ss, temp, '"');
  getline(ss, ret.first, '-');
  getline(ss, ret.second, '"');

  return ret;
}

std::map<std::string, histogram> createFPAHistograms(std::ifstream& iostr) {

  std::map<std::string, histogram> hists;

  std::string delimiter = "-";

  std::string line; 
  // skip first few lines that aren't relevant data
  for (int i = 0; i < 4; i++) 
    std::getline(iostr, line); 

  std::stringstream lineStream(line);
  std::string cell; 
  
  while (std::getline(lineStream, cell, ':')) {
    //std::cerr << "line: " << line << std::endl;
    std::string key = cell; 

     //split key into two.
     auto [dep_aid, arr_aid] = convertAPConnToAirports(key);
    // std::string dep_aid = key.substr(0, key.find(delimiter)); 
    // std::string arr_aid = key.substr(dep_aid.size() + delimiter.size());

    auto &res = hists[dep_aid];

    std::getline(lineStream, cell, ',');
    std::string val = cell; 
    int v = stoi(val);
    if (!res.empty())
      v = stoi(val) + res.back().second;
    
   
    res.push_back(std::make_pair(arr_aid, v));

    // grab next line
    std::getline(iostr, line); 
    lineStream.str(line);
  }
  return hists; 
}

histogram createFPAHistogram(std::ifstream& iostr) {
  std::vector<std::pair<std::string, int>> res; 
  std::string line; 
  // skip first few lines that aren't relevant data
  for (int i = 0; i < 4; i++) 
    std::getline(iostr, line); 

  std::stringstream lineStream(line);
  std::string cell; 
  
  while (std::getline(lineStream, cell, ':')) {
    //std::cerr << "line: " << line << std::endl;
    std::string key = cell; 
    std::getline(lineStream, cell, ',');
    std::string val = cell; 
    int v = stoi(val);
    if (!res.empty())
      v = stoi(val) + res.back().second;
    
    res.push_back(std::make_pair(key, v));

    // grab next line
    std::getline(iostr, line); 
    lineStream.str(line);
  }
  return res; 
}

histogram createFPTHistogram(std::ifstream& iostr) {
    std::vector<std::pair<std::string, int>> res; 
  std::string line; 
  // skip first few lines that aren't relevant data
  for (int i = 0; i < 4; i++) 
    std::getline(iostr, line); 

  std::stringstream lineStream(line);
  std::string cell; 
  
  while (std::getline(lineStream, cell, ':')) {
    std::string key = cell; 
    std::getline(lineStream, cell, ':');
    key += ":" + cell;
    std::getline(lineStream, cell, ',');
    std::string val = cell; 
    int v = stoi(val);
    if (!res.empty())
      v = stoi(val) + res.back().second;
    
    res.push_back(std::make_pair(key, v));

    // grab next line
    std::getline(iostr, line); 
    lineStream.str(line);
  }
  return res; 
}


std::string getRandValFromHistogram(const histogram &hist, std::mt19937 gen) {
  int rand = std::uniform_int_distribution<int>(1, hist.back().second)(gen) - 1;
  std::string ret = std::upper_bound(hist.begin(), hist.end(), std::make_pair("", rand), compHist)->first; 
  return ret;
}

void FillColumnNamesWithGenericAttr( std::vector<std::pair<std::string, std::string>> &column_names_and_types,
    std::string generic_col_name, std::string generic_col_type, int num_cols) {
    std::ostringstream ss;
    for (int i = 0; i < num_cols; i++) {
        ss << std::setw(2) << std::setfill('0') << i;
        column_names_and_types.push_back(std::make_pair(generic_col_name + ss.str(), generic_col_type));
        ss.str(std::string()); 
        ss.clear();
    }
}


/** Generate random alpha-numeric string of length [min_len, max_len] */
std::string RandomANString(size_t min_len, size_t max_len, std::mt19937 &gen) {
  std::string s;
  size_t length = std::uniform_int_distribution<size_t>(min_len,  max_len)(gen);
  for (size_t i = 0; i < length; ++i) {
    int j = std::uniform_int_distribution<size_t>(0, sizeof(ALPHA_NUMERIC) - 2)(gen);
    s.push_back(ALPHA_NUMERIC[j]);
  }
  return s;
}

/** Generate random alphabetical string of length [min_len, max_len]*/
std::string RandomAString(size_t min_len, size_t max_len, std::mt19937 &gen) {
  std::string s;
  size_t length = std::uniform_int_distribution<size_t>(min_len,  max_len)(gen);
  for (size_t i = 0; i < length; ++i) {
    int j = std::uniform_int_distribution<size_t>(10, sizeof(ALPHA_NUMERIC) - 2)(gen);
    s += ALPHA_NUMERIC[j];
  }
  return s;
}


/** Generate random numerical string of length [min_len, max_len] */
std::string RandomNString(size_t min_len, size_t max_len, std::mt19937 &gen) {
  std::string s;
  size_t length = std::uniform_int_distribution<size_t>(min_len,  max_len)(gen);
  for (size_t i = 0; i < length; ++i) {
    int j = std::uniform_int_distribution<size_t>(0, 9)(gen);
    s += ALPHA_NUMERIC[j];
  }
  return s;
}

}