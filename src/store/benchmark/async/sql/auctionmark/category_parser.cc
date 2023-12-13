/***********************************************************************
 *
 * Copyright 2023 Florian Suri-Payer <fsp@cs.cornell.edu>
 *                Liam Arzola <lma77@cornell.edu>
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 **********************************************************************/

#include "store/benchmark/async/sql/auctionmark/category_parser.h"
#include <vector>
#include <fstream>
#include <boost/algorithm/string.hpp>

namespace auctionmark {

Category::Category(uint32_t c_id, std::optional<uint32_t> c_parent_id, uint32_t item_cnt,
  const std::string &name, bool leaf) :
  c_id(c_id), c_parent_id(c_parent_id), item_cnt(item_cnt), name(name), leaf(leaf) {}

Category::~Category() {}

uint32_t Category::get_category_id() const {
  return c_id;
}

std::optional<uint32_t> Category::get_parent_category_id() const {
  return c_parent_id;
}

uint32_t Category::get_item_cnt() const {
  return item_cnt;
}

std::string Category::get_name() const {
  return name;
}

bool Category::is_leaf() const {
  return leaf;
}

bool Category::operator ==(const Category &other) const {
  return c_id == other.c_id && c_parent_id == other.c_parent_id && item_cnt == other.item_cnt && name == other.name && leaf == other.leaf;
}

size_t CategoryHash::operator()(const Category& c) const {
  return std::hash<size_t>()(std::hash<uint32_t>()(c.get_category_id()) + 
    std::hash<uint32_t>()(c.get_parent_category_id().value_or(0)) +
    std::hash<uint32_t>()(c.get_item_cnt()) +
    std::hash<std::string>()(c.get_name()) +
    std::hash<bool>()(c.is_leaf()));
}

CategoryParser::CategoryParser() {
  next_category_id = 0;
  categories = std::unordered_map<std::string, Category>();
  const std::string path = "store/benchmark/async/sql/auctionmark/categories.txt";
  std::ifstream file(path);
  std::string line;
  if(file.is_open()) {
    while(getline(file, line)) {
      extract_category(line);
    }
    file.close();
  } else {
    throw std::runtime_error("Could not open category file");
  }
}

CategoryParser::~CategoryParser() {}

void CategoryParser::extract_category(std::string s) {
  std::string delimiter = "\t";
  std::string category = "";
  std::vector<std::string> tokens;
  boost::split(tokens, s, boost::is_any_of(delimiter));
  for(int i = 0; i <= 4; i++) {
    boost::trim(tokens[i]);
    if(!tokens[i].empty()) {
      category.append(tokens[i]);
      category.append("/");
    } else {
      break;
    }
  }

  boost::trim(tokens[5]);
  int32_t item_cnt = std::stoi(tokens[5]);

  if(category.length() > 0) {
    category.pop_back();
  }

  add_new_category(category, item_cnt, true);
}

Category CategoryParser::add_new_category(std::string full_category_name, int item_cnt, bool is_leaf) {
  std::optional<Category> parent_category;

  std::string category_name = full_category_name;
  std::string parent_category_name = "";
  std::optional<uint32_t> parent_category_id = 0;

  if(full_category_name.find("/") != full_category_name.npos) {
    int serparator_index = full_category_name.find_last_of("/");
    parent_category_name = full_category_name.substr(0, serparator_index);
    category_name = full_category_name.substr(serparator_index + 1);
  }

  if(categories.count(parent_category_name)) {
    parent_category = categories[parent_category_name];
  } else if(!parent_category_name.empty()) {
    add_new_category(parent_category_name, 0, false);
  }

  if(parent_category.has_value()) {
    parent_category_id = parent_category->get_category_id();
  }

  Category category = Category(next_category_id++, parent_category_id, item_cnt, category_name, is_leaf);
  categories[full_category_name] = category;

  return category;
}

std::unordered_map<std::string, Category> CategoryParser::get_categories() const {
  return categories;
}

} // namespace auctionmark

