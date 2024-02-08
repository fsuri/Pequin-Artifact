#ifndef AUCTIONMARK_CATEGORY_H
#define AUCTIONMARK_CATEGORY_H

#include <string>
#include <optional>

namespace auctionmark {

class Category {
private:
    int category_id;
    std::optional<int> parent_category_id;
    int item_count;
    std::string name;
    bool is_leaf;

public:
    inline Category(int category_id, std::string name, std::optional<int> parent_category_id, int item_count, bool is_leaf)
        : category_id(category_id), name(name), parent_category_id(parent_category_id), item_count(item_count), is_leaf(is_leaf) {}

    inline std::string get_name() const {
        return this->name;
    }

    inline int get_category_id() const {
        return this->category_id;
    }

    inline std::optional<int> get_parent_category_id() const {
        return this->parent_category_id;
    }

    inline int get_item_count() const {
        return this->item_count;
    }

    inline bool is_leaf() const {
        return this->is_leaf;
    }

    inline bool operator==(const Category& other) const {
        return category_id == other.category_id
            && item_count == other.item_count
            && is_leaf == other.is_leaf
            && parent_category_id == other.parent_category_id
            && name == other.name;
    }
};

} // namespace auctionmark

#endif // AUIONMARK_CATEGORY_H
