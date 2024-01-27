#include <string>
#include <optional>

class Category {
private:
    int categoryID;
    std::optional<int> parentCategoryID;
    int itemCount;
    std::string name;
    bool isLeaf;

public:
    Category(int categoryID, std::string name, std::optional<int> parentCategoryID, int itemCount, bool isLeaf)
        : categoryID(categoryID), name(name), parentCategoryID(parentCategoryID), itemCount(itemCount), isLeaf(isLeaf) {}

    std::string getName() const {
        return this->name;
    }

    int getCategoryID() const {
        return this->categoryID;
    }

    std::optional<int> getParentCategoryID() const {
        return this->parentCategoryID;
    }

    int getItemCount() const {
        return this->itemCount;
    }

    bool isLeaf() const {
        return this->isLeaf;
    }

    bool operator==(const Category& other) const {
        return categoryID == other.categoryID
            && itemCount == other.itemCount
            && isLeaf == other.isLeaf
            && parentCategoryID == other.parentCategoryID
            && name == other.name;
    }
};