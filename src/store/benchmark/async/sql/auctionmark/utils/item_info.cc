#include "item_info.h"

namespace auctionmark {

ItemInfo::ItemInfo(ItemId id, std::optional<double> current_price, std::optional<std::chrono::system_clock::time_point> end_date, int num_bids)
    : item_id(id), num_bids(num_bids) {
    if (current_price.has_value()) {
        this->current_price = static_cast<float>(current_price.value());
    }
    this->end_date = end_date;
}

ItemId ItemInfo::get_item_id() const {
    return item_id;
}

UserId ItemInfo::get_seller_id() const {
    return item_id.get_seller_id();
}

bool ItemInfo::has_current_price() const {
    return current_price.has_value();
}

std::optional<float> ItemInfo::get_current_price() const {
    return current_price;
}

bool ItemInfo::has_end_date() const {
    return end_date.has_value();
}

std::optional<std::chrono::system_clock::time_point> ItemInfo::get_end_date() const {
    return end_date;
}

void ItemInfo::set_item_id(ItemId item_id) {
    this->item_id = item_id;
}

void ItemInfo::set_current_price(std::optional<float> current_price) {
    this->current_price = current_price;
}

void ItemInfo::set_end_date(std::optional<std::chrono::system_clock::time_point> end_date) {
    this->end_date = end_date;
}

long ItemInfo::get_num_bids() const {
    return num_bids;
}

void ItemInfo::set_num_bids(long num_bids) {
    this->num_bids = num_bids;
}

std::optional<ItemStatus> ItemInfo::get_status() const {
    return status;
}

void ItemInfo::set_status(std::optional<ItemStatus> status) {
    this->status = status;
}

bool ItemInfo::operator==(const ItemInfo& other) const {
    return item_id == other.item_id
        && current_price == other.current_price
        && end_date == other.end_date
        && num_bids == other.num_bids
        && status == other.status;
}

bool ItemInfo::operator<(const ItemInfo& other) const {
    return item_id < other.item_id;
}

} // namespace auctionmark