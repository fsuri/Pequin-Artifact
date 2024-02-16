#include "item_info.h"

namespace auctionmark {

ItemInfo::ItemInfo(ItemId id, double current_price, uint64_t end_date, uint64_t num_bids) 
    : item_id(item_id), current_price(current_price), end_date(end_date), num_bids(num_bids), status(ItemStatus::OPEN) {
    status = ItemStatus::OPEN;
}

ItemInfo::ItemInfo(std::string id, double current_price, uint64_t end_date, uint64_t num_bids) 
    : item_id(ItemId(id)), current_price(current_price), end_date(end_date), num_bids(num_bids), status(ItemStatus::OPEN) {
}

ItemInfo::ItemInfo() {
    item_id = ItemId();
    status = ItemStatus::OPEN;
    // std::optional<double> current_price = std::nullopt;
    // std::optional<uint64_t> end_date = std::nullopt;
    // long num_bids = 0;
    // std::optional<ItemStatus> status = std::nullopt;
}

ItemId ItemInfo::get_item_id() const {
    return item_id;
}

UserId ItemInfo::get_seller_id() const {
    return item_id.get_seller_id();
}

bool ItemInfo::has_current_price() const {
    return current_price > 0.0;
    //return current_price.has_value();
}

float ItemInfo::get_current_price() const {
    return current_price;
}

bool ItemInfo::has_end_date() const {
    return end_date > 0;
    //return end_date.has_value();
}

uint64_t ItemInfo::get_end_date() const {
    return end_date;
}

void ItemInfo::set_item_id(ItemId item_id) {
    this->item_id = item_id;
}

void ItemInfo::set_current_price(float current_price) {
    this->current_price = current_price;
}

void ItemInfo::set_end_date(uint64_t end_date) {
    this->end_date = end_date;
}

uint64_t ItemInfo::get_num_bids() const {
    return num_bids;
}

void ItemInfo::set_num_bids(uint64_t num_bids) {
    this->num_bids = num_bids;
}

ItemStatus ItemInfo::get_status() const {
    return status;
}

void ItemInfo::set_status(ItemStatus status) {
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