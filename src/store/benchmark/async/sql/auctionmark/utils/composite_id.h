#include <string>
#include <vector>
#include <sstream>
#include <iomanip>

class CompositeId {
protected:
    static const std::string PAD_STRING;
    static const int INT_MAX_DIGITS = 10;
    static const int LONG_MAX_DIGITS = 19;

    std::string encode(const std::vector<int>& offset_bits) const {
        std::stringstream compositeBuilder;

        std::vector<std::string> decodedValues = this->to_array();
        for (int i = 0; i < decodedValues.size(); i++) {
            std::string value = decodedValues[i];
            int valueLength = offset_bits[i];
            compositeBuilder << std::setw(valueLength) << std::setfill(PAD_STRING[0]) << value;
        }

        return compositeBuilder.str();
    }

    std::vector<std::string> decode(const std::string& composite_id, const std::vector<int>& offset_bits) {
        std::vector<std::string> decodedValues(offset_bits.size());

        int start = 0;
        for (int i = 0; i < decodedValues.size(); i++) {
            int valueLength = offset_bits[i];
            int end = start + valueLength;
            decodedValues[i] = composite_id.substr(start, valueLength);
            start = end;
        }
        return decodedValues;
    }

public:
    virtual std::string encode() const = 0;
    virtual void decode(const std::string& composite_id) = 0;
    virtual std::vector<std::string> to_array() const = 0;
};

const std::string CompositeId::PAD_STRING = "0";