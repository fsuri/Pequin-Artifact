CC = g++
CFLAGS = -Wall -Wextra -std=c++17
LDFLAGS = -Ilibpg_query -Llibpg_query 
CPPFLAGS = -lpg_query

TARGET = parser

all: $(TARGET)

$(TARGET): parser.cc
	$(CC) $(CFLAGS) $(CPPFLAGS) $< -o $@ $(LDFLAGS)

clean:
	rm -f $(TARGET)
