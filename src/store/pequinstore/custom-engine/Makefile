CC = g++
CFLAGS = -Wall -Wextra -std=c++17
LDFLAGS = -L libpg_query -l pg_query -L/opt/homebrew/lib
CPPFLAGS = -I/opt/homebrew/include -I libpg_query 

TARGET = parser

all: $(TARGET)

$(TARGET): parser.cc
	$(CC) $(CFLAGS) $(CPPFLAGS) $< -o $@ $(LDFLAGS)

clean:
	rm -f $(TARGET)
