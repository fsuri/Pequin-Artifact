cmake .
cmake --build . 
./main_test "postgresql://root@dhcp-vl2042-3565.redrover.cornell.edu:26257/defaultdb?sslmode=disable"

pkill -f cockroach
# cockroach demo
