cockroach start-single-node --insecure --background
echo "postgresql://root@`hostname`:26257/defaultdb?sslmode=disable" > test.config