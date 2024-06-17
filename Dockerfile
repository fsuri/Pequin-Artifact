# Use an official base image, e.g., a specific version of Ubuntu
FROM ubuntu:23.10

WORKDIR /home/liam/pequin

# Copy the server binary into the container
COPY src/store/benchmark/async/sql/auctionmark/sql-auctionmark-tables-schema.json /home/liam/benchmark_data/
COPY src/store/benchmark/async/sql/auctionmark/sql-auctionmark-data /home/liam/benchmark_data/
COPY src/store/benchmark/async/sql/auctionmark/auctionmark_profile /home/liam/benchmark_data/

# Expose the port the server listens on
EXPOSE 8080

# Command to run the server
CMD ["./server-binary"]

