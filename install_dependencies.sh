#!/bin/bash
clear
cd ~
echo "Installing Pequin-Artifact dependencies"
echo "In case of failure, follow manual instruction and consult troubleshooting"
echo ""

#General installation pre-reqs
echo "Installing general pre-reqs"
echo ""
sudo apt-get update
sudo apt-get upgrade

sudo apt install python3-pip
sudo -H pip3 install numpy
sudo apt-get install autoconf automake libtool curl make g++ unzip valgrind cmake gnuplot pkg-config ant 

echo "$(tput setaf 2) COMPLETE: GENERAL PRE-REQ $(tput sgr0)"
read -p "Press enter to continue"
echo ""

#Development library dependencies
echo "Installing Development library dependencies"
echo ""
sudo apt install libsodium-dev libgflags-dev libssl-dev libevent-dev libevent-openssl-2.1-7 libevent-pthreads-2.1-7 libboost-all-dev libuv1-dev libpq-dev postgresql-server-dev-all libfmt-dev libreadline-dev libeigen3-dev
echo "$(tput setaf 2) COMPLETE: GENERAL DEVELOPMENT LIB DEPS $(tput sgr0)"
echo ""
read -p "Press enter to continue"

mkdir dependencies
cd dependencies

#Optional: Hoard
echo "Installing Hoard Allocator"
echo ""
sudo apt-get install clang
git clone https://github.com/emeryberger/Hoard
cd Hoard
cd src
make
sudo cp libhoard.so /usr/local/lib
sudo echo 'export LD_PRELOAD=/usr/local/lib/libhoard.so' >>~/.bashrc
export LD_PRELOAD=/usr/local/lib/libhoard.so
cd ../..

#Installing taopq
echo "Installing TaoPq"
echo ""
git clone https://github.com/taocpp/taopq.git
cd taopq
git checkout 943d827
sudo cmake .
sudo cmake --build . -j $(nproc)
sudo make install
sudo ldconfig
cd ..

#Installing nlohman/json
echo "Installing Nlohman/Json"
echo ""
git clone https://github.com/nlohmann/json.git
cd json
cmake .
sudo make install
sudo ldconfig
cd ..

#googletest
echo "Installing googletest"
echo ""
git clone https://github.com/google/googletest.git
cd googletest
git checkout release-1.10.0

sudo cmake CMakeLists.txt
sudo make -j $(nproc)
sudo make install
sudo ldconfig
cd ..
sudo cp -r googletest /usr/src/gtest-1.10.0
echo "$(tput setaf 2) COMPLETE: Googletest $(tput sgr0)"
echo ""
read -p "Press enter to continue"

#protobuf
echo "Installing protobuf"
echo ""
git clone https://github.com/protocolbuffers/protobuf.git
cd protobuf
git checkout v3.5.1

./autogen.sh
./configure
sudo make -j $(nproc)
sudo make check -j $(nproc)
sudo make install
sudo ldconfig
cd ..
echo "$(tput setaf 2) COMPLETE: Protobuf $(tput sgr0)"
read -p "Press enter to continue"
echo ""
echo "Installing crypto dependencies"
echo ""

#secp256k1
echo "Installing secp256k1"
echo ""
git clone https://github.com/bitcoin-core/secp256k1.git
cd secp256k1
./autogen.sh
./configure
make -j $(nproc)
make check -j $(nproc)
sudo make install
sudo ldconfig
cd ..
echo "$(tput setaf 2) COMPLETE: secp256k1 $(tput sgr0)"
echo ""
read -p "Press enter to continue"

#cryptopp
echo "Installing cryptoPP"
echo ""
git clone https://github.com/weidai11/cryptopp.git
cd cryptopp
make -j $(nproc)
sudo make install
sudo ldconfig
cd ..
echo "$(tput setaf 2) COMPLETE: cryptoPP $(tput sgr0)"
echo ""
read -p "Press enter to continue"

#BLAKE3
echo "Installing BLAKE3"
echo ""
git clone https://github.com/BLAKE3-team/BLAKE3
cd BLAKE3/c
gcc -fPIC -shared -O3 -o libblake3.so blake3.c blake3_dispatch.c blake3_portable.c blake3_sse2_x86-64_unix.S blake3_sse41_x86-64_unix.S blake3_avx2_x86-64_unix.S blake3_avx512_x86-64_unix.S
sudo cp libblake3.so /usr/local/lib/
sudo ldconfig
cd ../../
echo "$(tput setaf 2) COMPLETE: BLAKE3 $(tput sgr0)"
echo ""
read -p "Press enter to continue"

#ed25519-donna
echo "Installing ed25519-donna"
echo ""
git clone https://github.com/floodyberry/ed25519-donna
cd ed25519-donna
gcc -fPIC -shared -O3 -m64 -o libed25519_donna.so ed25519.c -lssl -lcrypto
sudo cp libed25519_donna.so /usr/local/lib
sudo ldconfig
cd ..
echo "$(tput setaf 2) COMPLETE: ed25519-donna $(tput sgr0)"
echo ""
read -p "Press enter to continue"

###################
echo "Installing Peloton dependencies"
echo ""

echo "--libcount"
echo ""
git clone https://github.com/dialtr/libcount
cd libcount
sudo make
sudo make install
cd ..

echo "--libpg_query"
echo ""
git clone https://github.com/cmu-db/peloton.git
cd peloton/third_party/libpgquery
sudo make
cd ..
sudo cp -r libpgquery /usr/local/include
sudo cp libpg_query/libpg_query.a /usr/local/lib

echo "--libcuckoo"
echo ""
sudo cp -r libcuckoo /usr/local/include

echo "--date"
echo ""
sudo cp -r date /usr/local/include

echo "--adaptive_radix_tree"
echo ""
sudo cp -r adaptive_radix_tree /usr/local/include

sudo ldconfig
cd ../..

echo "$(tput setaf 2) COMPLETE: Peloton deps $(tput sgr0)"
echo ""

########################

read -p "Press enter to continue -- Manual interaction required for the next step: See install guide, section IntelTBB"
#IntelTBB
echo "Installing Intel TBB"
echo ""
apt -y install ncurses-term
wget https://registrationcenter-download.intel.com/akdlm/irc_nas/17977/l_BaseKit_p_2021.3.0.3219.sh
sudo bash l_BaseKit_p_2021.3.0.3219.sh
cd ~
source /opt/intel/oneapi/setvars.sh
echo source /opt/intel/oneapi/setvars.sh --force >>~/.bashrc
source ~/.bashrc
echo "$(tput setaf 2) COMPLETE: IntelTBB $(tput sgr0)"
echo ""
read -p "Press enter to continue -- Manual interaction required: See install guide BFTSmart"

#BFTSmart
echo "Installing BFT-SMART req (Java)"
echo ""
sudo apt-get install openjdk-11-jdk
export LD_LIBRARY_PATH=/usr/lib/jvm/java-1.11.0-openjdk-amd64/lib/server:$LD_LIBRARY_PATH
sudo ldconfig
sudo echo 'export LD_LIBRARY_PATH=/usr/lib/jvm/java-1.11.0-openjdk-amd64/lib/server:$LD_LIBRARY_PATH' >>~/.bashrc
echo "$(tput setaf 2) TODO: SEE MANUAL INSTALLATION REQ BFT-SMART $(tput sgr0)"

read -p "Press enter to continue"
#CockroachDB
echo "Installing CockroachDB"
echo ""
sudo mkdir -p /usr/local/lib/cockroach
wget https://binaries.cockroachdb.com/cockroach-v22.2.2.linux-amd64.tgz --no-check-certificate
tar -xf cockroach-v22.2.2.linux-amd64.tgz
sudo cp -i cockroach-v22.2.2.linux-amd64/lib/libgeos.so /usr/local/lib/cockroach/
sudo cp -i cockroach-v22.2.2.linux-amd64/lib/libgeos_c.so /usr/local/lib/cockroach/
sudo cp -i cockroach-v22.2.2.linux-amd64/cockroach /usr/local/bin

export LD_PRELOAD=/usr/local/lib/libhoard.so

echo ""
source ~/.bashrc
echo "$(tput setaf 2) FINISHED INSTALLATION: SUCCESS $(tput sgr0)"
