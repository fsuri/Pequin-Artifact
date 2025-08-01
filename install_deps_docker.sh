#!/bin/bash
clear
cd ~
echo "Installing Pequin-Artifact dependencies"
echo "In case of failure, follow manual instruction and consult troubleshooting"
echo ""

#General installation pre-reqs
echo "Installing general pre-reqs"
echo ""
apt-get update
apt-get upgrade

apt install python3-pip
pip3 install numpy
apt-get install autoconf automake libtool curl make g++ unzip valgrind cmake gnuplot pkg-config ant

echo "$(tput setaf 2) COMPLETE: GENERAL PRE-REQ $(tput sgr0)"
read -p "Press enter to continue"
echo ""

#Development library dependencies
echo "Installing Development library dependencies"
echo ""
apt install libsodium-dev libgflags-dev libssl-dev libevent-dev libevent-openssl-2.1-7 libevent-pthreads-2.1-7 libboost-all-dev libuv1-dev
echo "$(tput setaf 2) COMPLETE: GENERAL DEVELOPMENT LIB DEPS $(tput sgr0)"
echo ""
read -p "Press enter to continue"

mkdir dependencies
cd dependencies

#googletest
echo "Installing googletest"
echo ""
git clone https://github.com/google/googletest.git
cd googletest
git checkout release-1.10.0

cmake CMakeLists.txt
make -j $(nproc)
make install
ldconfig
cd ..
cp -r googletest /usr/src/gtest-1.10.0
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
make -j $(nproc)
make check -j $(nproc)
make install
ldconfig
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
make install
ldconfig
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
make install
ldconfig
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
cp libblake3.so /usr/local/lib/
ldconfig
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
cp libed25519_donna.so /usr/local/lib
ldconfig
cd ..
echo "$(tput setaf 2) COMPLETE: ed25519-donna $(tput sgr0)"
echo ""
read -p "Press enter to continue -- Manual interaction required: See install guide IntelTBB"

#IntelTBB
echo "Installing Intel TBB"
echo ""
apt -y install ncurses-term
wget https://registrationcenter-download.intel.com/akdlm/irc_nas/17977/l_BaseKit_p_2021.3.0.3219.sh
bash l_BaseKit_p_2021.3.0.3219.sh
cd ~
source /opt/intel/oneapi/setvars.sh
echo source /opt/intel/oneapi/setvars.sh --force >> ~/.bashrc
source ~/.bashrc
echo "$(tput setaf 2) COMPLETE: IntelTBB $(tput sgr0)"
echo ""
read -p "Press enter to continue -- Manual interaction required: See install guide BFTSmart"

#BFTSmart
echo "Installing BFT-SMART req (Java)"
echo ""
apt-get install openjdk-11-jdk
export LD_LIBRARY_PATH=/usr/lib/jvm/java-1.11.0-openjdk-amd64/lib/server:$LD_LIBRARY_PATH
ldconfig
echo "$(tput setaf 2) TODO: SEE MANUAL INSTALLATION REQ BFT-SMART $(tput sgr0)"


echo ""
echo "$(tput setaf 2) FINISHED INSTALLATION: SUCCESS $(tput sgr0)"



