# Installing Software Dependencies <a name="installing"></a>

The high-level requirements for building the codebase are: 
- Operating System: Ubuntu 20.04 LTS, Focal 
   - We recommend running on Ubuntu 20.04 LTS, Focal, as a) the binaries of our experimental evaluation were built and run on this operating system, and b) our supplied images use Ubuntu 20.04 LTS.    - If you cannot do this locally, consider using a CloudLab controller machine - see section "Setting up CloudLab".
   - Prior builds used Ubuntu 18.04 (Bionic) and should have remained backwards compatbile.
   <!-- You may try to use Ubuntu 20.04.2 LTS instead of 18.04 LTS. However, we do not guarantee a fully documented install process, nor precise repicability of our results. Note, that using Ubuntu 20.04.2 LTS locally (or as control machine) to generate and upload binaries may *not* be compatible with running Cloudlab machines using our cloud lab images (as they use 18.04 LTS(. In order to use Ubuntu 20.04.2 LTS you may have to manually create new disk images for CloudLab instead of using our supplied images for 18.04 LTS to guarantee library compatibility. -->
   <!-- You may try to run on Mac, which has worked for us in the past, but is not documented in the following ReadMe and may not easily be trouble-shooted by us. -->
  
- Requires python3 (install included below)
- Requires C++ 17 
- Requires Java Version >= 1.8 (for BFTSmart). We suggest you run the Open JDK java 11 version (install included below) as our Makefile is currently hard-coded for it. (install included below)

### AUTOMATIC INSTALLATION

Simply run `./install_dependencies.sh`. If the script is not set to executable by default, use `chmod +x install_dependencies.sh` first. 
Each installation step will print `COMPLETE` upon completion, and require manual input to proceed -- please verify that the installation step proceeded without errors. In case of errors, please consult the [manual installation](#MANUAL-INSTALLATION) and [troubleshooting](#Troubleshooting) below.
If successful, skip ahead to [Building binaries](#Building-binaries). 

> :warning: NOTE: The script requires explicit manual interaction when installing IntelTBB and BFT-SMaRt requisites. Please consult the manual installation below.

### MANUAL INSTALLATION

> :warning: For manual dependency installation follow the instructions below.

### General installation pre-reqs

Before beginning the install process, update your distribution:
1. `sudo apt-get update`
2. `sudo apt-get upgrade`

Then, install the following tools:

3. `sudo apt install python3-pip`
4. `sudo -H pip3 install numpy`
5. `sudo apt-get install autoconf automake libtool curl make g++ unzip valgrind cmake gnuplot pkg-config ant parallel`


### Development library dependencies

The prototype implementations depend the following development libraries:
- libevent-openssl
- libevent-pthreads
- libevent-dev
- libssl-dev
- libgflags-dev
- libsodium-dev
- libbost-all-dev
- libuv1-dev
- libpq-dev 
- postgresql-server-dev-all
- libfmt-dev

You may install them directly using:
- `sudo apt install libsodium-dev libgflags-dev libssl-dev libevent-dev libevent-openssl-2.1-7 libevent-pthreads-2.1-7 libboost-all-dev libuv1-dev libpq-dev postgresql-server-dev-all libfmt-dev libreadline-dev libeigen3-dev` 

- If using Ubuntu 18.04, use `sudo apt install libevent-openssl-2.1-6 libevent-pthreads-2.1-6` instead for openssl and pthreads.

- To build on Ubuntu 22.04 or later (not carefully vetted):

   Add these to the end of your `/etc/apt/sources.list`:
   ```
   deb     http://archive.ubuntu.com/ubuntu/ focal main
   deb-src http://archive.ubuntu.com/ubuntu/ focal main
   ```
   Run:
   ```
   sudo apt update
   sudo apt install cpp=4:9.3.0-1ubuntu2
   sudo apt install gcc=4:9.3.0-1ubuntu2
   sudo apt install g++=4:9.3.0-1ubuntu2
   ```
   Download `libfmt-dev` version 4:9.3.0-1ubuntu2 at:
   http://mirrors.kernel.org/ubuntu/pool/main/g/gcc-defaults/g++_9.3.0-1ubuntu2_amd64.deb
   
   Run from folder where downloaded:
   ```
   sudo apt install libfmt-dev_6.1.2+ds-2_amd64.deb
   ```

In addition, you will need to install the following libraries from source (detailed instructions below):
<!---- [Hoard Allocator](https://github.com/emeryberger/Hoard) -->
- [jemalloc](https://github.com/jemalloc/jemalloc)
- [taopq](https://github.com/taocpp/taopq)
- [nlohman/json](https://github.com/nlohmann/json)
- [googletest-1.10](https://github.com/google/googletest/releases/tag/release-1.10.0)
- [protobuf-3.5.1](https://github.com/protocolbuffers/protobuf/releases/tag/v3.5.1)
- [cryptopp-8.2](https://github.com/weidai11/cryptopp/releases/tag/CRYPTOPP_8_2_0) <!-- (htps://cryptopp.com/cryptopp820.zip)-->
- [bitcoin-core/secp256k1](https://github.com/bitcoin-core/secp256k1/)
- [BLAKE3](https://github.com/BLAKE3-team/BLAKE3)
- [ed25519-donna](https://github.com/floodyberry/ed25519-donna)
- [Intel TBB](https://software.intel.com/content/www/us/en/develop/tools/oneapi/base-toolkit/get-the-toolkit.html). 
   - You will additionally need to [configure your CPU](https://software.intel.com/content/www/us/en/develop/documentation/get-started-with-intel-oneapi-base-linux/top/before-you-begin.html) before being able to compile the prototypes.
- [PelotonDB](https://github.com/cmu-db/peloton)
- [CockroachDB](https://www.cockroachlabs.com/docs/stable/install-cockroachdb-linux.html)


Detailed install instructions:

We recommend organizing all installs in a dedicated folder:

1. `mkdir dependencies`
2. `cd dependencies`

<!-- #### Installing fmt
1. `git clone git@github.com:fmtlib/fmt.git`
2. `cd fmt`
3. `cmake .`
4. `sudo make install`
5. `sudo ldconfig`
6. `cd ..` -->

<!--- #### Installing Hoard Allocator
1. `sudo apt-get install clang`
2. `git clone https://github.com/emeryberger/Hoard`
3. `cd src`
4. `make`
5. `sudo cp libhoard.so /usr/local/lib`
6. `sudo echo 'export LD_PRELOAD=/usr/local/lib/libhoard.so' >> ~/.bashrc; source ~/.bashrc;` (once) or `export LD_PRELOAD=/usr/local/lib/libhoard.so` (everytime)
7. `cd ..`
-->
#### Installing jemalloc
```
git clone https://github.com/jemalloc/jemalloc
cd jemalloc
./autogen.h
make
sudo make install
sudo echo 'export LD_PRELOAD=/usr/local/lib/libjemalloc.so' >> ~/.bashrc; source ~/.bashrc;`
cd ..`
```
1. `git clone https://github.com/jemalloc/jemalloc`
2. `cd jemalloc`
3. `./autogen.h`
4. `make`
5. `sudo make install`
6. `sudo echo 'export LD_PRELOAD=/usr/local/lib/libjemalloc.so' >> ~/.bashrc; source ~/.bashrc;` (once) or `export LD_PRELOAD=/usr/local/lib/libjemalloc.so` (everytime)
7. `cd ..`

#### Installing taopq 

Download the library:

1. `git clone https://github.com/taocpp/taopq.git`
2. `cd taopq`
3. `git checkout 943d827`

Alternatively, you may download and unzip from source: 

1. `wget https://github.com/taocpp/taopq/archive/943d827.zip`
2. `unzip 943d827.zip`  

Next, build taopq:

4. `sudo cmake .`
5. `sudo cmake --build . -j`
6. `sudo make install`
7. `sudo ldconfig`
8. `cd ..`

#### Installing nlohman/json 

Download the library:

1. `git clone https://github.com/nlohmann/json.git`
2. `cd json`

Next, build nlohman/json

4. `cmake .`
6. `sudo make install`
7. `sudo ldconfig`
8. `cd ..`


#### Installing google test

Download the library:

1. `git clone https://github.com/google/googletest.git`
2. `cd googletest`
3. `git checkout release-1.10.0`

Alternatively, you may download and unzip from source: 

1. `wget https://github.com/google/googletest/archive/release-1.10.0.zip`
2. `unzip release-1.10.0.zip`  

Next, build googletest:

4. `sudo cmake CMakeLists.txt`
5. `sudo make -j $(nproc)`
6. `sudo make install`
7. `sudo ldconfig`
8. `cd ..`
9. `sudo cp -r googletest /usr/src/gtest-1.10.0` (Move whole folder)



#### Installing protobuf

Download the library:

1. `git clone https://github.com/protocolbuffers/protobuf.git`
2. `cd protobuf`
3. `git checkout v3.5.1`

Alternatively, you may download and unzip from source: 

1.`wget https://github.com/protocolbuffers/protobuf/releases/download/v3.5.1/protobuf-all-3.5.1.zip`
2.`unzip protobuf-all-3.5.1.zip`

Next, build protobuf:

4. `./autogen.sh`
5. `./configure`
6. `sudo make -j $(nproc)`
7. `sudo make check -j $(nproc)`
8. `sudo make install`
9. `sudo ldconfig`
10. `cd ..`


#### Installing secp256k1

Download and build the library:

1. `git clone https://github.com/bitcoin-core/secp256k1.git`
2. `cd secp256k1`
3. `git checkout ac83be33` (our cloudlab images have version 0.0.0)
4. `./autogen.sh`
5. `./configure`
6. `make -j $(nproc)`
7. `make check -j $(nproc)`
8. `sudo make install`
9. `sudo ldconfig`
10. `cd ..`


#### Installing cryptopp

Download and build the library:

1. `git clone https://github.com/weidai11/cryptopp.git`
2. `cd cryptopp`
3. `make -j $(nproc)`
4. `sudo make install`
5. `sudo ldconfig`
6. `cd ..`

#### Installing BLAKE3

Download the library:

1. `git clone https://github.com/BLAKE3-team/BLAKE3`
2. `cd BLAKE3/c`

Create a shared libary:

3. `gcc -fPIC -shared -O3 -o libblake3.so blake3.c blake3_dispatch.c blake3_portable.c blake3_sse2_x86-64_unix.S blake3_sse41_x86-64_unix.S blake3_avx2_x86-64_unix.S blake3_avx512_x86-64_unix.S`

Move the shared libary:

4. `sudo cp libblake3.so /usr/local/lib/`
5. `sudo ldconfig`
6. `cd ../../`

#### Installing ed25519-donna

Download the library:

1. `git clone https://github.com/floodyberry/ed25519-donna`
2. `cd ed25519-donna`

Create a shared library:

3. `gcc -fPIC -shared -O3 -m64 -o libed25519_donna.so ed25519.c -lssl -lcrypto`

Move the shared libary:

4. `sudo cp libed25519_donna.so /usr/local/lib`
5. `sudo ldconfig`
6. `cd ..`



#### Installing Peloton dependencies
```
//install libcount
git clone https://github.com/dialtr/libcount
cd libcount
sudo make
sudo make install
cd ..

//install peloton third party dependencies
git clone https://github.com/cmu-db/peloton.git
cd peloton/third_party/libpg_query
sudo make
cd ..
sudo cp -r libpg_query /usr/local/include
sudo cp libpg_query/libpg_query.a /usr/local/lib

sudo cp -r libcuckoo /usr/local/include

sudo cp -r date /usr/local/include

sudo cp -r adaptive_radix_tree /usr/local/include

sudo ldconfig
cd ../..
```



#### Installing Intel TBB
> :warning: If you run into issues with the installation you may refer to https://www.intel.com/content/www/us/en/docs/oneapi/installation-guide-linux/2023-0/overview.html for detailed install resources.

First, download the installation script:

1. `wget https://registrationcenter-download.intel.com/akdlm/IRC_NAS/e6ff8e9c-ee28-47fb-abd7-5c524c983e1c/l_BaseKit_p_2024.2.1.100.sh --no-check-certificate`
> :warning: Our tested install used the 2021 basekit (`wget https://registrationcenter-download.intel.com/akdlm/irc_nas/17977/l_BaseKit_p_2021.3.0.3219.sh`). However, the 2021 basekit download is now deprecated. The 2024 installer includes the 2021 package.

 Alternatively, you may download the latest Intel BaseKit version from https://www.intel.com/content/www/us/en/developer/tools/oneapi/base-toolkit-download.html?operatingsystem=linux&distributions=online (Note that you need to ensure the version is compatible with our code). 
 
 Next, execute the installation script
2. `sudo bash l_BaseKit_p_2024.2.1.100.sh`
(To run the installation script you may have to manually install `apt -y install ncurses-term` if you do not have it already).

Follow the installation instructions: 
- It will either open a GUI installation interface if availalbe, or otherwise show the same within the shell (e.g. on a control machine)
- Select custom installation 
- You need only "Intel oneAPI Threading Building Blocks". You may uncheck every other install -- In the shell use the space bar to uncheck all items marked with an X 
- Skip Eclipse IDE configuration
- You do not need to consent to data collection

Next, set up the intel TBB environment variables (Refer to https://software.intel.com/content/www/us/en/develop/documentation/get-started-with-intel-oneapi-base-linux/top/before-you-begin.html if necessary):

If you installed Intel TBB with root access, it should be installed under `/opt/intel/oneapi`. Run the following to initialize environment variables:

3. `source /opt/intel/oneapi/setvars.sh`

Note, that this must be done everytime you open a new terminal. You may add it to your .bashrc to automate it:

4. `echo source /opt/intel/oneapi/setvars.sh --force >> ~/.bashrc`
5. `source ~/.bashrc`

(When building on a Cloudlab controller instead of locally, the setvars.sh must be sourced manually everytime since bashrc will not be persisted across images. All other experiment machines will be source via the experiment scripts, so no further action is necessary there.)


This completes all required dependencies for Basil, Tapir and TxHotstuff. To successfully build the binary (and run TxBFTSmart) the following additional steps are necessary:

#### Additional prereq for BFTSmart 

First, install Java open jdk 1.11.0 in /usr/lib/jvm and export your LD_LIBRARY_Path:

1. `sudo apt-get install openjdk-11-jdk` Confirm that `java-11-openjdk-amd64` it is installed in /usr/lib/jvm  
2. `sudo echo 'export LD_LIBRARY_PATH=/usr/lib/jvm/java-1.11.0-openjdk-amd64/lib/server:$LD_LIBRARY_PATH' >> ~/.bashrc; source ~/.bashrc` (once) or `export LD_LIBRARY_PATH=/usr/lib/jvm/java-1.11.0-openjdk-amd64/lib/server:$LD_LIBRARY_PATH` (everytime)
3. `sudo ldconfig`

If it is not installed in `/usr/lib/jvm` then source the `LD_LIBRARY_PATH` according to your install location and adjust the following lines in the Makefile with your path:

- `# Java and JNI`
- `JAVA_HOME := /usr/lib/jvm/java-11-openjdk-amd64`  (adjust this)
- `CFLAGS += -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux`
- `LDFLAGS += -L/usr/lib/jvm/java-1.11.0-openjdk-amd64/lib/server -ljvm`  (adjust this)

Afterwards, navigate to `/usr/lib/jvm/java-11-openjdk-amd64/conf/security/java.security`and comment out (or remove) the following line: `jdk.tls.disabledAlgorithms=SSLv3, TLSv1, RC4, DES, MD5withRSA, DH keySize < 1024 EC keySize < 224, 3DES_EDE_CBC, anon, NULL`

#### Additional prereq for CockroachDB 

First, download and extract cockroach.
- `wget https://binaries.cockroachdb.com/cockroach-v22.2.2.linux-amd64.tgz --no-check-certificate`
- `tar -xf cockroach-v22.2.2.linux-amd64.tgz`

Then, create a directory to store the external libraries. Copy the libararies to the directory:
- `sudo mkdir -p /usr/local/lib/cockroach`
- `sudo cp -i cockroach-v22.2.2.linux-amd64/lib/libgeos.so /usr/local/lib/cockroach/`
- `sudo cp -i cockroach-v22.2.2.linux-amd64/lib/libgeos_c.so /usr/local/lib/cockroach/`
- `sudo cp -i cockroach-v22.2.2.linux-amd64/cockroach /usr/local/bin`

For any Troubleshooting consult: https://www.cockroachlabs.com/docs/stable/install-cockroachdb-linux.html

### Building binaries:
> :warning: Make sure to have configured all environment variables: source the TBB `/opt/intel/oneapi/setvars.sh` and the helper script `helper-scripts/set_env.sh` to make sure TBB, java, and jemalloc environment variables are set.

   
Finally, you can build the binaries:
Navigate to `Pequin-Artifact/src` and build:
- `make -j $(nproc)`



#### Troubleshooting:
   
##### Problems with locating libraries:
   
1. You may need to export your `LD_LIBRARY_PATH` if your installations are in non-standard locations:
   The default install locations are:

   <!--- Hoard: usr/local/lib -->
   - Jemalloc: usr/local/lib
   - TaoPq:  /usr/local/lib
   - Nlohman/JSON:  /usr/local/include
   - Secp256k1:  /usr/local/lib
   - CryptoPP: /usr/local/include  /usr/local/bin   /usr/local/share
   - Blake3: /usr/local/lib
   - Donna: /usr/local/lib
   - Googletest: /usr/local/lib /usr/local/include
   - Protobufs: /usr/local/lib
   - Intel TBB: /opt/intel/oneapi
   - CockroachDB: /usr/local/lib/cockroach  /usr/local/bin/cockroach

 Run `export LD_LIBRARY_PATH=/usr/local/lib:/usr/local/share:/usr/local/include:$LD_LIBRARY_PATH` (adjusted depending on where `make install` puts the libraries) followed by `sudo ldconfig`.
   
2. If you installed more Intel API tools besides "Intel oneAPI Threading Building Blocks", then the Intel oneAPI installation might have  installed a different protobuf binary. Since the application pre-pends the Intel install locations to `PATH`, you may need to manually pre-pend the original directories. Run: `export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:$PATH`

3. Building googletest differently:
   
   If you get an error: `make: *** No rule to make target '.obj/gtest/gtest-all.o', needed by '.obj/gtest/gtest_main.a'.  Stop.` try to install googletest directly into the `src` directory as follows:
   1. `git clone https://github.com/google/googletest.git`
   2. `cd googletest`
   3. `git checkout release-1.10.0`
   4. `rm -rf <Relative-Path>/Pequin-Artifact/src/.obj/gtest`
   5. `mkdir <Relative-Path>/Pequin-Artifact/src/.obj`
   6. `cp -r googletest <Relative-Path>/Pequin-Artifact/src/.obj/gtest`
   7. `cd <Relative-Path>/Pequin-Artifact/src/.obj/gtest`
   8. `cmake CMakeLists.txt`
   9. `make -j $(nproc)`
   10. `g++ -isystem ./include -I . -pthread -c ./src/gtest-all.cc`
   11. `g++ -isystem ./include -I . -pthread -c ./src/gtest_main.cc`

### Confirming that Basil binaries work locally (optional sanity check)
You may want to run a simple toy single server/single client experiment to validate that the binaries you built do not have an obvious error.

Navigate to `Pequin-Artifact/src`. Run `./keygen.sh` to generate local priv/pub key-pairs. 

Run server:

`./server-tester.sh`

Then run client:

`./client-tester.sh`

The client should finish within 10 seconds and the output file `client-0.out` should include summary of the transactions committed at the end.

