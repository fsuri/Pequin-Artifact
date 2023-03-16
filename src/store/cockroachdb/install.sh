if [[ "$OSTYPE" == "darwin"* ]]; then
    # Mac OSX
    brew install libboost-program-options-dev protobuf cmake libboost-thread-dev protobuf-compiler libeigen3-dev build-essential cabal-install libboost-system-dev libpq-dev liblzma-dev zlib1g-dev postgresql-server-dev-all libprotobuf-dev libbz2-dev libboost-test-dev 
elif [[ "$OSTYPE" == ""* ]]; then
    sudo apt-get install postgresql-server-dev-all libprotobuf-dev cabal-install libboost-thread-dev liblzma-dev libpq-dev cmake libeigen3-dev zlib1g-dev libboost-program-options-dev libboost-test-dev build-essential libboost-system-dev libbz2-dev protobuf-compiler 


PB_REL="https://github.com/protocolbuffers/protobuf/releases"
curl -LO $PB_REL/download/v3.17.0/protoc-3.17.0-linux-x86_64.zip
unzip protoc-3.17.0-linux-x86_64.zip -d $HOME/.local

export PATH="$PATH:$HOME/.local/bin"

apt-cache policy protobuf-compiler
brew install libpq
brew install protobuf

# for unix
sudo apt install build-essential cmake libboost-system-dev libboost-thread-dev libboost-program-options-dev libboost-test-dev libeigen3-dev zlib1g-dev libbz2-dev liblzma-dev 

sudo apt install libpq-dev postgresql-server-dev-all protobuf-compiler libprotobuf-dev cabal-install