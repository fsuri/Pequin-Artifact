# Setting up Cloudlab <a name="cloudlab"></a>
   
To run experiments on [Cloudlab](https://www.cloudlab.us/) you will need to request an account with your academic email (if you do not already have one) and create a new project  To request an account click [here](https://cloudlab.us/signup.php). You can create a new project either directly while requesting an account, or by selecting "Start/Join project" in your account drop down menu.

 > :warning: **[NOTE]** On Cloudlab, make sure to select `bash` as your default shell in your Account settings.

We have included screenshots below for easy usebility. Follow the [cloudlab manual](http://docs.cloudlab.us/) if you need additional information for any of the outlined steps. 

If you face any issues with registering, please make a post at the [Cloudlab forum](https://groups.google.com/g/cloudlab-users?pli=1). Replies are usually very swift during workdays on US mountain time (MT). Alternatively -- but *not recommended* --, if you are unable to get access to create a new project, request to join project "pequin" and wait to be accepted. Reach out to Florian Suri-Payer <fsp@cs.cornell.edu> if you are not accepted, or unsure how to join.

![image](https://user-images.githubusercontent.com/42611410/129490833-eb99f58c-8f0a-43d9-8b99-433af5dab559.png)

To start experiments that connect to remote Cloudlab machines, you will need to set up ssh and register your key with Cloudlab. This is necessary regardless of whether you are using your local machine or a Cloudlab control machine. 

Install ssh if you do not already have it: `sudo apt-get install ssh`. To create an ssh key and register it with your ssh agent follow these instructions: https://docs.github.com/en/github/authenticating-to-github/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent. Next, register your public key under your Cloudlab account user->Manage SSH Keys. Alternatively, you may add your keys driectly upon project creation.

Next, you are ready to start up an experiment:

To use a pre-declared profile supplied by us, start an experiment using the public profile ["pequin-base"](https://www.cloudlab.us/p/pequin/pequin-base). If you face any issues using this profile (or the disk images specified below), please make a post at the [Cloudlab forum](https://groups.google.com/g/cloudlab-users?pli=1) or contact Florian Suri-Payer <fsp@cs.cornell.edu>.
![image](https://user-images.githubusercontent.com/42611410/129490911-8c97d826-caa7-4f04-95a7-8a2c8f3874f7.png)

This profile by default starts with 18 server machines and 18 client machines, all of which use m510 hardware on the Utah cluster. 
This profile includes two disk images "pequin-base.server" (`urn:publicid:IDN+utah.cloudlab.us+image+pequin-PG0:pequin-base.server`) and "pequin-base.client" (`urn:publicid:IDN+utah.cloudlab.us+image+pequin-PG0:pequin-base.client`) that already include all dependencies and additional setup necessary to run experiments. Check the box "Use Control Machine" if you want to build binaries and run all experiments from one of the Cloudlab machines.
![image](https://user-images.githubusercontent.com/42611410/129490922-a99a1287-6ecc-4d50-b05d-dfe7bd0496d9.png)
Click "Next" and name your experiment (e.g. "pequin"). In the example below, our experiment name is "indicus", and the project name is "morty". All our pre-supplied experiment configurations use these names as default, and you will need to change them accordingly to your chosen names (see section "Running Experiments").
![image](https://user-images.githubusercontent.com/42611410/129490940-6c527b08-5def-4158-afd2-bc544e4758ab.png)
Finally, set a duration and start your experiment. Starting all machines may take a decent amount of time, as the server disk images contain large datasets that need to be loaded. Wait for it to be "ready":
![image](https://user-images.githubusercontent.com/42611410/129490974-f2b26280-d5e9-42ca-a9fe-82b80b8e2349.png)
You may ssh into the machines to test your connection using the ssh commands shown under "List View" or by using `ssh <cloudlab-username>@<node-name>.<experiment-name>.<project-name>-pg0.<cluster-domain-name>`. In the example below it would be: `ssh fs435@us-east-1-0.indicus.morty-pg0.utah.cloudlab.us`.
![image](https://user-images.githubusercontent.com/42611410/129490991-035a1865-43c3-4238-a264-e0d43dd0095f.png)


Since experiments require a fairly large number of machines, you may have to create a reservation in order to have enough resources. Go to the "Make reservation tab" and make a reservation for 36 m510 machines on the Utah cluster (37 if you plan to use a control machine). 
![image](https://user-images.githubusercontent.com/42611410/129491361-b13ef31b-707b-4e02-9c0f-800e6d9b4def.png)

Our profile by default allocates 18 servers (36 total machines), enough to run Pesto on TPCC for 3 shards. Most experiments, however, do not need this many machines: if you cannot get access to enough machines, simply use 6 server machines (remove the trailing 9 server names from the profile, i.e. keep only `['us-east-1-0', 'us-east-1-1', 'us-east-1-2', 'eu-west-1-0', 'eu-west-1-1', 'eu-west-1-2']`). This suffices to run all but the sharding experiment. 

Note, that the names are just placeholder names and do NOT correspond to real region placement. To emulate WAN latencies our experiment configs allow assigning ping latencies to sever-names.

### Using a control machine (skip if using local machine)
When using a control machine (and not your local machine) to start experiments, you will need to source setvars.sh and may need to export the LD_LIBRARY_PATH for the Java dependencies (see section "Install Dependencies") before building. You will need to do this everytime you start a new control machine because those may not be persisted across images.

Before connecting to your control machine, start an SSH agent in your local terminal
with `eval $(ssh-agent -s)`. Then add your Cloudlab SSH key to the agent `ssh-add <path to Cloudlab key>`.

Connect using your Cloudlab username and the following domain name:
`ssh -A <cloudlab-user>@control.<experiment-name>.<project-name>.utah.cloudlab.us`.
It is crucial that you connect using the `-A` setting in order to transfer your
local SSH agent to the control machine.  You may need to add `-pg0` to your
project name in order to connect, i.e. if your project is called "sosp108", it
may need to be "sosp108-pg0" in order to connect.

### Using a custom profile (skip if using pre-supplied profile)

If you decide to instead [create your own profile](https://www.cloudlab.us/manage_profile.php), use the following parameters (be careful to follow the same naming conventions of our profile for the servers or the experiment scripts/configuration provided will not work). You will need to buid your own disk image from scratch, as the public image is tied to the public profile. (You can try if the above images work, but likely they will not).

- Number of Replicas: `['us-east-1-0', 'us-east-1-1', 'us-east-1-2', 'eu-west-1-0', 'eu-west-1-1', 'eu-west-1-2', 'ap-northeast-1-0', 'ap-northeast-1-1', 'ap-northeast-1-2', 'us-west-1-0', 'us-west-1-1', 'us-west-1-2', 'eu-central-1-0', 'eu-central-1-1', 'eu-central-1-2', 'ap-southeast-2-0', 'ap-southeast-2-1', 'ap-southeast-2-2']`
- Number of sites (DCs): 6
- Replica Hardware Type: `m510`
- Replica storage: `64GB`
- Replica disk image: Your own (server) image
- Client Hardware Type: `'m510'` (add the '')
- Client storage: `16GB`
- Client disk image: Your own (client) image
- Number of clients per replica: `1`
- Total number of clients: `0` (this will still create 18 clients)
- Use control machine?:  Check this if you plan to use a control machine
- Control Hardware Type: `m510`

### Building and configuring disk images from scratch (skip if using pre-supplied images)
If you want to build an image from scratch, follow the instructions below:

Start by choosing to load a default Ubuntu 20.04 LTS image as "Replica disk image" and "Client disk image": `urn:publicid:IDN+emulab.net+image+emulab-ops:UBUNTU20-64-STD`.  (For Ubuntu 18.04 LTS use: `urn:publicid:IDN+emulab.net+image+emulab-ops:UBUNTU18-64-STD`.)

Next, follow the above manual installation guide (section "Installing Dependencies" to install all dependencies (you can skip adding tbb setvars.sh to .bashrc). 

Additionally, you will have to install the following requisites:
1. **NTP**:  https://vitux.com/how-to-install-ntp-server-and-client-on-ubuntu/ 
   
   Confirm that it is running: `sudo service ntp status` (check for status Active)

<!-- 2. **Data Sets**: Build TPCC/Smallbank data sets and move them to /usr/local/etc/   **THIS IS OUTDATED FOR PEQUIN. NO LONGER NEEDED!**
   
      **Store TPCC data:**
   - Navigate to`Pequin-Artifact/src/store/benchmark/async/tpcc` 
   - Run `./tpcc_generator --num_warehouses=<N> > tpcc-<N>-warehouse`
   - We used 20 warehouses, so replace `<N>` with `20`
   - Move output file to `/usr/local/etc/tpcc-<N>-warehouse`
   - You can skip this on client machines and create a separate disk image for cients without. This will considerably reduce image size and speed up experiment startup. 
 
      **Store Smallbank data:**
   - Navigate to `Pequin-Artifact/src/store/benchmark/async/smallbank/`
   - Run `./smallbank_generator_main --num_customers=<N>`
   - We used 1 million customers, so replace `<N>` with `1000000`
   - The script will generate two files, smallbank_names, and smallbank_data. Move them to /usr/local/etc/
   - The server needs both, the client needs only smallbank_names (not storing smallbank_data saves space for the image) -->

   
2. **Public Keys**: Generate Pub/Priv key-pairs, move them to /usr/local/etc/donna/

    - Navigate to `Pequin-Artifact/src` and run `keygen.sh`
    - By default keygen.sh uses type 4 = Ed25519 (this is what we evaluated the systems with); it can be modifed secp256k1 (type 3), but this requires editing the config files as well. (do not do this, to re-produce our experiments)
    - Move the key-pairs in the `/keys` folder to `/usr/local/etc/indicus-keys/donna/` (or to `/usr/local/etc/indicus-keys/secp256k1/` depending on what type used)

3. **Helper scripts**: 

    Navigate to Pequin-Artifact/helper-scripts. Copy all three scripts (with the exact name) and place them in `/usr/local/etc` on the Cloudlab machine. Add execution permissions: `chmod +x disable_HT.sh; chmod +x turn_off_turbo.sh; chmod +x set_env.sh` The scripts are used at runtime by the experiments to disable hyperthreading and turbo respectively, as well as to set environment variables for jemalloc and Java (for BFTSmart).
    
4. **Pre-Troubleshooting**:

   To avoid any issue when running *TxBFTSmart* locate your `java.security` file (`/usr/lib/jvm/java-11-openjdk-amd64/conf/security/java.security`) and comment out (or remove) the following line: `jdk.tls.disabledAlgorithms=SSLv3, TLSv1, RC4, DES, MD5withRSA, DH keySize < 1024 EC keySize < 224, 3DES_EDE_CBC, anon, NULL`

   
Once complete, create a new disk image (separate ones for server and client if you want to save space/time). Then, start the profile by choosing the newly created disk image.
To create a disk image, select "Create Disk Image" and name it accordingly.
![image](https://user-images.githubusercontent.com/42611410/129491499-eb7d0618-5dc4-4942-a25a-3b4a955c5077.png)
