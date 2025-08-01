'''
 Copyright 2021 Matthew Burke <matthelb@cs.cornell.edu>
                Florian Suri-Payer <fsp@cs.cornell.edu>

 Permission is hereby granted, free of charge, to any person
 obtaining a copy of this software and associated documentation
 files (the "Software"), to deal in the Software without
 restriction, including without limitation the rights to use, copy,
 modify, merge, publish, distribute, sublicense, and/or sell copies
 of the Software, and to permit persons to whom the Software is
 furnished to do so, subject to the following conditions:

 The above copyright notice and this permission notice shall be
 included in all copies or substantial portions of the Software.
 
 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 SOFTWARE.

'''
import json
import time
import concurrent.futures
import os
import sys
import threading

from utils.remote_util import *
from utils.git_util import *
from utils.eval_util import *
from lib.experiment_codebase import *

def is_using_master(config):
    return not 'use_master' in config or config['use_master']

def collect_exp_data(config, remote_exp_directory, local_directory_base, executor):
    download_futures = []
    remote_directory = os.path.join(remote_exp_directory, config['out_directory_name'])
    if is_using_master(config):
        master_host = get_master_host(config)
        copy_remote_directory_to_local(os.path.join(local_directory_base, 'master'), config['emulab_user'], master_host, remote_directory)
    for i in range(len(config['server_names'])):
        server_host = get_server_host(config, i)
        download_futures.append(executor.submit(copy_remote_directory_to_local, os.path.join(local_directory_base, 'server-%d' % i), config['emulab_user'], server_host, remote_directory))
        for j in range(config['client_nodes_per_server']):
            client_host = get_client_host(config, i, j)
            download_futures.append(executor.submit(copy_remote_directory_to_local, os.path.join(local_directory_base, 'client-%d-%d' % (i, j)), config['emulab_user'], client_host, remote_directory))
    return download_futures

def kill_servers(config, executor, kill_args=' -9'):
    """kills all servers in config using kill_args

    Parameters
    ----------
    config : dic
        config file
    executor : ThreadPoolExecutor
    kill_args : str, optional
        The kill signal for kill command
        The default is -9 
    """

    futures = []

    # Define n as the minimun number of replicas that is satisfies the fault tolerance
    if config['replication_protocol'] == 'indicus' or config['replication_protocol'] == 'pequin':
        n = 5 * config['fault_tolerance'] + 1
    elif config['replication_protocol'] == 'pbft' or config['replication_protocol'] == 'hotstuff' or config['replication_protocol'] == 'pg-smr' or \
        config['replication_protocol'] == 'peloton-smr' or config['replication_protocol'] == 'bftsmart' or config['replication_protocol'] == 'augustus':
        n = 3 * config['fault_tolerance'] + 1
    elif config['replication_protocol'] == 'crdb':
        n = config['fault_tolerance']
    else:
        n = 2 * config['fault_tolerance'] + 1

    if config['replication_protocol'] == 'pg':
        n = 1

    # Build a server idx -> kill script map
    x = len(config['server_names']) // n
    kill_commands = {}
    for group in range(config['num_groups']):
        
        process_idx = group // x
        
        for i in range(n):
            server_idx = (i * x + group) % len(config['server_names'])
            if is_exp_remote(config):
                if not server_idx in kill_commands:
                    # add kill command by its process name
                    # e.g. ../bin/server
                    kill_commands[server_idx] = kill_remote_process_by_name_cmd(
                        os.path.join(config['base_remote_bin_directory_nfs'],
                            config['bin_directory_name'],
                            config['server_bin_name']), kill_args)
                # add kill command by its port name
                # e.g. xxx:7087
                kill_commands[server_idx] += '; %s' % kill_remote_process_by_port_cmd(
                    config['server_port'] + process_idx, kill_args)
            else:
                if not server_idx in kill_commands:
                    # add kill command by its process name
                    # e.g. ../bin/server
                    kill_commands[server_idx] = kill_remote_process_by_name_cmd(
                        os.path.join(config['src_directory'],
                            config['bin_directory_name'],
                            config['server_bin_name']), kill_args)
                # add kill command by its port name
                # e.g. xxx:7087
                kill_commands[server_idx] += '; %s' % kill_remote_process_by_port_cmd(
                    config['server_port'] + process_idx, kill_args)
    
    for idx, cmd in kill_commands.items():
        if is_exp_remote(config):
            server_host = get_server_host(config, idx)
            # Run kill script in remote server
            futures.append(executor.submit(run_remote_command_sync, cmd,
                config['emulab_user'], server_host))
        else:
            futures.append(executor.submit(subprocess.run, cmd,
                stdout=subprocess.PIPE, universal_newlines=True, shell=True))
    concurrent.futures.wait(futures)

def kill_clients_no_config(config, n, m, executor):
    futures = []
    for j in range(m):
        for i in range(n):
            client_host = get_client_host(config, i, j)
            if is_exp_remote(config):
                futures.append(executor.submit(kill_remote_process_by_name,
                    os.path.join(config['base_remote_bin_directory_nfs'],
                        config['bin_directory_name'],
                        config['client_bin_name']), config['emulab_user'],
                    client_host, ' -9'))
            else:
                futures.append(executor.submit(kill_process_by_name,
                    os.path.join(config['src_directory'],
                        config['bin_directory_name'],
                        config['client_bin_name']), ' -9'))
    concurrent.futures.wait(futures)

def kill_clients(config, executor):
    kill_clients_no_config(config, len(config['server_names']), config['client_nodes_per_server'], executor)

def kill_master(config, remote_exp_directory):
    master_host = get_master_host(config)
    if is_exp_remote(config):
        kill_remote_process_by_name(os.path.join(
            config['base_remote_bin_directory_nfs'],
            config['bin_directory_name'], config['master_bin_name']),
            config['emulab_user'], master_host, ' -9')
        kill_remote_process_by_port(config['master_port'],
                config['emulab_user'], master_host, ' -9')
    else:
        kill_process_by_name(os.path.join(
            config['base_remote_bin_directory_nfs'],
            config['bin_directory_name'], config['master_bin_name']), ' -9')
        kill_process_by_port(config['master_port'], ' -9')

def terminate_clients_on_timeout(timeout, cond, client_ssh_threads):
    start = time.time()
    need_terminate = True
    cond.acquire()
    while time.time() - start < timeout:
        need_terminate = not cond.wait(timeout - (time.time() - start))
    if need_terminate:
        for c in client_ssh_threads:
            c.terminate()
    cond.release()

def wait_for_clients_to_terminate(config, client_ssh_threads):
    cond = threading.Condition()
    timeout_thread = threading.Thread(
            target=terminate_clients_on_timeout,
            args=(config['client_experiment_length'] + 30,
                cond,
                client_ssh_threads))
    timeout_thread.daemon = True
    timeout_thread.start()
    for c in client_ssh_threads:
        c.wait()
    cond.acquire()
    cond.notify()
    cond.release()
    #time.sleep(config['client_experiment_length'] + 10)

def start_clients(config, local_exp_directory, remote_exp_directory, run):
    client_processes = []
    total = 0
    for i in range(len(config['server_names'])):
        for j in range(config['client_nodes_per_server']):
            if is_exp_local(config):
                os.makedirs(os.path.join(local_exp_directory,
                    config['out_directory_name'],
                    'client-%d-%d' % (i, j)))
            client_host = get_client_host(config, i, j)

            if is_exp_remote(config): #Do these commands only once per client host.
                if(True or config['replication_protocol_settings']['hyper_threading'] == 'false') :
                        print("Disabling HT and Turbo; sourcing TBB")
                        cmd1 = 'sudo /usr/local/etc/disable_HT.sh'
                        #cmd1 = 'sudo /usr/local/etc/enable_HT.sh'
                        #cmd1 = 'sudo echo off | sudo tee /sys/devices/system/cpu/smt/control'
                        run_remote_command_async(cmd1, config['emulab_user'], client_host)
                        cmd2 = 'sudo /usr/local/etc/turn_off_turbo.sh'
                        run_remote_command_async(cmd2, config['emulab_user'], client_host)
                        #perm = 'sudo chmod +x ~/indicus/bin/benchmark'
                        #run_remote_command_async(perm, config['emulab_user'], client_host)

            appended_client_commands = ''
            cmd3 = 'source /opt/intel/oneapi/setvars.sh --force; '
            #run_remote_command_async(cmd3, config['emulab_user'], server_host, detach=False)
            appended_client_commands += cmd3
            for k in range(config['client_processes_per_client_node']):
                # TODO hack for now to start many clients simultaneously
                appended_client_commands += get_client_cmd(config, i, j, k,
                        run, local_exp_directory, remote_exp_directory)
                if k != 0 and k % 128 == 0:
                    if appended_client_commands[-2:] == '& ':
                        appended_client_commands = appended_client_commands[:-2]
                    if is_exp_remote(config):
                        client_processes.append(run_remote_command_async(
                            appended_client_commands + ' & wait', config['emulab_user'],
                            client_host, False))
                    else:
                        client_processes.append(subprocess.Popen(
                            appended_client_commands + ' & wait', shell=True))
                    appended_client_commands = ''
                total += 1
                if 'client_total' in config and total >= config['client_total']:
                    break
            if len(appended_client_commands) > 0:
                if appended_client_commands[-2:] == '& ':
                        appended_client_commands = appended_client_commands[:-2]
                if is_exp_remote(config):
                    #cmd4 = 'export LD_LIBRARY_PATH=/usr/lib/jvm/java-11-openjdk-amd64/lib/server/:$LD_LIBRARY_PATH;'  #Moved cmd4+5 to set_env.sh
                    #cmd5 = 'export LD_PRELOAD=/usr/local/lib/libhoard.so;'
                    #cmd5 = 'export LD_PRELOAD=/usr/local/lib/libjemalloc.so;'
                    cmd6 = 'source /usr/local/etc/set_env.sh;' # echo $LD_PRELOAD;' # source .bashrc' (disk image doesnt store bashrc. Could write to it dynamically though.) 
                    appended_client_commands = cmd6 + appended_client_commands

                    client_processes.append(run_remote_command_async(
                        appended_client_commands + ' & wait', config['emulab_user'],
                        client_host, False))
                else:
                    client_processes.append(subprocess.Popen(
                        appended_client_commands + ' & wait', shell=True))
            if 'client_total' in config and total >= config['client_total']:
                break
        if 'client_total' in config and total >= config['client_total']:
            break

    return client_processes

def start_servers(config, local_exp_directory, remote_exp_directory, run):
    server_threads = []
    if config['replication_protocol'] == 'indicus' or config['replication_protocol'] == 'pequin':
        n = 5 * config['fault_tolerance'] + 1
    elif config['replication_protocol'] == 'pbft' or config['replication_protocol'] == 'hotstuff' or config['replication_protocol'] == 'pg-smr' or \
        config['replication_protocol'] == 'peloton-smr' or config['replication_protocol'] == 'bftsmart' or config['replication_protocol'] == 'augustus':
        n = 3 * config['fault_tolerance'] + 1
    elif config['replication_protocol'] == 'crdb':
        n = config['fault_tolerance']
    else:
        n = 2 * config['fault_tolerance'] + 1
    
    if config['replication_protocol'] == 'pg':
        n = 1
        
    x = len(config['server_names']) // n
    start_commands = {}
    for group in range(config['num_groups']):
        process_idx = group // x
        for i in range(n):
            server_idx = (i * x + group) % len(config['server_names'])
            if is_exp_local(config):
                os.makedirs(os.path.join(local_exp_directory,
                    config['out_directory_name'], 'server-%d' % server_idx),
                    exist_ok=True)

            server_command = get_replica_cmd(config, server_idx,
                    process_idx, group, run, local_exp_directory, remote_exp_directory)
            if not server_idx in start_commands:
                start_commands[server_idx] = '(%s)' % server_command
            else:
                start_commands[server_idx] += ' & (%s)' % server_command

    for idx, cmd in start_commands.items():
        if is_exp_remote(config):
            server_host = get_server_host(config, idx)
           
            ## add ssh for hyperthreading off and turbo off
            #config['replication_protocol_settings']['hyper_threading']
            if(True or config['replication_protocol_settings']['hyper_threading'] == 'false') :
                print("Disabling HT and Turbo; sourcing TBB")
                cmd1 = 'sudo /usr/local/etc/disable_HT.sh'
                #cmd1 = 'sudo /usr/local/etc/enable_HT.sh'
                #cmd1 = 'sudo echo off | sudo tee /sys/devices/system/cpu/smt/control'
                run_remote_command_async(cmd1, config['emulab_user'], server_host)
                cmd2 = 'sudo /usr/local/etc/turn_off_turbo.sh'
                run_remote_command_async(cmd2, config['emulab_user'], server_host)
                #perm = 'sudo chmod +x ~/indicus/bin/server'
                #run_remote_command_async(perm, config['emulab_user'], server_host)
            
            
            ## set-up dbs for pg
            if config['replication_protocol'] == 'pg':
                print("setting up databases for postgres usage")

                if config['pg_replicated'] == True:
                    print ("using replicated postgres")

                    ### Clean previous data:
                    replica_host = "us-east-1-1.pg-smr.pequin-pg0.utah.cloudlab.us"
                    cmd100 = '/usr/lib/postgresql/12/bin/pg_ctl -D ~/replica/db stop >/dev/null'
                    run_remote_command_sync(cmd100, config['emulab_user'], replica_host)
                    cmd100 = '/usr/lib/postgresql/12/bin/pg_ctl -D ~/primary/db stop >/dev/null'
                    run_remote_command_sync(cmd100, config['emulab_user'], server_host)
                    cmd101 = 'sudo umount primary'
                    run_remote_command_sync(cmd101, config['emulab_user'], server_host)

                    ### Set up new primary:
                    cmd102 = './primary_aux.sh'
                    run_remote_command_sync(cmd102, config['emulab_user'], server_host)

                    ### Set up new replica:
                    cmd103 = './postgres_replica.sh 1'
                    run_remote_command_sync(cmd103, config['emulab_user'], replica_host)

                else:
                    print("using non-replicated postgres")
                    # Dropping old pg cluster (if exists)
                    cmd7 = 'sudo ./postgres_service.sh -r'
                    run_remote_command_sync(cmd7, config['emulab_user'], server_host)
                    # Creating a single db per machine
                    cmd8 = 'sudo ./postgres_service.sh -n 1'
                    run_remote_command_sync(cmd8, config['emulab_user'], server_host)
                    cmd11 = 'sudo pg_ctlcluster 12 pgdata stop'
                    run_remote_command_sync(cmd11, config['emulab_user'], server_host)
                    cmd12 = 'sudo pg_ctlcluster 12 pgdata start'
                    run_remote_command_sync(cmd12, config['emulab_user'], server_host)
                    print("Waiting for the setup")

                    # # Dropping old pg cluster (if exists)
                    # cmd7 = 'sudo /usr/local/etc/postgres_service.sh -r'
                    # run_remote_command_sync(cmd7, config['emulab_user'], server_host)
                    # # Creating a single db per machine
                    # cmd8 = 'sudo /usr/local/etc/postgres_service.sh -n 1'
                    # run_remote_command_sync(cmd8, config['emulab_user'], server_host)
                    # cmd11 = 'sudo pg_ctlcluster 12 pgdata stop'
                    # run_remote_command_sync(cmd11, config['emulab_user'], server_host)
                    # cmd12 = 'sudo pg_ctlcluster 12 pgdata start'
                    # run_remote_command_sync(cmd12, config['emulab_user'], server_host)
                    # print("Waiting for the setup")


            
            ## set-up dbs for pg-smr
            if config['replication_protocol'] == 'pg-smr':
                cmd7 = 'sudo ./postgres_service.sh -r > /dev/null 2>&1;'
                # # Creating a single db per machine
                cmd8 = 'sudo ./postgres_service.sh -n 1 > /dev/null 2>&1;'
                cmd11 = 'sudo pg_ctlcluster 12 pgdata stop;'
                cmd12 = 'sudo pg_ctlcluster 12 pgdata start;'
                #cmd13 = 'echo \"Finished PG setup\";'
                cmd = cmd7 + cmd8 + cmd11 + cmd12+ cmd

                #cmd7 = 'sudo /usr/local/etc/postgres_service.sh -c;'
                #cmd = cmd7  +cmd



            ##
            cmd3 = 'source /opt/intel/oneapi/setvars.sh --force; '
            #run_remote_command_async(cmd3, config['emulab_user'], server_host, detach=False)
            cmd =  cmd3 + cmd
            #cmd4 = 'export LD_LIBRARY_PATH=/usr/lib/jvm/java-11-openjdk-amd64/lib/server/:$LD_LIBRARY_PATH;'
            #cmd5 = 'export LD_PRELOAD=/usr/local/lib/libhoard.so;'
            #cmd5 = 'export LD_PRELOAD=/usr/local/lib/libjemalloc.so;'
            cmd6 = 'source /usr/local/etc/set_env.sh; export ASAN_OPTIONS=new_delete_type_mismatch=0;' # echo $LD_PRELOAD;' #; source .bashrc' #TODO: Or try sourcing .bashrc //Replace cmd4+cmd5..
            cmd = cmd6 + cmd


            server_threads.append(run_remote_command_async(cmd,
                config['emulab_user'], server_host, detach=False))
        else:
            print(server_command)
            server_threads.append(subprocess.Popen(cmd, shell=True))
        time.sleep(0.1)
    time.sleep(1)


    return server_threads

def start_master(config, local_exp_directory, remote_exp_directory, run):
    if is_exp_remote(config):
        exp_directory = remote_exp_directory
        path_to_master_bin = os.path.join(
                config['base_remote_bin_directory_nfs'],
                config['bin_directory_name'], config['master_bin_name'])
    else:
        exp_directory = local_exp_directory
        path_to_master_bin = os.path.join(
                config['src_directory'],
                config['bin_directory_name'], config['master_bin_name'])


    master_command = ' '.join([str(x) for x in [path_to_master_bin,
        '-N', len(config['server_names']),
        '-port', config['master_port']]])

    stdout_file = os.path.join(exp_directory,
        config['out_directory_name'], 'master-stdout-%d.log' % run)
    stderr_file = os.path.join(exp_directory,
        config['out_directory_name'], 'master-stderr-%d.log' % run)

    if is_exp_remote(config):
        master_command = tcsh_redirect_output_to_files(master_command,
                stdout_file, stderr_file)
    else:
        master_command = '%s 1> %s 2> %s' % (master_command, stdout_file, stderr_file)

    master_command = 'cd %s; %s' % (exp_directory, master_command)

    if is_exp_remote(config):
        master_host = get_master_host(config)
        return run_remote_command_async(master_command, config['emulab_user'],
                master_host, detach=False)
    else:
        return subprocess.Popen(master_command, shell=True)

SERVERS_SETUP = {}

def prepare_remote_server(config, server_host, local_exp_directory, remote_out_directory):
    if server_host not in SERVERS_SETUP:
        set_file_descriptor_limit(config['max_file_descriptors'], config['emulab_user'], server_host)
        change_mounted_fs_permissions(config['project_name'], config['emulab_user'], server_host, config['base_mounted_fs_path'])
        SERVERS_SETUP[server_host] = True
    change_mounted_fs_permissions(config['project_name'], config['emulab_user'], server_host, config['base_remote_exp_directory'])
    copy_path_to_remote_host(local_exp_directory, config['emulab_user'],
        server_host, config['base_remote_exp_directory'])
    run_remote_command_sync('mkdir -p %s' % remote_out_directory, config['emulab_user'], server_host)
    prepare_remote_server_codebase(config, server_host, local_exp_directory, remote_out_directory)

def prepare_remote_client(config, i, j, local_exp_directory, remote_out_directory):
    client_host = get_client_host(config, i, j)
    clean = 'sudo rm -rf /mnt/extra/experiments/*;' #Clean experiments folder.
    run_remote_command_async(clean, config['emulab_user'], client_host)

    if client_host not in SERVERS_SETUP:
        set_file_descriptor_limit(config['max_file_descriptors'], config['emulab_user'], client_host)
        change_mounted_fs_permissions(config['project_name'], config['emulab_user'], client_host, config['base_mounted_fs_path'])
        SERVERS_SETUP[client_host] = True
    change_mounted_fs_permissions(config['project_name'], config['emulab_user'], client_host, config['base_remote_exp_directory'])
    copy_path_to_remote_host(local_exp_directory, config['emulab_user'],
        client_host, config['base_remote_exp_directory'])
    run_remote_command_sync('mkdir -p %s' % remote_out_directory, config['emulab_user'], client_host)
    prepare_remote_server_codebase(config, client_host, local_exp_directory, remote_out_directory)

def prepare_remote_exp_directories(config, local_exp_directory, executor):
    remote_directory = os.path.join(config['base_remote_exp_directory'], os.path.basename(local_exp_directory))
    remote_out_directory = os.path.join(remote_directory, config['out_directory_name'])
    if is_using_master(config):
        master_host = get_master_host(config)
        if master_host not in SERVERS_SETUP:
            set_file_descriptor_limit(config['max_file_descriptors'], config['emulab_user'], master_host)
            change_mounted_fs_permissions(config['project_name'], config['emulab_user'], master_host, config['base_mounted_fs_path'])
            SERVERS_SETUP[master_host] = True
        change_mounted_fs_permissions(config['project_name'], config['emulab_user'], master_host, config['base_remote_exp_directory'])
        copy_path_to_remote_host(local_exp_directory, config['emulab_user'],
            master_host, config['base_remote_exp_directory'])
        run_remote_command_sync('mkdir -p %s' % remote_out_directory, config['emulab_user'], master_host)
    futures = []
    for i in range(len(config['server_names'])):
        server_host = get_server_host(config, i)
        clean = 'sudo rm -rf /mnt/extra/experiments/*;' #Clean experiments folder.
        run_remote_command_async(clean, config['emulab_user'], server_host)

        futures.append(executor.submit(prepare_remote_server, config, server_host,
            local_exp_directory, remote_out_directory))
        prepare_remote_server(config, server_host, local_exp_directory, remote_out_directory)
        for j in range(config['client_nodes_per_server']):
            futures.append(executor.submit(prepare_remote_client, config, i, j,
                local_exp_directory, remote_out_directory))
    concurrent.futures.wait(futures)
    return remote_directory

def collect_and_calculate(config, client_config_idx, remote_exp_directory, local_out_directory, executor):
    if is_exp_remote(config):
        download_futures = collect_exp_data(config, remote_exp_directory,
                local_out_directory, executor)
        concurrent.futures.wait(download_futures)
    stats, op_latencies, op_times, client_op_latencies, client_op_times = calculate_statistics(config, local_out_directory)
    generate_cdf_plots(config, local_out_directory, stats, executor)
    generate_ot_plots(config, local_out_directory, stats, op_latencies, op_times, client_op_latencies, client_op_times, executor)
    return local_out_directory

def get_arg_max():
    return int(subprocess.run(['getconf', 'ARG_MAX'], stdout=subprocess.PIPE,
        universal_newlines=True).stdout)

def setup_delays(config, wan, executor):
    futures = []
    name_to_ip = get_name_to_ip_map(config, config['emulab_user'],
            get_server_host(config, 0))
    if is_using_master(config):
        master_host = get_master_host(config)
        master_ip_to_delay = get_ip_to_delay(config, name_to_ip,
                config['master_server_name'])
        master_interface = get_exp_net_interface(config['emulab_user'],
                master_host)
        if wan:
            add_delays_for_ips(master_ip_to_delay, master_interface,
                   config['max_bandwidth'], config['emulab_user'], master_host)
        else:
            run_remote_command_sync('sudo tc qdisc del dev %s root' % master_interface, config['emulab_user'], master_host)

    for i in range(len(config['server_names'])):
        server_host = get_server_host(config, i)
        server_ip_to_delay = get_ip_to_delay(config, name_to_ip,
                config['server_names'][i], True)
        client_ip_to_delay = get_ip_to_delay(config, name_to_ip,
                config['server_names'][i])
        if wan:
            futures.append(executor.submit(get_iface_add_delays,
                server_ip_to_delay, config['max_bandwidth'],
                config['emulab_user'], server_host))
        else:
            futures.append(executor.submit(remove_delays, config['emulab_user'],
                server_host))

        for j in range(config['client_nodes_per_server']):
            client_host = get_client_host(config, i, j)
            if wan:
                futures.append(executor.submit(get_iface_add_delays,
                    client_ip_to_delay, config['max_bandwidth'],
                    config['emulab_user'], client_host))
            else:
                futures.append(executor.submit(remove_delays,
                    config['emulab_user'], client_host))

    concurrent.futures.wait(futures)

def get_local_path_to_bins(config):
    return os.path.join(config['src_directory'], config['bin_directory_name'])

def copy_binaries_to_nfs(config, executor):
    if 'remade_binaries' not in SERVERS_SETUP:
        remake_binaries(config)
        SERVERS_SETUP['remade_binaries'] = True
    nfs_enabled = not 'remote_bin_directory_nfs_enabled' in config or config['remote_bin_directory_nfs_enabled']
    n = 1 if nfs_enabled else len(config['server_names'])
    futures = []
    for i in range(n):
        server_host = get_server_host(config, i)
        #run_remote_command_sync('mkdir %s' % config['base_remote_bin_directory_nfs'], config['emulab_user'], server_host)
        run_remote_command_sync('mkdir -p %s' % os.path.join(config['base_remote_bin_directory_nfs'], config['bin_directory_name']), config['emulab_user'], server_host)
        if server_host not in SERVERS_SETUP:
            futures.append(executor.submit(copy_path_to_remote_host,
                os.path.join(config['src_directory'],
                    config['bin_directory_name'], config['server_bin_name']), config['emulab_user'],
                server_host, os.path.join(config['base_remote_bin_directory_nfs'], config['bin_directory_name'], config['server_bin_name'])))
        if not nfs_enabled:
            for j in range(config['client_nodes_per_server']):
                client_host = get_client_host(config, i, j)
                #run_remote_command_sync('mkdir %s' % config['base_remote_bin_directory_nfs'], config['emulab_user'], client_host)
                run_remote_command_sync('mkdir -p %s' % os.path.join(config['base_remote_bin_directory_nfs'], config['bin_directory_name']), config['emulab_user'], client_host)
                if client_host not in SERVERS_SETUP:
                    futures.append(executor.submit(copy_path_to_remote_host,
                        os.path.join(config['src_directory'],
                            config['bin_directory_name'], config['client_bin_name']), config['emulab_user'],
                        client_host, os.path.join(config['base_remote_bin_directory_nfs'], config['bin_directory_name'], config['client_bin_name'])))
    concurrent.futures.wait(futures)


def is_exp_local(config):
    return 'run_locally' in config and config['run_locally']

def is_exp_remote(config):
    return not is_exp_local(config)

def run_experiment(config_file, client_config_idx, executor):
    with open(config_file) as f:
        config = json.load(f)
        if not 'server_regions' in config:
            config['server_regions'] = {}
            for server_name in config['server_names']:
                config['server_regions'][server_name] = [server_name]

        if not 'region_rtt_latencies' in config:
            config['region_rtt_latencies'] = config['server_ping_latencies']

        if not 'client_stats_blacklist' in config:
            config['client_stats_blacklist'] = []
        if not 'client_combine_stats_blacklist' in config:
            config['client_combine_stats_blacklist'] = []
        if not 'client_cdf_plot_blacklist' in config:
            config['client_cdf_plot_blacklist'] = []
        if not 'client_total' in config:
            config['client_total'] = config['client_nodes_per_server'] * config['client_processes_per_client_node'] * len(config['server_names'])

        wan = 'server_emulate_wan' in config and (config['server_emulate_wan'] and (not 'run_locally' in config or not config['run_locally']))
        if not 'run_locally' in config or not config['run_locally']:
            print('Setting up emulated WAN latencies.')
            setup_delays(config, wan, executor)
        kill_clients(config, executor)
        kill_servers(config, executor)
        if 'remade_binaries' not in SERVERS_SETUP:
            remake_binaries(config)
            SERVERS_SETUP['remade_binaries'] = True
        if is_exp_remote(config):
            copy_binaries_to_nfs(config, executor)
        setup_nodes(config)
        local_exp_directory = prepare_local_exp_directory(config, config_file)
        local_out_directory = os.path.join(local_exp_directory,
                config['out_directory_name'])
        if is_exp_local(config):
            os.makedirs(local_out_directory)
        remote_exp_directory = None
        if is_exp_remote(config):
            remote_exp_directory = prepare_remote_exp_directories(config, local_exp_directory, executor)
        kill_clients(config, executor)
        for i in range(config['num_experiment_runs']):
            servers_alive = False
            retries = 0
            master_thread = None
            server_threads = None
            while not servers_alive and retries <= config['max_retries']:
                if is_using_master(config):
                    kill_master(config, remote_exp_directory)
                    master_thread = start_master(config, local_exp_directory,
                            remote_exp_directory, i)
                kill_servers(config, executor)
                time.sleep(2)
                server_threads = start_servers(config, local_exp_directory, remote_exp_directory, i)
                all_alive = True
                for st in range(len(server_threads)):
                    if server_threads[i].poll() != None:
                        print("Server thread %d not alive." % st)
                        all_alive = False
                        break
                servers_alive = all_alive
                retries += 1
            if not servers_alive:
                sys.stderr.write('Failed to start all servers.\n')
                raise
            if 'server_load_time' in config:
                print("Waiting %d seconds for servers to load." % config['server_load_time'])
                time.sleep(config['server_load_time'])

            client_threads = start_clients(config, local_exp_directory,
                    remote_exp_directory, i)
            
            wait_for_clients_to_terminate(config, client_threads)
            kill_clients(config, executor)
            time.sleep(1)
            kill_servers(config, executor, ' -15')
            for server_thread in server_threads:
                server_thread.terminate()
            if is_using_master(config):
                master_thread.terminate()
                kill_master(config, remote_exp_directory)
        return executor.submit(collect_and_calculate, config,
                client_config_idx, remote_exp_directory, local_out_directory,
                executor)

def run_multiple_experiments(config_file, executor):
    """ Runs experiments specified in configuration file

    Parameters
    ----------
    config_file : str
        config file name
    executor : ThreadPoolExecutor

    Returns
    ----------
    exp_dir: exp_dir
        experiemnt output directory
    out: list
        list of output directory and sub directory

    """
    start = time.time()
    exp_dir = None
    out_dirs = None
    with open(config_file) as f:
        config = json.load(f)

        if not 'src_commit_hash' in config:
            config['src_commit_hash'] = get_current_branch(config['src_directory'])

        # verify that we can run all of the experiments
        if len(config['experiment_independent_vars']) == 0:
            sys.stderr.write('Need at least 1 independent variable to run multiple experiments.\n')
            sys.exit(1)
        if not 'experiment_independent_vars_unused' in config:
            config['experiment_independent_vars_unused'] = config['experiment_independent_vars']
        for i in range(len(config['experiment_independent_vars_unused'])):
            for j in range (len(config['experiment_independent_vars_unused'][i])):
                for k in range(j):
                    if len(config[config['experiment_independent_vars_unused'][i][j]]) != len(config[config['experiment_independent_vars_unused'][i][k]]):
                        sys.stderr.write('%s and %s arrays in config file must have same length.\n' % (
                            config['experiment_independent_vars_unused'][i][j],
                            config['experiment_independent_vars_unused'][i][k]))
                        sys.exit(1)

        config_name = os.path.splitext(os.path.basename(config_file))[0]

        exp_futs = [] # future experiments
        exp_futs_idxs = [] # future experiment indices
        config_files = []
        indep_vars_list = []

        exp_dir = get_timestamped_exp_dir(config)
        os.makedirs(exp_dir, exist_ok=True)

        out_dirs = []
        sub_out_dirs = []
        for i in range(len(config[config['experiment_independent_vars_unused'][0][0]])):
            config_new = config.copy()
            config_new['base_local_exp_directory'] = exp_dir
            config_new['experiment_independent_vars_unused'] = config['experiment_independent_vars_unused'][1:]

            for j in range(len(config['experiment_independent_vars_unused'][0])):
                config_new[config['experiment_independent_vars_unused'][0][j]] = config[config['experiment_independent_vars_unused'][0][j]][i]

            config_file_new = os.path.join(exp_dir, '%s-%d.json' % (config_name, i))
            with open(config_file_new, 'w+') as f_new:
                json.dump(config_new, f_new, indent=2, sort_keys=True)
            config_files.append(config_file_new)

            if len(config_new['experiment_independent_vars_unused']) == 0:
                exp_futs.append(run_experiment(config_file_new, i, executor))
                exp_futs_idxs.append(i)
            else:
                out_directory, sub_out_directories = run_multiple_experiments(config_file_new, executor)
                out_dirs.append(out_directory)
                sub_out_dirs.append(sub_out_directories)

        retries = 0
        while len(out_dirs) < len(config[config['experiment_independent_vars_unused'][0][0]]) and retries <= config['max_retries']:
            retry_exp_futs = []
            for i in range(len(exp_futs)):
                try:
                    out_dir = exp_futs[i].result()
                    sub_out_dirs.insert(exp_futs_idxs[i], out_dir)
                except:
                    print('Unexpected error during %s %d: ' % (config_files[exp_futs_idxs[i]], exp_futs_idxs[i]))
                    print(traceback.format_exc())
                    retry_exp_futs.append(exp_futs_idxs[i])
            if len(out_dirs) == len(config[config['experiment_independent_vars_unused'][0][0]]):
                break
            exp_futs = []
            exp_futs_idxs = []
            for j in retry_exp_futs:
                exp_futs.append(run_experiment(config_files[j], j, executor))
                exp_futs_idxs.append(j)
            retries += 1

        print("%s took %f seconds!" % (config_name, time.time() - start))

        print(exp_dir)
        out = [sub_out_dirs, out_dirs]
        generate_plots(config, exp_dir, out)
    return exp_dir, out

def run_varying_clients_experiment(config_file, executor):
    start = time.time()
    exp_dir = None
    out_dirs = None
    with open(config_file) as f:
        config = json.load(f)
        if len(config['client_nodes_per_server']) != len(config['client_processes_per_client_node']):
            sys.stderr.write('%s and %s arrays in config file must have same length.\n' % ('client_nodes_per_server', 'client_processes_per_node'))

        config_name = os.path.splitext(os.path.basename(config_file))[0]
        exp_futs = []
        exp_futs_idxs = []
        config_files = []
        exp_dir = get_timestamped_exp_dir(config)
        os.makedirs(exp_dir, exist_ok=True)
        for i in range(len(config['client_nodes_per_server'])):
            config_new = config.copy()
            config_new['base_local_exp_directory'] = exp_dir
            n = config['client_nodes_per_server'][i]
            m = config['client_processes_per_client_node'][i]
            if 'client_total' in config:
                config_new['client_total'] = config['client_total'][i]
            if 'client_threads_per_process' in config:
                config_new['client_threads_per_process'] = config['client_threads_per_process'][i]
            config_new['client_nodes_per_server'] = n
            config_new['client_processes_per_client_node'] = m
            config_file_new = os.path.join(exp_dir,
                '%s-cli-%d-%d.json' % (config_name, n, m))
            with open(config_file_new, 'w+') as f_new:
                json.dump(config_new, f_new, indent=2, sort_keys=True)
            config_files.append(config_file_new)
            exp_futs.append(run_experiment(config_file_new, i, executor))
            exp_futs_idxs.append(i)


        retries = 0
        out_dirs = {}
        while len(out_dirs) < len(config['client_nodes_per_server']) and retries < config['max_retries']:
            retry_exp_futs = []
            for i in range(len(exp_futs)):
                try:
                    out_dir = exp_futs[i].result()
                    out_dirs[exp_futs_idxs[i]] = out_dir
                except:
                    print('Unexpected error during %s %d: ' % (config_files[exp_futs_idxs[i]], exp_futs_idxs[i]))
                    print(traceback.format_exc())
                    retry_exp_futs.append(exp_futs_idxs[i])
            if len(out_dirs) == len(config['client_nodes_per_server']):
                break
            exp_futs = []
            exp_futs_idxs = []
            for j in retry_exp_futs:
                exp_futs.append(run_experiment(config_files[j], j, executor))
                exp_futs_idxs.append(j)
            retries += 1

        generate_tput_lat_plots(config, exp_dir, out_dirs)
        print("%s took %f seconds!" % (config_name, time.time() - start))
    return exp_dir, out_dirs

def run_multiple_protocols_experiment(config_file, executor=None):
    start = time.time()
    exp_dir = None
    with open(config_file) as f:
        existing_exec = False
        if executor == None:
            executor = concurrent.futures.ThreadPoolExecutor(max_workers=16)
        else:
            existing_exec = True
        config = json.load(f)
        if not existing_exec == None:
            kill_clients_no_config(config, len(config['server_names']), max(config['client_nodes_per_server']), executor)
        config_name = os.path.splitext(os.path.basename(config_file))[0]
        out_directories = []
        sub_out_directories = []
        exp_dir = get_timestamped_exp_dir(config)
        os.makedirs(exp_dir, exist_ok=True)
        for i in range(len(config['replication_protocol'])):
            config_new = config.copy()
            config_new['base_local_exp_directory'] = exp_dir
            server_replication_protocol = config['replication_protocol'][i]
            config_new['replication_protocol'] = server_replication_protocol
            config_new['plot_cdf_series_title'] = config['plot_cdf_series_title'][i]
            config_new['plot_tput_lat_series_title'] = config['plot_tput_lat_series_title'][i]
            config_new['replication_protocol_settings'] = config['replication_protocol_settings'][i]
            config_file_new = os.path.join(exp_dir,
                '%s-%s-%d.json' % (config_name, server_replication_protocol.replace('_', '-'), i))
            with open(config_file_new, 'w+') as f_new:
                json.dump(config_new, f_new, indent=2, sort_keys=True)
            protocol_out_directory, protocol_sub_out_dirs = run_varying_clients_experiment(config_file_new, executor)
            out_directories.append(protocol_out_directory)
            sub_out_directories.append(protocol_sub_out_dirs)
        generate_agg_cdf_plots(config, exp_dir, sub_out_directories)
        generate_agg_tput_lat_plots(config, exp_dir, out_directories)
        print("%s took %f seconds!" % (config_name, time.time() - start))
        if not existing_exec:
            executor.shutdown()
        return exp_dir, out_directories, sub_out_directories

def run_multiple_tail_at_scale(config_file):
    start = time.time()
    exp_dir = None
    with open(config_file) as f:
        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
            config = json.load(f)
            if True:
                kill_clients_no_config(config, len(config['server_names']), max(config['client_nodes_per_server']), executor)
                config_name = os.path.splitext(os.path.basename(config_file))[0]
                directories = []
                out_directories = []
                sub_out_directories = []
                exp_dir = get_timestamped_exp_dir(config)
                os.makedirs(exp_dir, exist_ok=True)
                for i in range(len(config['client_tail_at_scale'])):
                    config_new = config.copy()
                    config_new['base_local_exp_directory'] = exp_dir
                    config_new['client_tail_at_scale'] = config['client_tail_at_scale'][i]
                    config_file_new = os.path.join(exp_dir, '%s-%d.json' % (config_name, i))
                    with open(config_file_new, 'w+') as f_new:
                        json.dump(config_new, f_new, indent=2, sort_keys=True)
                    directory, protocol_out_dirs, protocol_sub_out_dirs = run_multiple_protocols_experiment(config_file_new, executor)
                    directories.append(directory)
                    out_directories.append(protocol_out_dirs)
                    sub_out_directories.append(protocol_sub_out_dirs)
                print("%s took %f seconds!" % (config_name, time.time() - start))
            generate_tail_at_scale_plots(config, exp_dir, sub_out_directories)
            #generate_tail_at_scale_plots(config, 'experiments/emulab/2018-09-18-02-46-26', [[['experiments/emulab/2018-09-18-02-46-26/2018-09-18-02-46-31/2018-09-18-02-46-31/2018-09-18-02-47-03/out/']], [['experiments/emulab/2018-09-18-02-46-26/2018-09-18-02-48-14/2018-09-18-02-48-14/2018-09-18-02-48-32/out/']]])
