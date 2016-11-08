#!/usr/bin/env python
"""
fp (FirePlug) ... Run your code on remote Docker hosts

Usage:
    $ cp -r ~/.aws .              # Copy your aws configuration
    $ docker-machine create       # Create docker machine
    $ fp --init                   # Initialize fp configuration
    $ fp python your_script.py    # Run your script on current directory

Author: Kohei
License: BSD3
"""
import os
import sys
import yaml
import json
import subprocess
import argparse
import configparser as ConfigParser


VERSION = '1.1'


if os.path.exists('.fp'):
    __conf = ConfigParser.ConfigParser()
    __conf.read('.fp')


def get_single_config_file_value(header, key):
    return __conf.get(header, key)


# --------------
# DOCKER-MACHINE


def _docker_machine_cmd(cmd):
    """
    Run docker-machine command and return the stdout as list of string.
    """
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    ret = []
    for line in proc.stdout:
        ret.append( line.decode('utf-8').rstrip() )
        proc.stdout.flush()
    return ret


def docker_machine_inspect(docker_host):
    cmd = ['docker-machine', 'inspect', docker_host]
    ret = _docker_machine_cmd(cmd)
    ret_string = "\n".join(ret)

    return json.loads(ret_string)


def docker_machine_config(docker_host):
    cmd = ['docker-machine', 'config', docker_host]
    return _docker_machine_cmd(cmd)


def docker_hosts():
    cmd = ['docker-machine', 'ls']
    ret = _docker_machine_cmd(cmd)

    # TODO: considering the status is runnning or not.
    return [line.split(' ')[0] for line in ret[1:]]


# ------
# DOCKER


def _docker_cmd(cmd):
    """
    Run docker command and return the stdout as list of string.
    """
    pass


def calc_num_current_process(docker_host):
    docker_option_list = docker_machine_config(docker_host)

    docker_option_list += ['ps']
    docker_cmd = ['docker'] + docker_option_list

    proc = subprocess.Popen(docker_cmd, stdout=subprocess.PIPE)
    ret = []
    for line in proc.stdout:
        ret.append(line)
    proc.wait()

    # Subtract 1 for header
    num_proc = len(ret) - 1

    return num_proc


def build_docker_image(docker_host, args):
    working_image = get_single_config_file_value('docker', 'working_image')
    docker_option_list = docker_machine_config(docker_host)

    docker_option_list += ['build', '-t', working_image, '.']
    docker_cmd = ['docker'] + docker_option_list

    print("({}) Build images ...".format(docker_host))
    if args.verbose:
        print("({}) >>> ".format(docker_host) + " ".join(docker_cmd))
        proc = subprocess.Popen(docker_cmd)
        proc.wait()
    else:
        with open(os.devnull, 'w') as devnull:
            proc = subprocess.Popen(docker_cmd,
                                    stdout=devnull,
                                    stderr=devnull)
            proc.wait()
    return


# ----
# main

def sync_s3_bucket(docker_host, args, reverse=False):
    # Get info from configuration file
    working_image = get_single_config_file_value('docker', 'working_image')
    bucket_path = get_single_config_file_value('sync', 's3')
    sync_to = get_single_config_file_value('sync', 'datapath')
    mount_path = "{}:{}".format(
        get_single_config_file_value('filesystem', 'hostside_path'),
        get_single_config_file_value('filesystem', 'mount_point'))

    docker_option_list = docker_machine_config(docker_host)
    docker_option_list += [
        'run', '--rm', '-i', '-v', mount_path, working_image]
    run_cmd = ['aws', 's3', 'sync', bucket_path, sync_to]

    # Upload base
    if reverse is True:
        run_cmd = ['aws', 's3', 'sync', sync_to, bucket_path]

    docker_cmd = ['docker'] + docker_option_list + run_cmd

    print("({}) Sync files ...".format(docker_host))
    if args.verbose:
        print("({}) >>> ".format(docker_host) + " ".join(docker_cmd))
        proc = subprocess.Popen(docker_cmd)
        proc.wait()
    else:
        with open(os.devnull, 'w') as devnull:
            proc = subprocess.Popen(docker_cmd,
                                    stdout=devnull,
                                    stderr=devnull)
            proc.wait()
    return


def run_docker(docker_host, script_args, args):
    working_image = get_single_config_file_value('docker', 'working_image')
    mount_path = "{}:{}".format(
        get_single_config_file_value('filesystem', 'hostside_path'),
        get_single_config_file_value('filesystem', 'mount_point'))

    docker_option_list = docker_machine_config(docker_host)
    docker_option_list += [
        'run', '--rm', '-i', '-v', mount_path, working_image
    ]
    docker_cmd = ['docker'] + docker_option_list + script_args

    print("({}) Run container ...".format(docker_host))
    if args.verbose:
        print("({}) >>> ".format(docker_host) + " ".join(docker_cmd))

    proc = subprocess.Popen(docker_cmd)
    proc.wait()
    return


def mkdir_datapath(docker_host, args):
    sync_datapath = get_single_config_file_value('sync', 'datapath')
    cmd = ['sudo', 'mkdir', '-p', sync_datapath]
    cmd = ['docker-machine', 'ssh', docker_host] + cmd

    # Run
    if args.verbose:
        print("({}) >>> ".format(docker_host) + " ".join(cmd))

    # TODO: error handling
    _docker_machine_cmd(cmd)


def describe_gcp_ipaddress(docker_host):
    cmd = ['gcloud', 'compute', 'instances', 'describe', docker_host]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    ret = []
    for line in proc.stdout:
        ret.append( line.decode('utf-8').rstrip() )
        proc.stdout.flush()

    inspect_ret = yaml.load("\n".join(ret))
    ipaddr = inspect_ret['networkInterfaces'][0]['accessConfigs'][0]['natIP']
    return ipaddr


def rsync_files(docker_host, args, sync_remote_path, sync_local_path, reverse=False):
    inspect_ret = docker_machine_inspect(docker_host)
    driver_name = inspect_ret['DriverName']
    if driver_name == 'google':
        sshkey_path = inspect_ret['Driver']['SSHKeyPath']
        ipaddr = describe_gcp_ipaddress(docker_host)
    elif driver_name == 'amazonec2':
        sshkey_path = inspect_ret['Driver']['SSHKeyPath']
        ipaddr = inspect_ret['Driver']['IPAddress']
    else:
        raise RuntimeError(
            "Unsupported DriverName is used: {}".format(driver_name))

    # Enable to ssh login as root (need to consider here)
    cmd = ['sudo', 'cp', '/home/ubuntu/.ssh/authorized_keys', '/root/.ssh/']
    cmd = ['docker-machine', 'ssh', docker_host] + cmd

    # Run
    if args.verbose:
        print("({}) >>> ".format(docker_host) + " ".join(cmd))

    # TODO: error handling
    _docker_machine_cmd(cmd)

    # Run rsync (Note that the end of copy source shoud be '/')
    ssh_cmd = " ".join([
        'ssh', '-i', sshkey_path,
        '-o', 'StrictHostKeyChecking=no',
        '-o', 'UserKnownHostsFile=/dev/null'])
    cmd = [
        'rsync', '-az', '-e',
        '{}'.format(ssh_cmd),
        '--copy-links',
        '--progress',
        sync_local_path,
        'root@{}:{}'.format(ipaddr, sync_remote_path)]

    if reverse is True:
        source_place = cmd[-2]
        target_place = cmd[-1]
        cmd[-2] = target_place
        cmd[-1] = source_place

    # TODO: check the end of source
    if not cmd[-2].endswith('/'):
        cmd[-2] = cmd[-2] + '/'

    print("({}) Sync files...".format(docker_host))
    if args.verbose:
        print("({}) >>> ".format(docker_host) + " ".join(cmd))

    # Start rsync command
    with open(os.devnull, 'w') as devnull:
        proc = subprocess.Popen(cmd, stderr=devnull)
        proc.wait()


def rsync_data_files(docker_host, args, reverse):
    sync_remote_path = get_single_config_file_value('sync', 'datapath')
    sync_local_path = get_single_config_file_value('sync', 'localpath')
    rsync_files(docker_host, args, sync_remote_path, sync_local_path, reverse=reverse)


def check_host_is_ready(docker_host):
    # TODO: check running status
    if calc_num_current_process(docker_host) == 0:
        return True
    else:
        return False


def run(args, remaining_args):
    """
    Build docker image, sync files and run the specified command.

    sync path is defined on a config file, which is located on `.fp`.
    """
    host_list = docker_hosts()
    bucket_path = get_single_config_file_value('sync', 's3')

    if args.host is not None:
        if args.host in host_list:
            host_list = [args.host]
        else:
            print("RuntimeError: Unknown host: {}".format(args.host))
            sys.exit(1)

    for docker_host in host_list:
        if not check_host_is_ready(docker_host):
            continue

        # Check datapath (mkdir -p)
        mkdir_datapath(docker_host, args)

        # >>> Build
        if not args.nobuild:
            build_docker_image(docker_host, args)

        # >>> Sync
        if not args.nosync and not args.outsync:
            if bucket_path == 'None':
                rsync_data_files(docker_host, args, reverse=False)
            else:
                sync_s3_bucket(docker_host, args)

        # >>> Run
        if not args.norun:
            run_docker(docker_host, remaining_args, args)

        # >>> Sync
        if not args.nosync and not args.insync:
            if bucket_path == 'None':
                rsync_data_files(docker_host, args, reverse=True)
            else:
                sync_s3_bucket(docker_host, args, reverse=True)

        sys.exit(0)

    # TODO: handling the case when `--host` option is used.
    print("RuntimeError: No docker host is ready to run.")
    sys.exit(1)


def show_version():
    print(VERSION)
    sys.exit(0)


def init():
    project_name = input("[1/5] Enter your project name: ")

    s3_bucket = input(
        "[2/5] Enter your s3 bucket name (Default: None): "
    ).strip() or 'None'

    local_data_path = input(
        "[3/5] Enter data path on local client (Default: data): "
    ).strip() or 'data'

    local_library_paths = input(
        "[4/5] Enter paths for local libraries you'd like you use, separated by commas (Default: None): "
    ).strip() or 'None'

    base_docker_image = input(
        "[5/5] Enter your base docker image (Default: smly/alpine-kaggle): "
    ).strip() or 'smly/alpine-kaggle'

    if not project_name:
        raise RuntimeError('Please enter a project name')

    if s3_bucket != 'None' and not os.path.exists('.aws'):
        raise RuntimeError('Put your .aws config inside current directory')

    remote_data_path = os.path.join("/data", project_name)
    s3_path = "s3://{name:s}/{proj_name:s}".format(
        name=s3_bucket,
        proj_name=project_name
    )
    if s3_bucket in ['None', 'False']:
        s3_path = 'None'

    # write out .dockerignore
    if os.path.exists('./.dockerignore'):
        raise RuntimeError(".dockerignore already exists")

    with open('./.dockerignore', 'w') as f:
        f.write("""*.swp
data
trunk
.git
Dockerfile
.dockerignore
*~
""")

    # write out dockerfile
    if os.path.exists('./Dockerfile'):
        raise RuntimeError("Dockerfile is already exists.")

    with open("./Dockerfile", 'w') as f:
        f.write("""FROM {base_docker_image:s}
RUN ln -s {remote_data_path:s} /root/data
COPY ./ /root/
WORKDIR /root
""".format(base_docker_image=base_docker_image,
           remote_data_path=remote_data_path))

    # write out fp configuration file
    if os.path.exists('./.fp'):
        raise RuntimeError(".fp already exists")

    with open("./.fp", 'w') as f:
        f.write("""[filesystem]
hostside_path = /data
mount_point = /data

[docker]
base_image = {base_image:s}
working_image = {working_image:s}

[sync]
s3 = {s3_path:s}
datapath = {remote_data_path:s}
localpath = {local_data_path:s}
local_library_paths = {local_library_paths}
""".format(s3_path=s3_path,
           working_image=project_name,
           remote_data_path=remote_data_path,
           local_data_path=local_data_path,
           base_image=base_docker_image,
           local_library_paths=local_library_paths
))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--init",
        action='store_true',
        default=False,
        help='Create configuration file and Dockerfile.')
    parser.add_argument(
        "--version",
        action='store_true',
        default=False,
        help='Show version info')
    parser.add_argument(
        "--norun",
        action='store_true',
        default=False,
        help="No run")
    parser.add_argument(
        "--nobuild",
        action='store_true',
        default=False,
        help="No build")
    parser.add_argument(
        "--nosync",
        action='store_true',
        default=False,
        help="No sync")
    parser.add_argument(
        "--insync",
        action='store_true',
        default=False,
        help="insync only")
    parser.add_argument(
        "--outsync",
        action='store_true',
        default=False,
        help="outsync only")
    parser.add_argument(
        "--verbose",
        action='store_true',
        default=False,
        help="Verbose mode")
    parser.add_argument(
        "--host",
        default=None,
        help='Specify a docker host to run.')
    args, remaining_args = parser.parse_known_args()

    if args.init:
        init()
        sys.exit(0)

    if not os.path.exists('.fp'):
        print("No config file for FirePlug on current directory.")
        print("Initialize FirePlug with `fp --init` first.")
        sys.exit(1)

    if args.version:
        show_version()
    else:
        run(args, remaining_args)
