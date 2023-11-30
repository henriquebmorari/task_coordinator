import glob
import json
import yaml
import signal
import pathlib
from time import sleep
from task import TaskPool
from datetime import datetime
from argparse import ArgumentParser
from coord_client import KazooCoordClient

def main():
    parser = ArgumentParser()
    parser.add_argument('action', type=str, choices=['run', 'update-conf', 'status'])
    parser.add_argument('--zk-host', dest='zk_host', type=str, metavar='HOSTS_STRING')
    parser.add_argument('--conf-path', dest='conf_path', type=lambda p: str(pathlib.Path(p).absolute()))
    parser.add_argument('--thread-classes-paths', dest='thread_classes_paths', type=lambda paths: list(map(lambda path: str(pathlib.Path(path).absolute()), paths.split(','))))
    parser.add_argument('--poll-interval', dest='poll_interval', type=int, default=5)
    parser.add_argument('--workername', dest='workername', type=str)
    parser.add_argument('--appname', dest='appname', type=str)
    parser.add_argument('-f', dest='format', type=str, choices=['json', 'yaml'], default='json')
    args = parser.parse_args()

    def validate_args(required_options: list):
        option_errors = []
        for option in required_options:
            dest = parser._option_string_actions[option].dest
            if not getattr(args, dest):
                option_errors += [option]
        if option_errors:
            parser.error(f'the following arguments are required: {", ".join(option_errors)}')

    if args.action == 'run':
        validate_args(['--zk-host', '--workername', '--appname', '--thread-classes-paths'])
        run(args)
    elif args.action ==  'update-conf':
        validate_args(['--zk-host', '--conf-path'])
        update_conf(args)
    elif args.action ==  'status':
        validate_args(['--zk-host', '--appname'])
        status(args)
    
    exit(0)

def run(args):
    try:
        client = KazooCoordClient(args.zk_host)
        task_pool = TaskPool(client, args.workername, args.appname, args.conf_path, args.thread_classes_paths)

        def handler(signum, frame):
            task_pool.stop()
            exit(0)
        signal.signal(signal.SIGINT, handler)

        while True:
            task_pool.watch()
            sleep(int(args.poll_interval))

    except Exception as e:
        print(str(e))
        if 'task_pool' in locals():
            task_pool.stop()

def update_conf(args):
    client = KazooCoordClient(args.zk_host)

    for tasks_conf_file in glob.glob(args.conf_path + '/*.yaml'):
        with open(tasks_conf_file, 'r') as f:
            task_conf = yaml.safe_load(f)

        if 'common_args' in task_conf and task_conf['common_args']:
            for taskname,task in task_conf['tasks'].items():
                new_args = task_conf['common_args'].copy()
                if 'args' in task:
                    new_args.update(task['args'])
                task_conf['tasks'][taskname]['args'] = new_args

        task_conf_node = f"/apps/{task_conf['appname']}/conf/tasks"
        client.ensure_path(task_conf_node)
        client.set(task_conf_node, yaml.dump(task_conf['tasks']).encode("utf-8"))

        print(task_conf_node)

def status(args):
    client = KazooCoordClient(args.zk_host)

    app_path = f'/apps/{args.appname}'

    workers = {}
    party_path = f'{app_path}/party'
    for member in client.get_children(party_path):
        member_data = client.get(f'{party_path}/{member}')
        worker = member_data[0].decode('ascii')
        ctime = datetime.fromtimestamp(member_data[1].ctime / 1000).isoformat()
        data_dict = {'joined_at': ctime}
        workers[worker] = data_dict

    worker_task = {}
    locks_path = f'{app_path}/locks'
    for task in client.get_children(locks_path):
        children = client.get_children(f'{locks_path}/{task}')
        if not children:
            continue
        task_lock = sorted(children)[0]
        task_lock_node = f'{locks_path}/{task}/{task_lock}'
        node_data = client.get(task_lock_node)
        worker = node_data[0].decode('ascii')
        ctime = datetime.fromtimestamp(node_data[1].ctime / 1000).isoformat()
        data_dict = {'lock_acquired_at': ctime}

        if worker in worker_task:
            worker_task[worker][task] = data_dict
        else:
            worker_task[worker] = {task: data_dict}

    conf = {}
    conf_path = f'{app_path}/conf'
    for conf_type in client.get_children(conf_path):
        conf_data = client.get(f'{conf_path}/{conf_type}')
        conf_yaml = conf_data[0].decode('ascii')
        conf_dict = yaml.safe_load(conf_yaml)
        mtime = datetime.fromtimestamp(conf_data[1].mtime / 1000).isoformat()
        data_dict = {'modified_at': mtime, 'data': conf_dict}
        conf[conf_type] = data_dict

    tasks = [ { 'taskname': taskname } for taskname in conf['tasks']['data'].keys() ]

    if args.format == 'json':
        formatter = json.dumps
    elif args.format == 'yaml':
        formatter = yaml.dump

    print(formatter({
        'workers': workers,
        'tasks': tasks,
        'tasks_per_worker': worker_task,
        'configurations': conf
    }))

    client.stop()
    client.close()

if __name__ == "__main__":
    main()
