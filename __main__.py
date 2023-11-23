import glob
import yaml
import signal
import pathlib
from time import sleep
from task import TaskPool
from argparse import ArgumentParser
from coord_client import KazooCoordClient

def main():
    parser = ArgumentParser()
    parser.add_argument('action', type=str, choices=['run', 'update-conf'], metavar='HOSTS_STRING')
    parser.add_argument('--zk-host', dest='zk_host', type=str, metavar='HOSTS_STRING')
    parser.add_argument('--conf-path', dest='conf_path', type=lambda p: str(pathlib.Path(p).absolute()))
    parser.add_argument('--thread-classes-paths', dest='thread_classes_paths', type=lambda paths: list(map(lambda path: str(pathlib.Path(path).absolute()), paths.split(','))))
    parser.add_argument('--poll-interval', dest='poll_interval', type=int, default=5)
    parser.add_argument('--workername', dest='workername', type=str)
    parser.add_argument('--appname', dest='appname', type=str)
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

def run(args):
    client = KazooCoordClient(args.zk_host)
    task_pool = TaskPool(client, args.workername, args.appname, args.conf_path, args.thread_classes_paths)

    def handler(signum, frame):
        task_pool.stop()
        exit(0)
    signal.signal(signal.SIGINT, handler)

    while True:
        task_pool.watch()
        sleep(int(args.poll_interval))

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

if __name__ == "__main__":
    main()
