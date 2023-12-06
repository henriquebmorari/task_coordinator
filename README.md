# Task manager

`task_manager` implements the management of tasks in a distributed manner, using Zookeeper as the lock and party coordinator.

## Usage

### Updating the configuration

Example of tasks configuration file in the `tasks_conf_sample` folder.

The tasks configuration is read from Zookeeper on z-node `/apps/{app name}/conf/tasks`. To update the configuration on Zookeeper run:

```sh
python task_manager update-conf \
--zk-host {zookeeper_hosts} \
--conf-path {tasks_configuration_path}
```

All `.yaml` files inside the `tasks_configuration_path` folder will be read.

### Running a worker

Example of a task thread class in the `thread_classes_sample` folder.

To start a new worker for an application, run:

```sh
python task_manager run \
--zk-host {zookeeper_hosts} \
--workername {worker_name} \
--appname {application_name} \
--conf-path {tasks_configuration_path} \
--thread-classes-paths {tasks_thread_classes_path}
```

The tasks thread classes must be defined in `.py` files, inside the `tasks_thread_classes_path` folder, and extend the `taskmanagerthread.TaskManagerThread` class.

The `--application_name` argument must be the `appname` field contained in the tasks configuration yaml file.

The `--conf-path` argument is optional and can be used for task-specific configuration files.

Example:

```sh
python task_manager run \
--zk-host "zookeeper1:2181,zookeeper2:2181" \
--workername test \
--appname sample \
--conf-path task_manager/tasks_conf_sample \
--thread-classes-paths task_manager/thread_classes_sample
```

### Getting the status for the tasks of a running application

```sh
python task_manager status \
--zk-host {zookeeper_hosts} \
--appname {application_name} \
-f {yaml/json}
```
