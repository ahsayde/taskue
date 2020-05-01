# Command Line Interface

Taskue cli is a command line tool for managing taskue resources.
It stores its configuration in `~/.taskue.toml` file and it looks like this

```toml
redis_host = "localhost"    # redis hostname
redis_port = 6379           # redis port
namespace = "default"       # namespace
```

## Syntax
Use the following syntax to run taskue commands from your terminal:

```bash
taskue [RESOURCE] [COMMAND] [OPTIONS]
```

## Resources


### Configuration
Manage configuration

#### Syntax
```bash
taskue config [COMMAND] [OPTIONS]
```

#### Commands

<mark>show</mark> *Show current configuration*

<mark>edit</mark> *Edit configuration file in the editor*

<mark>set</mark> *Set individuals value in configuration*

- **Options**
    - `--redis-host`, `-h` *redis hostname*
    - `--redis-port`, `-p` *redis port number*
    - `--redis-secret`, `-s` *redis password*

<mark>reset</mark> *Reset configuration to the default*


### Namespace

#### Syntax
```bash
taskue namespace [COMMAND] [OPTIONS]
```

#### Commands

<mark>get</mark> *Get current namespace*

<mark>list</mark> *List all namespaces*

<mark>switch</mark> *Switch to namespace*

- **Arguments**
    - `name` *Namespace name*

<mark>delete</mark> *Delete namespace*

- **Arguments**
    - `name` *Namespace name*


### Runner

#### Syntax
```bash
taskue runner [COMMAND] [OPTIONS]
```

#### Commands

<mark>list</mark> *List all runners*

<mark>start</mark> *Start new taskue runner*

- **Options**
    - `--name`, `-n` *Runner name (should be unique)*
    - `--timeout`, `-t` *Runner default timeout*
    - `--queues`, `-q` *Redis password*

- **Usage**
    ```bash
    taskue runner start --name runner-1 --timeout 60 --queue foo --queue bar
    ```

### Workflow

#### Syntax
```bash
taskue workflow [COMMAND] [OPTIONS]
```

#### Commands

<mark>list</mark> *List all workflows*

- **Options**
    - `--page`, `-p` *Page number*
    - `--limit`, `-l` *Results per page*

<mark>get</mark> *Get workflow details*

- **Arguments**
    - `uid` *Workflow unique id*
- **Options**
    - `--json`, `-j` *Return results in json format*

<mark>wait</mark> *Wait until workflow finish*

- **Arguments**
    - `uid` *Workflow unique id*
- **Options**
    - `--timeout`, `-t` *Maximum timeout in seconds*

<mark>delete</mark> *Delete workflow*

- **Arguments**
    - `uid` *Workflow unique id*



### Task

#### Syntax
```bash
taskue task [COMMAND] [OPTIONS]
```

#### Commands

<mark>list</mark> *List all task*

- **Options**
    - `--page`, `-p` *Page number*
    - `--limit`, `-l` *Results per page*

<mark>get</mark> *Get task details*

- **Arguments**
    - `uid` *task unique id*
- **Options**
    - `--json`, `-j` *Return results in json format*

<mark>wait</mark> *Wait until task finish*

- **Arguments**
    - `uid` *Task unique id*
- **Options**
    - `--timeout`, `-t` *Maximum timeout in seconds*

<mark>delete</mark> *Delete task*

- **Arguments**
    - `uid` *Task unique id*
