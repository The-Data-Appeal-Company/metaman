# Metaman

[![Go Report Card](https://goreportcard.com/badge/github.com/The-Data-Appeal-Company/metaman)](https://goreportcard.com/report/github.com/The-Data-Appeal-Company/metaman)
![Go](https://github.com/The-Data-Appeal-Company/metaman/workflows/Go/badge.svg?branch=master)
[![license](https://img.shields.io/github/license/The-Data-Appeal-Company/metaman.svg)](LICENSE)

### Usage
```
metaman is the command-line tool/api to interact with metastore.
Currently supported metastore are: Glue, Hive.
Supported operations are:
- create tables
- drop tables along with data
- sync different metastore

Usage:
metaman [command]

Available Commands:
api         run an api for metastire management
completion  Generate the autocompletion script for the specified shell
create      create tables
drop        drop table
help        Help about any command
sync        sync tables between metastore

Flags:
-c, --config string   Configuration path default ./config.yml (default "config.yml")
-h, --help            help for metaman

Use "metaman [command] --help" for more information about a command.
``` 

### Create
```
Usage:
  metaman create [flags]

Flags:
  -d, --database string            database name
  -h, --help                       help for create
      --metastores stringArray     list of metastore
  -t, --tables-definition string   path to json with tables definition
```

### Drop
```
Usage:
  metaman drop [flags]

Flags:
  -d, --database string      database name
      --delete-data          delete table data
  -h, --help                 help for drop
  -m, --metastore string     metastore
      --tables stringArray   list of table names
```

### Sync
```
Usage:
  metaman sync [flags]

Flags:
  -d, --database string   database name
      --delete-tables     delete tables from target non existing in source
  -h, --help              help for sync
  -s, --source string     source metastore
  -t, --target string     target metastore
```

### Api
```
Usage:
  metaman api [flags]

Flags:
  -h, --help   help for api
```