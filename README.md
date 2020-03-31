# NGNIX log statistics

The script analyzes the ngnix log file and calculates statistics based on the request time parameter. Script renders the results into `report.html` 
You can refer the `report-2017.06.30.html` as an report example example.
Currently, the usage of customized report template is not supported. To run the script, you have to specify `/log` and `/report` folder to store log files and reports.

### Prerequisites

There are no external dependencies other than `pyymal` library for the external config file. Currently, only YAML configs are supported. Other formats are considered in the future.

```
pip3 install pyyaml
```

### Running the script

A step by step series of examples that tell you how to get a development env running

Say what the step will be

```
python3 log_analyzer/log_analyzer.py
```

You can pass custom config file

```
python3 log_analyzer/log_analyzer.py --config conf.yaml
```


## Running the tests

To run test

```
python3 -m unittest tests/test_log_analyzer.py
```


## Authors

* **Daniil Chepenko** 

## License

This project is licensed under the MIT License

