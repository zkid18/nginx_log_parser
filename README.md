# NGNIX log statistics

The script analyze the ngnix log file and calculate statistics based on request time parameter.
Script renders the results into `report.html`
You can refer the `report-2017.06.30.html` as an report example example.

Currently the usage of customized report template is not supported.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Prerequisites

There are no external dependenices other that `pyymal` libaray for external config file. 
Currently only `yaml` configs are supported. Other format are considered in the future.

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
python3 -m unittest tests/test_log_analyzer
```


## Authors

* **Daniil Chepenko** 

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

