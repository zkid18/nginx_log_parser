#!/usr/bin/env python
# -*- coding: utf-8 -*-


# log_format ui_short '$remote_addr  $remote_usera :$http_x_real_ip [$time_local] "$request" '
#                     '$status $body_bytes_sent "$http_referer" '
#                     '"$http_user_agent" "$http_x_forwarded_for" "$http_X_REQUEST_ID" "$http_X_RB_USER" '
#                     '$request_time';

import gzip
import re
from copy import deepcopy
import os
import sys
from collections import defaultdict
import logging
from string import Template
import argparse
import yaml

config = {
    "REPORT_SIZE": 1000,
    "REPORT_DIR": "./reports",
    "LOG_DIR": "./log",
}

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
parsing_threshold = 0.2


def render(data, report_dict, filename):
    """Function for rendering aggregated list

    Reports are saved in the dictionary specified in config file
    Function render is template dependend.
    Inly template.html is supported

    Agrgs:
        data: Aggregated data
        report_dict: dictionary for report files
        filename: Name of the final report

    Todo:
        Check if specified arguments are valid
    """

    template_dir = os.path.join(ROOT_DIR, 'report.html')
    with open(template_dir, 'r', encoding='utf-8') as html_template:
        template = html_template.read()
    string_template = Template(template)
    filename = os.path.join(ROOT_DIR, report_dict, 'report-{}.html'.format(filename))
    sub_dict = dict(table_json=str(data))
    with open(filename, 'w') as fh:
        fh.write(string_template.safe_substitute(sub_dict))


def save_top_logs(data, report_size, report_dir,  filename):
    """Function for finding top logs sorted by 'time_sum'

    Function finds top URL with the maximum time_sum

    Args:
        data: List of aggregated logs
        report_size: Number of top URL include in report

    Returns:
        Save top logs to plain file
    """
    sorted_by_time_sum = sorted(data, key=lambda x: x['time_sum'])
    top_url = [data['url'] for data in sorted_by_time_sum][:report_size]
    top_log_report_path = os.path.join(ROOT_DIR, report_dir, 'top_log_report_{}'.format(filename))
    with open(top_log_report_path, 'w') as fh:
        for url in top_url:
            fh.write(url + '\n')


def aggregate_log(raw_data):
    ''' Aggregate log files with by following parameters

    Args:
        raw_data: List of dictionaries {url:time_req}

    Returns:
        aggregated_data: List of aggregated logs with follofing keys
        * url - URL from lof
        * count - number of  the URL occurance, the absolute value
        * count_perc - number of  URL occurrance in percentage relative to the total number of requests
        * time_sum - total  $ request_time for a given URL,theabsolute value
        * time_perc - total  $ request_time for a given URL,
            in percent relative to the total $ request_time of all requests
        * time_avg - average  $ request_time for a given URL
        * time_max - maximum $ request_time for a given URL
        * time_med - median  $ request_time for a given URL
    '''

    logging.info("Started aggregation")
    total_number_urls = sum([len(req_time_list) for _, req_time_list in raw_data.items()])
    sum_request_time = sum([sum(req_time_list) for _, req_time_list in raw_data.items()])

    def round_up(num):
        multiplier = 10 ** 3
        inum = int(num*multiplier)
        ceil = inum if num*multiplier == float(inum) else inum+1
        return ceil/multiplier

    def median(time_list):
        quotient, remainder = divmod(len(time_list), 2)
        if remainder:
            return sorted(time_list)[quotient]
        else:
            return sum(sorted(time_list)[quotient-1:quotient+1])/2

    aggregated_data = []
    logging.info("Started aggregating")
    for url, req_time_list in raw_data.items():
        aggregated_json = {}
        aggregated_json['url'] = url
        aggregated_json['count'] = len(req_time_list)
        aggregated_json['count_perc'] = round_up(len(req_time_list)/total_number_urls)
        aggregated_json['time_avg'] = round_up(sum(req_time_list)/len(req_time_list))
        aggregated_json['time_max'] = max(req_time_list)
        aggregated_json['time_med'] = round_up(median(req_time_list))
        aggregated_json['time_perc'] = round_up(sum(req_time_list)/sum_request_time)
        aggregated_json['time_sum'] = round_up(sum(req_time_list))
        aggregated_data.append(aggregated_json)

    return aggregated_data


def parse_log(line):
    """Parse logs with regex

    Function that parse raw logs and extracts request_time and url

    Args:
        line: Decoded line

    Returns:
        reqest_time_data: float number or None if log can't be parsed
        url_data: String or None if log can't be parsed
    """

    url_format = re.compile(r"""((?:(?<=PUT )|(?<=GET )|(?<=POST )|(?<=HEAD ))(.*)(?=\ http))""", re.IGNORECASE)
    request_time_format = re.compile(r"""(([0-9]*[.])?[0-9]+(?!.*\d))$""", re.IGNORECASE)
    url_data = re.search(url_format, line)
    request_time_data = re.search(request_time_format, line)
    if url_data:
        url_data = url_data.group()
    if request_time_data:
        request_time_data = request_time_data.group()
    return request_time_data, url_data


def decode_log(line):
    ''' Readking log from the file

    Function to decode the raw byte line

    Args:
        line - raw byte line

    Retunrs:
        Decoded line


    Things to do:
    1. Test the input and try to decode it
    2. Input can be as a byte string
    '''
    try:
        line = line.decode('utf-8')
    except UnicodeDecodeError:
        line = line.decode('ISO-8859-1')
        logging.debug("UnicodeDecodeError for line {}".format(line))
    return line


def read_log(read_log_file):
    """ Log manager

    Function read file by line, decode it and create dict
    with the following structure: {'url': list_of_req_time}

    Args:
        read_log_file: Log file path

    Returns:
        url_time_json: dictionary of the following structure:
                        {'url': list_of_req_time)}
        error_ratio: error_ratio for log parsing

    Todo:
        Create an iterator pipeline for decoding and parsing
    """
    unsuccessfull_log_parsing = 0
    total_log_parsing = 0
    url_time_json = defaultdict(list)
    for line in read_log_file:
        decoded_line = decode_log(line)
        request_time, url = parse_log(decoded_line)
        if request_time:
            logging.info("Successfully parsed request time")
        else:
            unsuccessfull_log_parsing += 1
            logging.debug("Error while parsring {0} with parameter req_time".format(line))
            continue
        if url:
            logging.info("Successfully parsed url")
        else:
            unsuccessfull_log_parsing += 1
            logging.debug("Error while parsring {0} with parameter url".format(line))
            continue
        url_time_json[url].append(float(request_time))
        total_log_parsing += 1
        error_rate = unsuccessfull_log_parsing/total_log_parsing
    return url_time_json, error_rate


def find_logs(log_dir):
    """ Find log in a speicfied directory

    Read nginix logs in gzip or plain format

    Args:
        log_dir: path to the log directory

    Returns:
        valid_log_files: list of valid log files

    Todo:
        rewrite using iterators
    """
    valid_log_files = []
    file_format = re.compile(r"""(nginx-access-ui.log-\d{8})($|(.+?)(gz|tgz))""")
    for log_file in os.listdir(log_dir):
        if re.search(file_format, log_file):
            logging.info("Find a valid log file - {}".format(log_file))
            valid_log_files.append(log_file)
    return valid_log_files


def open_log(log_file_path):
    """ Mapping files path with reading method

    Depending wether it is a plain file or gzip find a way to open the file

    Args:
        log_file_path: path to a log file

    Returns:
        Method for open file
    """
    gz_format = ('gz', 'tgz')
    open_method = gzip.open(log_file_path, 'rb') if log_file_path.endswith(gz_format) else open(log_file_path, 'rb')
    return open_method


def is_log_parsed(report_dir, log_file):
    """ Check if log already exist in report folder

    Agrs:
        report_dir: path to reports
        log_file: name of the log file

    Return:
        True if file exsit, otherwise False
    """

    file_format = re.compile(r"""\d{8}""")
    render_file_name = re.search(file_format, log_file).group()
    render_file = 'report-{}.html'.format(render_file_name)
    return True if render_file in os.listdir(os.path.join(ROOT_DIR, report_dir)) else False


def main(config):

    logging_filename = config['LOGGING_FILE'] if 'LOGGING_FILE' in config else None
    print(logging_filename)
    if logging_filename:
        logging.basicConfig(
                        level=logging.DEBUG,
                        format='%(asctime)s] %(levelname).1s %(message)s',
                        datefmt='%Y.%m.%d %H:%M:%S',
                        filename=logging_filename
                        )
    else:
        logging.basicConfig(
                        level=logging.DEBUG,
                        format='%(asctime)s] %(levelname).1s %(message)s',
                        datefmt='%Y.%m.%d %H:%M:%S',
                        stream=sys.stdout
                        )

    logs_to_parse = find_logs(os.path.join(ROOT_DIR, config['LOG_DIR']))

    if len(logs_to_parse) == 0:
        logging.error("Log dir is empty")
        raise Exception("Log dir is empty")

    for log_file in logs_to_parse:
        if is_log_parsed(config['REPORT_DIR'], log_file):
            logging.debug("Report for {} already exsit".format(log_file))
            continue
        log_file_path = os.path.join(ROOT_DIR, config['LOG_DIR'], log_file)
        open_method = open_log(log_file_path)

        with open_method as opened_log_file:
            url_time_json, error_ratio = read_log(opened_log_file)

        logging.info("Error threshold is {}".format(error_ratio))

        if error_ratio > parsing_threshold:
            continue

        aggregated_data = aggregate_log(url_time_json)
        logging.info("Log file data {} aggregated".format(log_file))

        file_format = re.compile(r"""\d{8}""")
        save_file_name = re.search(file_format, log_file).group()

        save_top_logs(aggregated_data, config['REPORT_SIZE'], config['REPORT_DIR'], save_file_name)
        render(aggregated_data, config['REPORT_DIR'],  save_file_name)
        logging.info("Log file {} was rendered". format(log_file))


def join_configs(external_config, default_config):
    """ Function that join default and external configs

    Check if external config keys are valid
    config = {
        "REPORT_SIZE": 1000,
        "REPORT_DIR": "./reports",
        "LOG_DIR": "./log",
    }
    and join config and external file

    Args:
        external_config: path to external config
        default_config: default global config

    Returns:
        external of default config file
    }"""
    merged_config = deepcopy(external_config)
    default_keys = ['REPORT_SIZE', 'REPORT_DIR', 'LOG_DIR']
    for key in default_keys:
        if key not in merged_config:
            merged_config[key] = default_config[key]

    try: 
        merged_config['REPORT_SIZE'] = int(merged_config['REPORT_SIZE'])
    except ValueError:
        merged_config['REPORT_SIZE'] = default_config['REPORT_SIZE']

    return merged_config


def read_config_file(external_config_file_path):
    """ Function to read config file

    Read external config file and return None if file not found

    Args:
        external_config_file_path: Path to config file

    Return:
        dict or None

    Todo:
        Parse several different config format
    """

    try:
        with open(external_config_file_path) as yaml_file:
            external_config_dict = yaml.load(yaml_file, Loader=yaml.BaseLoader)
        return external_config_dict

    except FileNotFoundError:
        return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Read cofing')
    parser.add_argument('-c', '--config', required=False,  help='Config file')
    args = vars(parser.parse_args())
    if args['config']:
        external_config = read_config_file(args['config'])
        config = join_configs(external_config, config)
    main(config)
