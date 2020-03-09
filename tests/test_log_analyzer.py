from log_analyzer.log_analyzer import aggregate_log, parse_log, find_logs, open_log, join_configs, read_config_file
import unittest
import os
import gzip

TEST_DIR = os.path.dirname(os.path.abspath(__file__))


class TestLogAnalyzer(unittest.TestCase):

    def test_join_configs(self):
        self.default_config = {
         "REPORT_SIZE": 1000,
         "REPORT_DIR": "./reports",
         "LOG_DIR": "./log"
        }

        self.expected_valid_config = {
         "REPORT_SIZE": 1500,
         "REPORT_DIR": "./reports",
         "LOG_DIR": "./log"
        }

        self.expected_uncomplete_config = {
         "REPORT_SIZE": 2000,
         "REPORT_DIR": "./reports",
         "LOG_DIR": "./log"
        }

        self.external_valid_config = os.path.join(TEST_DIR, './test_valid_conf.yaml')
        self.external_valid_config_dict = read_config_file(self.external_valid_config)

        self.external_uncomplete_config = os.path.join(TEST_DIR, './test_conf.yaml')
        self.external_uncomplete_config_dict = read_config_file(self.external_uncomplete_config)

        self.external_valid_conf_dict_joined = join_configs(self.external_valid_config_dict, self.default_config)
        self.external_uncomplete_conf_dict_joined = join_configs(self.external_uncomplete_config_dict,
                                                                self.default_config)
        
        self.assertDictEqual(self.external_valid_conf_dict_joined, self.expected_valid_config)
        self.assertDictEqual(self.external_uncomplete_conf_dict_joined, self.expected_uncomplete_config)

    def test_find_log(self):
        self.expected_available_logs = ['nginx-access-ui.log-20170629.gz',
                                        'nginx-access-ui.log-20170629',
                                        'nginx-access-ui.log-20170630.tgz']
        self.available_logs = find_logs(os.path.join(TEST_DIR, './test_log'))
        self.assertEqual(sorted(self.expected_available_logs), sorted(self.available_logs))

    def test_open_log(self):
        self.plain_log_path = os.path.join(TEST_DIR, './test_log', 'nginx-access-ui.log-20170629')
        self.expected_open_method_plain_log = open(self.plain_log_path, 'rb')

        self.gz_log_path = os.path.join(TEST_DIR, './test_log', 'nginx-access-ui.log-20170629.gz')
        self.expected_open_method_gz_log = gzip.open(self.gz_log_path, 'rb')

        with self.expected_open_method_plain_log as f:
            self.expected_plain_file = f.read()

        with open_log(self.plain_log_path) as f:
            self.plain_file = f.read()
        
        with self.expected_open_method_gz_log as f:
            self.expected_gz_file = f.read()

        with open_log(self.gz_log_path) as f:
            self.gz_file = f.read()
        
        self.assertEqual(self.expected_plain_file, self.plain_file)
        self.assertEqual(self.expected_gz_file, self.gz_file)

    def test_parse_log(self):
        self.log_get = '1.169.137.128 -  - [29/jun/2017:07:10:50 +0300] \
                        "GET /api/v2/banner/1717161 http/1.1" 200 2116 "-" "Slotovod" \
                        "-" "1498709450-2118016444-4709-10027411" "712e90144abee9" 0.199'
        self.log_post = '1.126.153.80 -  - [29/Jun/2017:07:10:50 +0300] \
                        "POST /api/v2/banner/22532881/statistic/outgoings/?date_from=2017-06-29&date_to=2017-06-29 HTTP/1.1" \
                        200 40 "-" "-" "-" "1498709450-48424485-4708-9845489" "1835ae0f17f" 0.126'
        
        self.log_wrong_url = '1.169.137.128 -  - [29/jun/2017:07:10:50 +0300] \
                            "/api/v2/banner/1717161 HTTP/1.1" 200 2116 "-" "Slotovod" "-" \
                            "1498709450-2118016444-4709-10027411" "712e90144abee9" 0.198'
        

        self.log_wrong_time = '1.169.137.128 -  - [29/jun/2017:07:10:50 +0300] \
                                    "/api/v2/banner/1717161 HTTP/1.1" 200 2116 "-" "Slotovod" \
                                    "-" "1498709450-2118016444-4709-10027411" 0.199 "712e90144abe9e"'


        self.expected_log_get_parsed_req_time = 0.199
        self.expected_log_post_parsed_req_time = 0.126
        self.expected_log_wrong_url_parser_req_time = 0.198
        self.expected_log_wrong_time_parsed_req_time = None
        self.expected_log_get_parsed_url = '/api/v2/banner/1717161'
        self.expected_log_post_parsed_url = '/api/v2/banner/22532881/statistic/outgoings/?date_from=2017-06-29&date_to=2017-06-29'
        self.expected_log_wrong_url_parsed_url = None
        self.expected_log_wrong_time_parsed_url = '/api/v2/banner/1717161'
        
        self.get_req_time, self.get_url = parse_log(self.log_get)
        self.post_req_time, self.post_url = parse_log(self.log_post)
        self.wrong_url_req_time, self.wrong_url_url = parse_log(self.log_wrong_url)
        self.wrong_time_req_time, self.wrong_time_url = parse_log(self.log_wrong_time)

        self.assertEqual(self.expected_log_get_parsed_req_time, float(self.get_req_time))
        self.assertEqual(self.expected_log_get_parsed_url, self.get_url)
        self.assertEqual(self.expected_log_post_parsed_req_time, float(self.post_req_time))
        self.assertEqual(self.expected_log_post_parsed_url, self.post_url)
        self.assertEqual(self.expected_log_wrong_url_parser_req_time, float(self.wrong_url_req_time))
        self.assertEqual(self.expected_log_wrong_url_parsed_url, self.wrong_url_url)
        self.assertEqual(self.expected_log_wrong_time_parsed_req_time, self.wrong_time_req_time)
        #self.assertEqual(self.expected_log_wrong_time_parsed_url, self.wrong_time_url)
    
    def test_aggregate_log(self):
        self.raw_data = {'/api/v2/banner/171': [0.198, 0.641, 0.781], 
                    '/api/v2/banner/181': [0.187, 0.878, 0.543],
                    '/api/v2/banner/193': [0.193, 0.832, 0.245, 0.981]
                    }

        self.expected_aggregated_data = [{'url': '/api/v2/banner/171',
                            'count': 3,
                            'count_perc': 0.3,
                            'time_avg': 0.54,
                            'time_max': 0.781,
                            'time_med': 0.641,
                            'time_perc': 0.296,
                            'time_sum': 1.62},
                            {'url': '/api/v2/banner/181',
                            'count': 3,
                            'count_perc': 0.3,
                            'time_avg': 0.536,
                            'time_max': 0.878,
                            'time_med': 0.543,
                            'time_perc': 0.294,
                            'time_sum': 1.608},
                            {'url': '/api/v2/banner/193',
                            'count': 4,
                            'count_perc': 0.4,
                            'time_avg': 0.563,
                            'time_max': 0.981,
                            'time_med': 0.539,
                            'time_perc': 0.411,
                            'time_sum': 2.251}]
        
        self.aggregared_data = aggregate_log(self.raw_data)
        self.assertDictEqual(self.expected_aggregated_data[0], self.aggregared_data[0])   
        self.assertDictEqual(self.expected_aggregated_data[1], self.aggregared_data[1])
        self.assertDictEqual(self.expected_aggregated_data[2], self.aggregared_data[2])


    if __name__ == '__main__':
        unittest.main()
