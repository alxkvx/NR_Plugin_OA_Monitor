import sys
import os
import requests
import socket
import json
import logging
import time

#--------------------------------------------------------------------
class Newrelic_Metrics(object):

    def __init__(self, proxies, nr_licence_key, agent_name, agent_version, guid,
                 poll_cycle, metrics, hostname):
        self.proxies = proxies
        self.nr_api_url = 'https://platform-api.newrelic.com/platform/v1/metrics'
        self.nr_licence_key = nr_licence_key
        self.agent_name = agent_name
        self.agent_version = agent_version
        self.guid = guid
        self.poll_cycle = poll_cycle
        self.metrics = metrics
        self.hostname = hostname

    def http_request(self, url, proxies, headers, data, timeout):
        """ HTTP request """
        try:
            logging.info("Sending HTTP POST request: %s " % (url))
            encoded_str = json.dumps(data)
            start_timer = time.time()
            print("encoded_str = {0}".format(encoded_str))
            if self.proxies:
                r = requests.post(url=url,
                                proxies=self.proxies,
                                headers=headers,
                                data=encoded_str,
                                timeout=timeout)
            else:
                r = requests.post(url=url, headers=headers, data=encoded_str, timeout=timeout)

            response_time = "%.2f" % (time.time() - start_timer)
            if r.status_code == requests.codes.ok:
                logging.info("HTML code: %s" % (r.status_code))
                return True, r.status_code, r.headers, response_time
            else:
                logging.info("\nAn error occurred, status code %s" % (r.status_code))
                return False, r.status_code, r.headers, response_time
        except requests.exceptions.RequestException, e:
            logging.info('\nERROR: Request exception occurred')
            logging.info(e)
            return False, None, None, None, None


    def newrelic_post_request(self):
        # disable url urllib3 warnings for older python versions
        requests.packages.urllib3.disable_warnings()

        """ Getting formatted payload """
        agent={}
        agent["host"]=self.hostname
        agent["version"]=self.agent_version
        components_load={}
        components_load["name"]=self.agent_name
        components_load["guid"]=self.guid
        components_load["duration"]=self.poll_cycle
        components_load["metrics"]=self.metrics
        components=[]
        components.append(components_load)
        payload={}
        payload["agent"]=agent
        payload["components"]=components

        for key, value in self.metrics.iteritems():
            logging.info("%s %s" % (key, value))

        #logging.info("Payload: \n%s" % payload)
        logging.info("Payload: \n%s" % payload)
        headers = {"Content-Type": "application/json",
                   "Accept": "application/json",
                   "X-License-Key": self.nr_licence_key}
        logging.info("headers: \n%s" % headers)
        (request_status, status_code, headers, response_time) = self.http_request(self.nr_api_url, self.proxies, headers, payload, 10)
        logging.info("HTTP request status:\n %s, \n HTTP code:%s, \n Headers: %s, \n Latency:%s\n\n" % (request_status,
                                                                                                                 status_code,
                                                                                                                 headers,
                                                                                                                 response_time))
    def insights_post_request(self, xinsertkey, account_id):
        requests.packages.urllib3.disable_warnings()
        url = 'https://insights-collector.newrelic.com/v1/accounts/{0}/events'.format(account_id)
        payload = self.metrics
        logging.info("Payload: \n%s" % payload)
        headers = {"Content-Type": "application/json",
                   "X-Insert-Key": xinsertkey}
        logging.info("headers: \n%s" % headers)
        (request_status, status_code, headers, response_time) = self.http_request(url, self.proxies, headers, payload, 10)
        logging.info("HTTP request status:\n %s, \n HTTP code:%s, \n Headers: %s, \n Latency:%s\n\n" % (request_status,
                                                                                                                 status_code,
                                                                                                                 headers,
                                                                                                                 response_time))


