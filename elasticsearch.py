# collectd-elasticsearch
# ========================
#
# Python-based plugin to get simple ES stats per node
# and put it into collectd
__author__ = 'Bas van Veen, Danny van der Meulen'

import socket
import urllib2 as urllib
import json
import collectd
import collections


class EsMonitor(object):

    def __init__(self):
        self.plugin_name = 'elasticsearch'
        self.elasticsearch_host = socket.gethostname()
        self.elasticsearch_port = 9200
        self.elasticsearch_sections = ['indices', 'jvm', 'process']
        self.verbose_logging = False

    def log_verbose(self, msg):
        if not self.verbose_logging:
            return
        collectd.info('elasticsearch plugin [verbose]: %s' % msg)

    def configure_callback(self, conf):
        """Receive configuration block"""
        for node in conf.children:
            if node.key == 'ElasticsearchHost':
                self.elasticsearch_host = node.values[0]
            elif node.key == 'ElasticsearchPort':
                self.elasticsearch_port = int(node.values[0])
            elif node.key == 'ElasticsearchSections':
                self.elasticsearch_sections = list(node.values)
            elif node.key == 'Verbose':
                self.verbose_logging = bool(node.values[0])
            else:
                collectd.warning('elasticsearch plugin: Unknown config key: %s.' % node.key)
        self.url = 'http://{}:{}/_nodes/_local/stats/{}'.format(self.elasticsearch_host, self.elasticsearch_port, ','.join(self.elasticsearch_sections))

    def dispatch_value(self, plugin_instance, value_type, instance, value):
        """Dispatch a value to collectd"""
        if value < 0:
            self.log_verbose('Value %s=%s is negative, skipping' % (instance, value))
            return
        if self.verbose_logging is True:
            self.log_verbose('Sending value: %s.%s.%s=%s' % (self.plugin_name, plugin_instance, instance, value))
        val = collectd.Values()
        val.plugin = self.plugin_name
        val.plugin_instance = plugin_instance
        val.type = value_type
        val.type_instance = instance
        val.values = [value, ]
        val.dispatch()

    def flatten(self, d, parent_key='', sep='.'):
        """Flatten dicts when nested"""
        items = []
        for k, v in d.items():
            new_key = parent_key + sep + k if parent_key else k
            if isinstance(v, collections.MutableMapping):
                items.extend(self.flatten(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))

        return dict(items)

    def read_callback(self):
        """Collectd read callback"""
        self.log_verbose('Read callback called')

        result = json.load(urllib.urlopen(self.url))

        cluster_name = result['cluster_name']

        for node, node_config in result['nodes'].iteritems():
            for section in self.elasticsearch_sections:
                for key, value in node_config[section].iteritems():
                    if not isinstance(value, int):
                        flat_value = self.flatten(value)
                        for k, v in flat_value.iteritems():
                            instance = '{}.{}.{}'.format(cluster_name, section, key)
                            if isinstance(v, int):
                                """ We still have to change this to gauge if appropiate """
                                self.dispatch_value(instance, 'counter', k, v)
                    else:
                        instance = '{}.{}'.format(cluster_name, section)
                        if isinstance(value, int):
                            self.dispatch_value(instance, 'counter', key, value)


es = EsMonitor()
collectd.register_config(es.configure_callback)
collectd.register_read(es.read_callback)
