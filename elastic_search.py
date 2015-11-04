"""Elastic search

Use singleton :data:`elastic` to access :class:`ElasticSearch` class

Use :meth:`load` for loading data from file
"""

import requests
import json
import os
import multiprocessing
import time

__all__ = ['elastic', 'load', 'ElasticError']

_E_PATH = os.path.join(os.path.expanduser('~'), 'Downloads',
                       'elasticsearch-2.0.0')


class _ElasticServer(object):
    def __init__(self, e_path):
        """Initialize Elastic Server

        Arguments:
            e_path (str): Path to the Elastic server
        """
        if not os.path.isdir(os.path.join(e_path, 'bin')):
            raise ElasticError('Path to Elastic server is not correct')
        if not os.path.isfile(os.path.join(e_path, 'bin', 'elasticsearch')):
            raise ElasticError('Path to Elastic server is not correct')

        self.e_path = e_path
        self.pr = multiprocessing.Process(
            target=os.system,
            args=('sh {} -d -p pid'.format(
                os.path.join('bin', 'elasticsearch')), ))

    def run(self):
        """Start Elastic Search server"""
        os.chdir(self.e_path)
        self.pr.start()
        time.sleep(10)

    def stop(self):
        os.chdir(self.e_path)
        os.system('kill `cat pid`')


class ElasticSearch(object):
    """Manage ElasticSearch

    Attributes:
        db (dict): Structure of actual DB
    """
    def __init__(self):
        """Initialize ElasticSearch"""
        # self.e = _ElasticServer(_E_PATH)
        # self.e.run()
        self.url = 'http://localhost:9200'
        self._db = {}

    def __del__(self):
        # self.e.stop()
        pass

    @property
    def db(self):
        return self._db

    def put(self, e_index, e_type, e_id, data):
        """Put one record into DB

        Arguments:
            e_index (str): Elastic index
            e_type (str): Elastic type
            e_id (int): Elastic ID
            data (dict): Record data

        Returns:
            requests.Response: Elastic's response
        """
        if e_index not in self._db.keys():
            self._db[e_index] = {}
        if e_type not in self._db[e_index].keys():
            self._db[e_index][e_type] = []
        if e_id not in self._db[e_index][e_type]:
            self._db[e_index][e_type].append(e_id)

        url = '{}/{}/'.format(
            self.url, '/'.join(map(str, [e_index, e_type, e_id])))
        return requests.put(url, json.dumps(data))

    def get(self, e_index, e_type, e_id):
        """Get one record from DB

        Arguments:
            e_index (str): Elastic index
            e_type (str): Elastic type
            e_id (int): Elastic ID

        Raises:
            ElasticError: When BD structure is not valid

        Returns:
            requests.Response: Elastic's response
        """
        if (e_index not in self._db.keys() or
                e_type not in self._db[e_index].keys() or
                e_id not in self._db[e_index][e_type]):
            raise ElasticError('DB structure is not valid')

        url = '{}/{}'.format(
            self.url, '/'.join(map(str, [e_index, e_type, e_id])))
        return requests.get(url)

    def simple_search(self, e_index, e_type, query):
        """Search in DB

        Arguments:
            e_index (str): Elastic index
            e_type (str): Elastic type
            query (str): Elastic query

        Raises:
            ElasticError: When BD structure is not valid

        Returns:
            requests.Response: Elastic's response
        """
        if (e_index not in self._db.keys() or
                e_type not in self._db[e_index].keys()):
            raise ElasticError('DB structure is not valid')

        url = '{}/{}/_search?q={}&pretty=true'.format(
            self.url, '/'.join(map(str, [e_index, e_type])), query)
        return requests.get(url)

    def put_records(self, e_index, e_type, records):
        """Put multiple records into DB

        Arguments:
            e_index (str): Elastic index
            e_type (str): Elastic type
            records (list): Records in dict format
        """
        if (e_index not in self._db.keys() or
                e_type not in self._db[e_index].keys()):
            base = 1
        else:
            base = max(self._db[e_index][e_type]) + 1

        for i in range(len(records)):
            self.put(e_index, e_type, base + i, records[i])


elastic = ElasticSearch()


def load(file_path):
    """Load data from file to database

    Arguments:
        file_path (str): Path to file with data
    """
    file_path = os.path.join(os.path.dirname(__file__), file_path)
    if not os.path.isfile(file_path):
        raise ElasticError('{} is not valid file'.format(file_path))

    with open(file_path, 'r') as ads_file:
        ads_json = json.load(ads_file)

    for e_index in ads_json.keys():
        for e_type in ads_json[e_index].keys():
            elastic.put_records(e_index, e_type, ads_json[e_index][e_type])


class ElasticError(Exception):
    """Exception for Elastic Search"""
    pass
