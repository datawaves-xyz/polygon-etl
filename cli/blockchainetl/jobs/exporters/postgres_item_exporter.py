# MIT License
#
# Copyright (c) 2020 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import collections
import logging

from sqlalchemy import create_engine

from blockchainetl.jobs.exporters.converters.composite_item_converter import CompositeItemConverter
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait


class PostgresItemExporter:

    def __init__(self, connection_url, item_type_to_insert_stmt_mapping, converters=(), print_sql=True):
        self.connection_url = connection_url
        self.item_type_to_insert_stmt_mapping = item_type_to_insert_stmt_mapping
        self.converter = CompositeItemConverter(converters)
        self.print_sql = print_sql

        self.engine = self.create_engine()
        self.logger = logging.getLogger('PostgresItemExporter')
        self.executor = ThreadPoolExecutor(max_workers=20)

    def open(self):
        pass

    def export_items(self, items):
        items_grouped_by_type = group_by_item_type(items)

        futures = []

        for item_type, insert_stmt in self.item_type_to_insert_stmt_mapping.items():
            item_group = items_grouped_by_type.get(item_type)
            if item_group:
                converted_items = list(self.convert_items(item_group))
                self.logger.info('{}: start exporting {} items'.format(item_type, len(converted_items)))

                def execute_handler(insert_items):
                    connection = self.engine.connect()
                    connection.execute(insert_stmt, insert_items)
                    self.logger.info('Chunk size {} exported'.format(len(insert_items)))

                chunk_size = 200
                list_chunked = [converted_items[i:i + chunk_size] for i in range(0, len(converted_items), chunk_size)]

                for i in range(0, len(list_chunked)):
                    futures.append(self.executor.submit(execute_handler, list_chunked[i]))

        wait(futures)

    def convert_items(self, items):
        for item in items:
            yield self.converter.convert_item(item)

    def create_engine(self):
        engine = create_engine(self.connection_url, echo=self.print_sql, pool_recycle=3600)
        return engine

    def close(self):
        self.executor.shutdown(True)


def group_by_item_type(items):
    result = collections.defaultdict(list)
    for item in items:
        result[item.get('type')].append(item)

    return result
