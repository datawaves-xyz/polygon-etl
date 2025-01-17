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
import time

from sqlalchemy import create_engine

from blockchainetl.jobs.exporters.converters.composite_item_converter import CompositeItemConverter
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait


class PostgresItemExporter:

    def __init__(self, connection_url, item_type_to_insert_stmt_mapping, converters=(), print_sql=True,
                 chunk_size=200, max_workers=20):
        self.connection_url = connection_url
        self.item_type_to_insert_stmt_mapping = item_type_to_insert_stmt_mapping
        self.converter = CompositeItemConverter(converters)
        self.print_sql = print_sql

        self.engine = self.create_engine()
        self.logger = logging.getLogger('PostgresItemExporter')
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.chunk_size = chunk_size

    def open(self):
        pass

    def export_items(self, items):
        items_grouped_by_type = group_by_item_type(items)

        for item_type, insert_stmt in self.item_type_to_insert_stmt_mapping.items():
            item_group = items_grouped_by_type.get(item_type)
            if item_group:
                converted_items = list(self.convert_items(item_group))

                self.logger.info('{}: start exporting {} items'.format(item_type, len(converted_items)))

                start = time.time()

                def execute_handler(chunked_items):
                    connection = self.engine.connect()
                    connection.execute(insert_stmt, chunked_items)

                futures = []
                for i in range(0, len(converted_items), self.chunk_size):
                    futures.append(
                        self.executor.submit(execute_handler, converted_items[i:i + self.chunk_size]))

                wait(futures)

                elapsed_time = time.time() - start
                self.logger.info('{}: {} items exported, at {}/s'.format(item_type, len(converted_items),
                                                                         int(len(converted_items) / elapsed_time)))

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
