# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from builtins import str
from past.builtins import basestring
import os
import sys
import pyexasol
from contextlib import closing

from airflow.hooks.dbapi_hook import DbApiHook


class ExasolHook(DbApiHook):
    """
    Interact with Exasol.
    You can specify ssl parameters in the extra field of your connection
    as ``{"sslmode": "require", "sslcert": "/path/to/cert.pem", etc}``.

    Note: For Redshift, use keepalives_idle in the extra connection parameters
    and set it to less than 300 seconds.
    """
    conn_name_attr = 'exasol_conn_id'
    default_conn_name = 'exasol_default'
    supports_autocommit = True

    def __init__(self, *args, **kwargs):
        super(ExasolHook, self).__init__(*args, **kwargs)
        self.schema = kwargs.pop("schema", None)

    def get_conn(self):
        conn = self.get_connection(self.exasol_conn_id)
        conn_args = dict(
            dsn='%s:%s'%(conn.host,conn.port),
            user=conn.login,
            password=conn.password,
            schema=self.schema or conn.schema)
        # check for ssl parameters in conn.extra
        for arg_name, arg_val in conn.extra_dejson.items():
            if arg_name in ['compression', 'encryption','json_lib','client_name']:
                conn_args[arg_name] = arg_val

        conn = pyexasol.connect(**conn_args)
        return conn

    def get_pandas_df(self, sql, parameters=None):
        with closing(self.get_conn()) as conn:
            conn.export_to_pandas(sql, query_params=parameters)


    def get_records(self, sql, parameters=None):
        """
        Executes the sql and returns a set of records.

        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :type sql: str or list
        :param parameters: The parameters to render the SQL query with.
        :type parameters: mapping or iterable
        """
        if sys.version_info[0] < 3:
            sql = sql.encode('utf-8')

        with closing(self.get_conn()) as conn:
            with closing(conn.execute(sql, parameters)) as cur:
                return cur.fetchall()

    def get_first(self, sql, parameters=None):
        """
        Executes the sql and returns the first resulting row.

        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :type sql: str or list
        :param parameters: The parameters to render the SQL query with.
        :type parameters: mapping or iterable
        """
        if sys.version_info[0] < 3:
            sql = sql.encode('utf-8')

        with closing(self.get_conn()) as conn:
            with closing(conn.execute(sql,parameters)) as cur:
                return cur.fetchone()

    def run(self, sql, autocommit=False, parameters=None):
        """
        Runs a command or a list of commands. Pass a list of sql
        statements to the sql parameter to get them to execute
        sequentially

        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :type sql: str or list
        :param autocommit: What to set the connection's autocommit setting to
            before executing the query.
        :type autocommit: bool
        :param parameters: The parameters to render the SQL query with.
        :type parameters: mapping or iterable
        """
        if isinstance(sql, basestring):
            sql = [sql]

        with closing(self.get_conn()) as conn:
            if self.supports_autocommit:
                self.set_autocommit(conn, autocommit)

            for s in sql:
                if sys.version_info[0] < 3:
                    s = s.encode('utf-8')
                self.log.info(s)
                with closing(conn.execute(s, parameters)) as cur:
                    self.log.info(cur.row_count)
            # If autocommit was set to False for db that supports autocommit,
            # or if db does not supports autocommit, we do a manual commit.
            if not self.get_autocommit(conn):
                conn.commit()

    def set_autocommit(self, conn, autocommit):
        """
        Sets the autocommit flag on the connection
        """
        if not self.supports_autocommit and autocommit:
            self.log.warn(
                ("%s connection doesn't support "
                 "autocommit but autocommit activated."),
                getattr(self, self.conn_name_attr))
        conn.set_autocommit(autocommit)

    def get_autocommit(self, conn):
        """
        Get autocommit setting for the provided connection.
        Return True if autocommit is set.
        Return False if autocommit is not set or set to False or conn
        does not support autocommit.
        :param conn: Connection to get autocommit setting from.
        :type conn: connection object.
        :return: connection autocommit setting.
        :rtype bool.
        """
        autocommit = conn.attr.get('autocommit')
        if autocommit is None:
            autocommit = super(ExasolHook, self).get_autocommit(conn)
        return autocommit

    @staticmethod
    def _serialize_cell(cell, conn):
        """
        Postgresql will adapt all arguments to the execute() method internally,
        hence we return cell without any conversion.

        See http://initd.org/psycopg/docs/advanced.html#adapting-new-types for
        more information.

        :param cell: The cell to insert into the table
        :type cell: object
        :param conn: The database connection
        :type conn: connection object
        :return: The cell
        :rtype: object
        """
        return cell
