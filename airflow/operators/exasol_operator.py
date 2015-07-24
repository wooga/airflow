import logging
from datetime import timedelta

from airflow.hooks import ExasolHook
from airflow.models import BaseOperator
from airflow.utils import apply_defaults


class ExasolOperator(BaseOperator):
    """
    Executes sql code in a specific Exasol database

    :param exasol_conn_id: reference to a specific exasol database
    :type exasol_conn_id: string
    :param sql: the sql code to be executed
    :type sql: string or string pointing to a template file. File must have
        a '.sql' extensions.
    """

    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    '''
    TODO: add jdbc_driver_classname, jdbc_driver_path, jdbc_connection_url with defaults
    '''
    @apply_defaults
    def __init__(
            self, sql,
            exasol_conn_id='exasol_default', autocommit=True,
            retries=35, retry_delay=timedelta(seconds=300),
            *args, **kwargs):
        super(ExasolOperator, self).__init__(retries=retries, retry_delay=retry_delay, *args, **kwargs)

        self.sql = sql
        self.exasol_conn_id = exasol_conn_id
        self.autocommit = autocommit

    def execute(self, context):
        logging.info('Executing: ' + self.sql)
        self.hook = JdbcHook(conn_id=self.exasol_conn_id)
        for row in self.hook.get_records(self.sql, self.autocommit):
            logging.info('Result: ' + ','.join(map(str,row)) )