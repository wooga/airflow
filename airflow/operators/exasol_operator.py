__author__ = 'janomar'
from airflow.operators.jdbc_operator import JdbcOperator
from airflow.hooks.jdbc_hook import JdbcHook
from airflow.utils import apply_defaults

import logging

class ExasolOperator(JdbcOperator):
    """
    Alias for backwards compatibility
    """
    @apply_defaults
    def __init__(self, retries=35 ,exasol_conn_id='exasol_default', *args, **kwargs):
        super(ExasolOperator,self).__init__(retries=retries, jdbc_conn_id=exasol_conn_id, *args,**kwargs)

     # TODO find out if why we use get_records instead of run here, it's a really bad idea
     # get_records should be side effect free, right now that's not the case... it shouldn't require any kind of commit
     # fix autocommit somehow, if we have to use get_records - try to avoid exasol specific hook if possible..
    def execute(self, context):
        logging.info('Executing: ' + self.sql)
        self.hook = JdbcHook(jdbc_conn_id=self.jdbc_conn_id)
        for row in self.hook.get_records(self.sql, self.autocommit):
            logging.info('Result: ' + ','.join(map(str,row)) )