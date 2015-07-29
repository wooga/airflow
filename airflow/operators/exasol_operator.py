__author__ = 'janomar'
from airflow.operators.jdbc_operator import JdbcOperator
from airflow.utils import apply_defaults
from datetime import timedelta


class ExasolOperator(JdbcOperator):
    """
    Alias for backwards compatibility
    """
    @apply_defaults
    def __init__(self, retries=35, retry_delay=timedelta(seconds=300), autocommit=True, exasol_conn_id='exasol_default', *args, **kwargs):
        super(ExasolOperator,self).__init__(retries=retries, retry_delay=retry_delay, autocommit=autocommit, jdbc_conn_id=exasol_conn_id, *args,**kwargs)
