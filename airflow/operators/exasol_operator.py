__author__ = 'janomar'
from airflow.operators.jdbc_operator import JdbcOperator
from airflow.utils import apply_defaults


class ExasolOperator(JdbcOperator):
    """
    Alias for backwards compatibility
    """
    @apply_defaults
    def __init__(self, retries=35 ,exasol_conn_id='exasol_default', *args, **kwargs):
        super(ExasolOperator,self).__init__(retries=retries, jdbc_conn_id=exasol_conn_id, *args,**kwargs)

