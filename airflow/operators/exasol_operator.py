__author__ = 'janomar'
from airflow.operators.jdbc_operator import JdbcOperator
from airflow.utils import apply_defaults


class ExasolOperator(JdbcOperator):
    """
    Alias for backwards compatibility
    """
