from airflow.hooks.jdbc_hook import JdbcHook


class ExasolHook(JdbcHook):
    """
    Interact with Exasol
    """

    def __init__(
            self, jdbc_url='jdbc:exa:{0}:{1};schema={2}',
                  jdbc_driver_name = 'com.exasol.jdbc.EXADriver',
                  jdbc_driver_loc='/var/exasol/exajdbc.jar', *args, **kwargs):
        super(ExasolHook, self).__init__(jdbc_url=jdbc_url, jdbc_driver_name=jdbc_driver_name,jdbc_driver_loc=jdbc_driver_loc, *args, **kwargs)