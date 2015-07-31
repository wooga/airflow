from datetime import datetime
import logging

from airflow.models import BaseOperator, TaskInstance
from airflow.utils import apply_defaults, State
from airflow import settings

__author__ = 'cedrikneumann'


class SubScheduleOperator(BaseOperator):
    """
    Executes a Python callable

    :param condition: A reference to an object that is callable with exactly one parameter
    :type condition: python callable
    """
    template_fields = tuple()
    ui_color = '#ffefeb'

    @apply_defaults
    def __init__(
            self,
            condition=lambda dt: True,
            *args, **kwargs):
        super(SubScheduleOperator, self).__init__(*args, **kwargs)
        self.condition = condition

    def execute(self, context):
        dt = context['execution_date']

        if not isinstance(dt, datetime):
            logging.error("execution_date=%s is not a datetime object" % dt)
            return False

        if self.condition(dt):
            logging.info("Condition satisfied")
            logging.info("Continue with downstream tasks")
            return

        logging.info("Condition not satisfied")
        logging.info("Marking other directly downstream tasks as skipped")
        session = settings.Session()
        for task in context['task'].downstream_list:
            ti = TaskInstance(
                task, execution_date=context['ti'].execution_date)
            ti.state = State.SKIPPED
            ti.start_date = datetime.now()
            ti.end_date = datetime.now()
            session.merge(ti)
        session.commit()
        session.close()
        logging.info("Done.")
