"""Clocked schedule Implementation."""

from celery import schedules
from celery.utils.time import maybe_make_aware
from .time_utils import NEVER_CHECK_TIMEOUT


class clocked(schedules.BaseSchedule):
    """
    A schedule that runs at a specific clocked time.
    Used for one-off tasks that need to run at a specific time.

    Arguments:
        clocked_time (datetime): The time at which the task should run.
        nowfun (callable): Function returning the current time (default: utcnow).
        app (Celery): The Celery application (optional).
    Returns:
        clocked: An instance of the clocked schedule.
    Raises:
        ValueError: If clocked_time is not a valid datetime.
    """

    def __init__(self, clocked_time, nowfun=None, app=None):
        """Initialize clocked."""
        self.clocked_time = maybe_make_aware(clocked_time)
        super().__init__(nowfun=nowfun, app=app)

    def remaining_estimate(self, last_run_at):
        """Return the time remaining until the scheduled time."""
        return self.clocked_time - self.now()

    def is_due(self, last_run_at):
        """Return whether the task is due to run and the next time to check."""
        rem_delta = self.remaining_estimate(None)
        remaining_s = max(rem_delta.total_seconds(), 0)
        if remaining_s == 0:
            return schedules.schedstate(is_due=True, next=NEVER_CHECK_TIMEOUT)
        return schedules.schedstate(is_due=False, next=remaining_s)

    def __repr__(self):
        """
        Return a string representation of the clocked schedule.
        Returns:
            str: String representation of the clocked schedule.
        """
        return f'<clocked: {self.clocked_time}>'

    def __eq__(self, other):
        """
        Check equality between two clocked schedules.
        """
        if isinstance(other, clocked):
            return self.clocked_time == other.clocked_time
        return False

    def __ne__(self, other):
        """
        Check inequality between two clocked schedules.
        """
        return not self.__eq__(other)

    def __reduce__(self):
        """
        Helper for pickle.
        """
        return self.__class__, (self.clocked_time, self.nowfun)
