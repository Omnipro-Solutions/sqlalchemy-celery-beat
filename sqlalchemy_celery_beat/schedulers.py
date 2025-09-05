# coding=utf-8

import datetime as dt
import logging
import math
import os
from multiprocessing.util import Finalize

import sqlalchemy as sa
from celery import current_app, schedules
from celery.beat import ScheduleEntry, Scheduler
from celery.utils.log import get_logger
from celery.utils.time import maybe_make_aware
from kombu.utils.encoding import safe_repr, safe_str
from kombu.utils.json import dumps, loads
from omni.pro.user.access import INTERNAL_USER

from .clockedschedule import clocked
from .models import ClockedSchedule, CrontabSchedule, IntervalSchedule, PeriodicTask, PeriodicTaskChanged, SolarSchedule
from .session import SessionManager, session_cleanup
from .time_utils import NEVER_CHECK_TIMEOUT

# This scheduler must wake up more frequently than the
# regular of 5 minutes because it needs to take external
# changes to the schedule into account.
DEFAULT_MAX_INTERVAL = 5  # seconds

DEFAULT_BEAT_DBURI = "sqlite:///schedule.db"

DEFAULT_BEAT_SCHEMA = None

DEFAULT_BEAT_ENGINE_OPTIONS = {}

ADD_ENTRY_ERROR = """\
Cannot add entry %r to database schedule: %r. Contents: %r
"""


session_manager = SessionManager()


logger = get_logger("sqlalchemy_celery_beat.schedulers")


class ModelEntry(ScheduleEntry):
    """
    Scheduler entry taken from database row.
    Analogous to celery.beat.ScheduleEntry
    (which is taken from a dict entry).
    See celery.beat.ScheduleEntry for more information.
        1. It is instantiated by passing a PeriodicTask model instance.
        2. It reads the schedule from the related model (e.g. CrontabSchedule).
        3. It is responsible for updating the model (last_run_at, total_run_count).
        4. It is instantiated by the Scheduler.
        5. It is used by the Scheduler to determine when the task is due next.
        6. It is used by the Scheduler to create the task to be sent to the worker.
        7. It is used by the Scheduler to save changes to the model.
        8. It is used by the Scheduler to create new entries from a dict.
        9. It is used by the Scheduler to convert schedules to model instances.
    """

    model_schedules = (
        # (schedule_type, model_type)
        (schedules.crontab, CrontabSchedule),
        (schedules.schedule, IntervalSchedule),
        (schedules.solar, SolarSchedule),
        (clocked, ClockedSchedule),
    )
    save_fields = ["last_run_at", "total_run_count", "no_changes"]

    def __init__(self, model, Session, app=None, **kw):
        """
        Initialize the model entry.
        Args:
            model (PeriodicTask): The PeriodicTask model instance.
            Session (sessionmaker): The SQLAlchemy session factory.
            app (Celery): The Celery application instance.
        Kwargs:
            Additional keyword arguments (not used).
        Raises:
            ValueError: If the schedule cannot be determined from the model.
        """
        self.app = app or current_app._get_current_object()
        self.Session = Session
        self.name = model.name
        self.task = model.task
        try:
            self.schedule = model.schedule
            logger.debug("schedule: {}".format(self.schedule))
        except Exception:
            logger.error(
                "Disabling schedule %s that was removed from database",
                self.name,
            )
            self._disable(model)

        try:
            self.args = loads(model.args or "[]")
            self.kwargs = loads(model.kwargs or "{}")
        except ValueError as exc:
            logger.exception(
                "Removing schedule %s for argument deseralization error: %r",
                self.name,
                exc,
            )
            self._disable(model)

        self.options = {}
        for option in ["queue", "exchange", "routing_key", "priority"]:
            value = getattr(model, option)
            if value is None:
                continue
            self.options[option] = value

        expires = getattr(model, "expires_", None)
        if expires:
            if isinstance(expires, dt.datetime):
                self.options["expires"] = maybe_make_aware(expires).astimezone(self.app.timezone)
            else:
                self.options["expires"] = expires

        self.options["headers"] = loads(model.headers or "{}")
        self.options["periodic_task_name"] = model.name

        self.total_run_count = model.total_run_count
        self.model = model

        if not model.last_run_at:
            model.last_run_at = self._default_now()
            # if last_run_at is not set and
            # model.start_time last_run_at should be in way past.
            # This will trigger the job to run at start_time
            # and avoid the heap block.
            if self.model.start_time:
                model.last_run_at = model.last_run_at - dt.timedelta(days=365 * 30)

        self.last_run_at = model.last_run_at

        # Because the last_run_at read from the database may not have time zone information,
        # so time zone information must be added here
        self.last_run_at = maybe_make_aware(self.last_run_at).astimezone(self.app.timezone)

    def _disable(self, model):
        """
        Disable the periodic task.
        This is called when there is an error with the schedule or arguments.
        """
        model.no_changes = True
        self.model.enabled = self.enabled = model.enabled = False
        session = self.Session()
        with session_cleanup(session):
            session.add(model)
            session.commit()
            # session.refresh(model)

    def is_due(self):
        """
        Return a tuple of (due: bool, next_time_to_check: float).
        See celery.beat.ScheduleEntry.is_due for more information.
        """
        if not self.model.enabled:
            # 5 second delay for re-enable.
            return schedules.schedstate(False, 5.0)

        # START DATE: only run after the `start_time`, if one exists.
        if self.model.start_time is not None:
            now = maybe_make_aware(self._default_now())
            start_time = maybe_make_aware(self.model.start_time)
            if now < start_time:
                # The datetime is before the start date - don't run.
                delay = math.ceil((start_time - now).total_seconds())
                return schedules.schedstate(False, delay)

        # ONE OFF TASK: Disable one off tasks after they've ran once
        if self.model.one_off and self.model.enabled and self.model.total_run_count > 0:
            self.model.enabled = False  # disable
            self.model.total_run_count = 0  # Reset
            self.model.no_changes = False  # Mark the model entry as changed
            save_fields = ("enabled",)  # the additional fields to save
            self.save(save_fields)

            return schedules.schedstate(False, NEVER_CHECK_TIMEOUT)  # Don't recheck

        # tz = self.app.timezone
        # last_run_at_in_tz = maybe_make_aware(self.last_run_at).astimezone(tz)
        # return self.schedule.is_due(last_run_at_in_tz)
        return self.schedule.is_due(self.last_run_at)

    def _default_now(self):
        """
        Get the current time with timezone information.
        Returns:
            datetime: The current UTC time with timezone information.
        """
        now = maybe_make_aware(dt.datetime.utcnow())
        return now

    def __next__(self):
        """
        Update the last run time and total run count.
        Returns:
            ModelEntry: A new instance of ModelEntry with updated values.
        """
        self.model.last_run_at = self._default_now()
        self.model.total_run_count += 1
        self.model.no_changes = True
        return self.__class__(self.model, Session=self.Session)

    next = __next__  # for 2to3

    def save(self, fields=tuple()):
        """
        Save the current model state to the database.
        Args:
            fields (tuple): Additional fields to save.
        1. It will only save the fields in self.save_fields and the fields passed in.
        2. It will acquire a row-level lock on the PeriodicTask row to prevent
           concurrent updates.
        3. If the row has been deleted, it will log a warning and not raise an error.
        4. It will commit the transaction after updating the fields.
        5. It will use a new session for the operation.
        6. It will handle session cleanup using the session_cleanup context manager.
        """
        session = self.Session()
        with session_cleanup(session):
            # Object may not be synchronized, so only
            # change the fields we care about.
            stmt = sa.select(PeriodicTask).filter_by(id=self.model.id).with_for_update().limit(1)
            obj = session.scalar(stmt)
            if obj:  # make sure object was not deleted
                for field in self.save_fields:
                    setattr(obj, field, getattr(self.model, field))
                for field in fields:
                    setattr(obj, field, getattr(self.model, field))
                session.add(obj)
                session.commit()
            else:
                logger.warning("couldn't update model %s, assuming it was deleted.", self.model.name)

    @classmethod
    def to_model_schedule(cls, session, schedule):
        """
        Convert a Celery schedule to a corresponding model instance.
        Args:
            session (Session): The SQLAlchemy session to use.
            schedule (schedules.schedule): The Celery schedule to convert.
        Returns:
            ScheduleModel: The corresponding model instance.
        Raises:
            ValueError: If the schedule type is not supported.
        """
        for schedule_type, model_class in cls.model_schedules:
            # change to schedule
            schedule = schedules.maybe_schedule(schedule)
            if isinstance(schedule, schedule_type):
                model_schedule = model_class.from_schedule(session, schedule)
                return model_schedule
        raise ValueError("Cannot convert schedule type {0!r} to model".format(schedule))

    @classmethod
    def from_entry(cls, name, Session, app=None, **entry):
        """
        Create or update a ModelEntry from a dict entry.
        Args:
            name (str): The name of the periodic task.
            Session (sessionmaker): The SQLAlchemy session factory.
            app (Celery): The Celery application instance.
        Kwargs:
            Additional fields for the periodic task.
        Returns:
            ModelEntry: The created or updated ModelEntry instance.
        Raises:
            ValueError: If there is an error creating or updating the entry.
            KeyError: If a required field is missing from the entry.
            TypeError: If the entry is not a valid dict.

        **entry sample:

            {'task': 'celery.backend_cleanup',
             'schedule': schedules.crontab('0', '4', '*'),
             'options': {'expires': 43200}}

        """
        session = Session()
        with session_cleanup(session):
            periodic_task = session.query(PeriodicTask).filter_by(name=name).first()
            temp = cls._unpack_fields(session, **entry)
            if not periodic_task:
                periodic_task = PeriodicTask(name=name, **temp)
            else:
                periodic_task.update_mixin(**temp)
            session.add(periodic_task)
            session.commit()
            res = cls(periodic_task, app=app, Session=Session)
            return res

    @classmethod
    def _unpack_fields(cls, session, schedule, args=None, kwargs=None, relative=None, options=None, **entry):
        """
        Unpack the fields from the entry dict into the appropriate model fields.
        Args:
            session (Session): The SQLAlchemy session to use.
            schedule (schedules.schedule): The Celery schedule to convert.
            args (list): The positional arguments for the task.
            kwargs (dict): The keyword arguments for the task.
            relative (bool): Whether the schedule is relative.
            options (dict): Additional options for the task.
            entry (dict): Additional fields for the periodic task.
        Returns:
            dict: The unpacked fields for the periodic task.
        Raises:
            ValueError: If there is an error unpacking the fields.
            KeyError: If a required field is missing from the entry.
            TypeError: If the entry is not a valid dict.

        **entry sample:

            {'task': 'celery.backend_cleanup',
             'schedule': <crontab: 0 4 * * * (m/h/d/dM/MY)>,
             'options': {'expires': 43200}}

        """
        model_schedule = cls.to_model_schedule(session, schedule)
        entry.update(
            # the model_id which to relationship
            # {model_field + '_id': model_schedule.id},
            {"schedule_model": model_schedule},
            args=dumps(args or []),
            kwargs=dumps(kwargs or {}),
            **cls._unpack_options(**options or {})
        )
        return entry

    @classmethod
    def _unpack_options(
        cls,
        queue=None,
        exchange=None,
        routing_key=None,
        priority=None,
        headers=None,
        one_off=False,
        expire_seconds=None,
        expires=None,
        start_time=None,
    ):
        """
        Unpack the options into the appropriate model fields.
        Args:
            queue (str): The name of the queue to use.
            exchange (str): The name of the exchange to use.
            routing_key (str): The routing key to use.
            priority (int): The priority of the task.
            headers (dict): The headers to include with the task.
            one_off (bool): Whether the task is a one-off task.
            expire_seconds (int): The number of seconds until the task expires.
            expires (datetime): The expiration time for the task.
            start_time (datetime): The start time for the task.
        Returns:
            dict: The unpacked options for the periodic task.
        """
        data = {
            "queue": queue,
            "exchange": exchange,
            "routing_key": routing_key,
            "priority": priority,
            "headers": dumps(headers or {}),
            "one_off": one_off,
            "start_time": start_time,
            "expire_seconds": expire_seconds,
        }
        if expires:
            if isinstance(expires, int):
                data["expire_seconds"] = expires
            elif isinstance(expires, dt.timedelta):
                data["expires"] = dt.datetime.utcnow() + expires
        return data

    def __repr__(self):
        """
        Return a string representation of the ModelEntry instance.
        """
        return "<ModelEntry: {0} {1}(*{2}, **{3}) {4}>".format(
            safe_str(self.name),
            self.task,
            safe_repr(self.args),
            safe_repr(self.kwargs),
            self.schedule,
        )


class DatabaseScheduler(Scheduler):
    """
    Database-backed scheduler for Celery.
    """

    Entry = ModelEntry
    Model = PeriodicTask
    Changes = PeriodicTaskChanged

    _schedule = None
    _last_timestamp = None
    _initial_read = True
    _heap_invalidated = False

    def __init__(self, *args, **kwargs):
        """
        Initialize the database scheduler.
        Args:
            args: Positional arguments (not used).
            kwargs: Keyword arguments.
        Kwargs:
            app (Celery): The Celery application instance.
            dburi (str): The database URI.
            schema (str): The database schema.
            engine_options (dict): Additional options for the SQLAlchemy engine.
            max_interval (int): The maximum interval between scheduler ticks.
        1. It sets up the database connection using the provided dburi, schema, and
           engine_options.
        2. It creates the necessary database tables if they do not exist.
        3. It initializes the parent Scheduler class.
        4. It sets up a Finalize object to ensure the sync method is called on exit.
        5. It sets the max_interval for the scheduler ticks.
        6. It initializes internal state variables for tracking schedule changes.
        Raises:
            KeyError: If the 'app' keyword argument is not provided.
        """
        self.app = kwargs["app"]
        self.dburi = kwargs.get("dburi") or self.app.conf.get("beat_dburi") or DEFAULT_BEAT_DBURI
        self.schema = kwargs.get("schema") or self.app.conf.get("beat_schema") or DEFAULT_BEAT_SCHEMA
        self.engine_options = (
            kwargs.get("engine_options") or self.app.conf.get("beat_engine_options") or DEFAULT_BEAT_ENGINE_OPTIONS
        )
        self.engine, self.Session = session_manager.create_session(
            self.dburi, schema=self.schema, **self.engine_options
        )
        session_manager.prepare_models(self.engine, schema=self.schema)

        self._dirty = set()
        Scheduler.__init__(self, *args, **kwargs)
        self._finalize = Finalize(self, self.sync, exitpriority=5)
        self.max_interval = kwargs.get("max_interval") or self.app.conf.beat_max_loop_interval or DEFAULT_MAX_INTERVAL

    def setup_schedule(self):
        """
        override method in parent class.
        It will be called in parent class __init__.
        1. It installs default entries into the schedule.
        2. It updates the schedule from the app configuration.
        3. It logs the setup process.
        4. It uses the install_default_entries and update_from_dict methods.
        5. It does not return any value.
        6. It is called only once during the initialization of the scheduler.
        """
        logger.info("setup_schedule")
        self.install_default_entries(self.schedule)
        self.update_from_dict(self.app.conf.beat_schedule)

    def all_as_schedule(self):
        """
        Read the schedule from the database.
        Returns:
            dict: A dictionary of schedule entries.
        1. It queries the database for all enabled PeriodicTask entries.
        2. It creates a ModelEntry for each enabled PeriodicTask.   
        3. It handles ValueError exceptions that may occur during ModelEntry creation.
        4. It uses a new session for the operation.
        5. It handles session cleanup using the session_cleanup context manager.
        """
        session = self.Session()
        with session_cleanup(session):
            logger.debug("DatabaseScheduler: Fetching database schedule")
            # get all enabled PeriodicTask
            models = session.query(self.Model).filter_by(enabled=True).all()
            s = {}
            for model in models:
                try:
                    s[model.name] = self.Entry(model, app=self.app, Session=self.Session)
                except ValueError:
                    pass
            return s

    def schedule_changed(self):
        """
        Check if the schedule has changed.
        """
        session = self.Session()
        with session_cleanup(session):
            changes = session.query(self.Changes).get(1)
            if not changes:
                changes = self.Changes(id=1)
                session.add(changes)
                session.commit()
                return False

            last, ts = self._last_timestamp, changes.last_update
            try:
                if ts and ts > (last if last else ts):
                    return True
            finally:
                self._last_timestamp = ts
            return False

    def reserve(self, entry):
        """override

        It will be called in parent class.
        """
        new_entry = next(entry)
        # Need to store entry by name, because the entry may change
        # in the mean time.
        self._dirty.add(new_entry.name)
        return new_entry

    def sync(self):
        """
        override method in parent class.
        It will be called in parent class __init__.
        Write all dirty entries to the database.
        1. It iterates over the dirty entries and saves them to the database.
        2. It handles KeyError exceptions that may occur during saving.
        3. It handles DatabaseError and InterfaceError exceptions that may occur during the operation.
        """
        logger.debug("Writing entries...")
        _tried = set()
        _failed = set()
        try:
            while self._dirty:
                name = self._dirty.pop()
                try:
                    self.schedule[name].save()  # save to database
                    logger.debug("{name} save to database".format(name=name))
                    _tried.add(name)
                except KeyError as exc:
                    logger.error(exc)
                    _failed.add(name)
        except sa.exc.DatabaseError as exc:
            logger.exception("Database error while sync: %r", exc)
        except sa.exc.InterfaceError as exc:
            logger.warning("DatabaseScheduler: InterfaceError in sync(), " "waiting to retry in next call...")
        finally:
            # retry later, only for the failed ones
            self._dirty |= _failed

    def update_from_dict(self, dict_):
        """
        Update the schedule from a dictionary.
        Args:
            dict_ (dict): A dictionary of schedule entries.
        """
        s = {}
        for name, entry_fields in dict_.items():
            # {'task': 'celery.backend_cleanup',
            #  'schedule': schedules.crontab('0', '4', '*'),
            #  'options': {'expires': 43200}}
            try:
                entry = self.Entry.from_entry(name, Session=self.Session, app=self.app, **entry_fields)
                if entry.model.enabled:
                    s[name] = entry
            except Exception as exc:
                logger.error(ADD_ENTRY_ERROR, name, exc, entry_fields)

        # update self.schedule
        self.schedule.update(s)

    def install_default_entries(self, data):
        """
        Install default schedule entries.
        """
        entries = {}
        if self.app.conf.result_expires:
            entries.setdefault(
                "celery.backend_cleanup",
                {
                    "task": "celery.backend_cleanup",
                    "schedule": schedules.crontab("0", "4", "*"),
                    "options": {"expire_seconds": 12 * 3600},
                    "tenant": os.getenv("TENANT"),
                    "updated_by": INTERNAL_USER,
                },
            )
        self.update_from_dict(entries)

    def schedules_equal(self, *args, **kwargs):
        """
        override method in parent class.
        It will be called in parent class.
        """
        if self._heap_invalidated:
            self._heap_invalidated = False
            return False
        return super(DatabaseScheduler, self).schedules_equal(*args, **kwargs)

    @property
    def schedule(self):
        """
        override method in parent class.
        It will be called in parent class.
        1. It checks if the schedule has changed in the database.
        2. If the schedule has changed, it reads the schedule from the database
           and updates the internal schedule.
        """
        initial = update = False
        if self._initial_read:
            logger.debug("DatabaseScheduler: initial read")
            initial = update = True
            self._initial_read = False
        elif self.schedule_changed():
            # when you updated the `PeriodicTasks` model's `last_update` field
            logger.info("DatabaseScheduler: Schedule changed.")
            update = True

        if update:
            self.sync()
            self._schedule = self.all_as_schedule()
            # the schedule changed, invalidate the heap in Scheduler.tick
            if not initial:
                self._heap = []
                self._heap_invalidated = True
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "Current schedule:\n%s",
                    "\n".join(repr(entry) for entry in self._schedule.values()),
                )
        # logger.debug(self._schedule)
        return self._schedule

    @property
    def info(self):
        """
        Information about the scheduler.
        """
        # return infomation about Schedule
        return "    . db -> {self.dburi}".format(self=self)
