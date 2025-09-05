# coding=utf-8
"""SQLAlchemy session."""

import time
from contextlib import contextmanager

from celery.utils.time import get_exponential_backoff_interval
from kombu.utils.compat import register_after_fork
from omni.pro.models.base import Base as OmniBase
from sqlalchemy import DDL, Column, Integer, create_engine
from sqlalchemy.exc import DatabaseError
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

# from sqlalchemy.schema import CreateSchema  # does not work for SA 1.4


class Base(OmniBase):
    """Base class which provides automated table name
    and surrogate primary key column.

    """

    @declared_attr
    def __tablename__(cls):
        return f"celery_{cls.__name__.lower()}"

    id = Column(Integer, primary_key=True)

    __table_args__ = {"sqlite_autoincrement": True, "schema": "celery_schema"}


ModelBase = declarative_base(cls=Base)
PREPARE_MODELS_MAX_RETRIES = 10


@contextmanager
def session_cleanup(session):
    """
    Context manager to handle session cleanup.
    Rolls back the session in case of an exception and ensures the session is closed.
    Args:
        session (Session): The SQLAlchemy session to manage.
    """
    try:
        yield
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def _after_fork_cleanup_session(session):
    """Handle after fork cleanup for session."""
    session._after_fork()


class SessionManager:
    """
    Manage SQLAlchemy sessions.
    It handles engine and session creation, including special handling for
    forking scenarios.
    Args:
        dburi (str): Database URI.
        schema (str, optional): Database schema. Defaults to None.
        short_lived_sessions (bool, optional): Whether to use short-lived sessions. Defaults to False.
        **kwargs: Additional arguments passed to create_engine.
    Methods:
        get_engine(dburi, **kwargs): Get or create an engine for the given dburi.
        create_session(dburi, schema=None, short_lived_sessions=False, **kwargs): Create a sessionmaker for the given dburi and schema.
        prepare_models(engine, schema=None): Prepare the database models.
        session_factory(dburi, schema=None, **kwargs): Create a session factory for the given dburi and schema.
    """

    def __init__(self):
        """
        Initialize the SessionManager.
        Args:
            register_after_fork (callable, optional): Function to register after fork handlers. Defaults to None.
            dburi (str): Database URI.
            schema (str, optional): Database schema. Defaults to None.
            short_lived_sessions (bool, optional): Whether to use short-lived sessions. Defaults to False.
            **kwargs: Additional arguments passed to create_engine.
        """
        self._engines = {}
        self._sessions = {}
        self.forked = False
        self.prepared = False
        if register_after_fork is not None:
            register_after_fork(self, _after_fork_cleanup_session)

    def _after_fork(self):
        """Handle after fork cleanup."""
        self.forked = True

    def get_engine(self, dburi, **kwargs):
        """
        Get or create an engine for the given dburi.
        Args:
            dburi (str): Database URI.
            **kwargs: Additional arguments passed to create_engine.
        Returns:
            Engine: SQLAlchemy engine.
        """
        if self.forked:
            try:
                return self._engines[dburi]
            except KeyError:
                engine = self._engines[dburi] = create_engine(dburi, **kwargs)
                return engine
        else:
            kwargs = {k: v for k, v in kwargs.items() if not k.startswith("pool")}
            return create_engine(dburi, poolclass=NullPool, **kwargs)

    def create_session(self, dburi, schema=None, short_lived_sessions=False, **kwargs):
        """
        Create a sessionmaker for the given dburi and schema.
        Args:
            dburi (str): Database URI.
            schema (str, optional): Database schema. Defaults to None.
            short_lived_sessions (bool, optional): Whether to use short-lived sessions. Defaults to False.
            **kwargs: Additional arguments passed to create_engine.
        Returns:
            tuple: (Engine, sessionmaker)
        """
        engine = self.get_engine(dburi, future=True, **kwargs)
        engine = engine.execution_options(schema_translate_map={"celery_schema": schema})
        if self.forked:
            if short_lived_sessions or dburi not in self._sessions:
                self._sessions[dburi] = sessionmaker(bind=engine, expire_on_commit=False)
            return engine, self._sessions[dburi]
        return engine, sessionmaker(bind=engine, expire_on_commit=False)

    def prepare_models(self, engine, schema=None):
        """
        Prepare the database models.
        Args:
            engine (Engine): SQLAlchemy engine.
            schema (str, optional): Database schema. Defaults to None.
        """
        if not self.prepared:
            # SQLAlchemy will check if the items exist before trying to
            # create them, which is a race condition. If it raises an error
            # in one iteration, the next may pass all the existence checks
            # and the call will succeed.
            retries = 0
            while True:
                try:
                    if schema:
                        with engine.connect() as connection:
                            connection.execute(DDL("CREATE SCHEMA IF NOT EXISTS %(schema)s", {"schema": schema}))
                            connection.commit()

                    ModelBase.metadata.create_all(engine)
                except DatabaseError:
                    if retries < PREPARE_MODELS_MAX_RETRIES:
                        sleep_amount_ms = get_exponential_backoff_interval(10, retries, 1000, True)
                        time.sleep(sleep_amount_ms / 1000)
                        retries += 1
                    else:
                        raise
                else:
                    break

            self.prepared = True

    def session_factory(self, dburi, schema=None, **kwargs):
        """
        Create a session factory for the given dburi and schema.
        """
        engine, session = self.create_session(dburi, schema=schema, **kwargs)
        self.prepare_models(engine)
        return session()
