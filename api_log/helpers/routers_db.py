from core.helpers.default_auth_session_apps import DEFAULT_APPS
from core.helpers.path_route_db import request_cfg

HISTORY_TOP_PATH_URLS = ["history", "api_log"]

DB_HISTORY = "history"


class HistoryRouter:
    """
    A router to control all database operations for history models
    """
    app_labels_current = ("api_log",)
    app_labels_read_write = ("api_log", "contenttypes")
    app_labels_gen = app_labels_current + DEFAULT_APPS
    db_name = DB_HISTORY

    def db_for_read(self, model, **hints):
        """
        Attempts to read models of app api_log and django default app models
        """

        # Only admin users are in admin DB
        if model._meta.app_label in self.app_labels_read_write:
            return self.db_name
        return None

    def db_for_write(self, model, **hints):
        """
        Attempts to write models of app api_log and django default app
        models
        """

        if model._meta.app_label in self.app_labels_read_write:
            return self.db_name
        return None

    def allow_relation(self, obj1, obj2, **hints):
        """
        Allow relations if the models in the app api_log and django
        default app is involved
        """
        if (
            obj1._meta.app_label in self.app_labels_gen or
            obj2._meta.app_label in self.app_labels_gen
        ):
            return True
        return None

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        """
        Make tool determines migrations for app models (api_log and django
        default models) it does not take effect on default DB, on django
        version 3.2.5
        """

        if app_label in self.app_labels_current:
            return db == self.db_name

        if db == self.db_name and app_label == "contenttypes":
            return True

        if app_label in DEFAULT_APPS and db == self.db_name:
            return False

        return None
