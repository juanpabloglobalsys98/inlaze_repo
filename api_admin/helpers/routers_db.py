from core.helpers.default_auth_session_apps import DEFAULT_APPS
from core.helpers.path_route_db import request_cfg

ADMIN_TOP_PATH_URLS = ["admin", "api_admin"]

DB_ADMIN = "admin"


class AdminRouter:
    """
    A router to control all database operations for admin models
    """
    app_labels_current = ("api_admin", "admin",
                          "django_celery_beat", "django_celery_results")
    app_labels_gen = app_labels_current + DEFAULT_APPS
    db_name = DB_ADMIN

    def db_for_read(self, model, **hints):
        """
        Attempts to read models of app api_admin and django default app
        models
        """

        if (model._meta.app_label in DEFAULT_APPS):
            if hasattr(request_cfg, 'is_partner'):
                if(request_cfg.is_partner):
                    return None

        # Only default apps that manage sessions will be check by url
        if (model._meta.app_label in DEFAULT_APPS):
            if hasattr(request_cfg, 'url_path'):
                if(request_cfg.url_path.split("/")[1] in ADMIN_TOP_PATH_URLS):
                    return self.db_name

        # Only admin users are in admin DB
        if model._meta.app_label in self.app_labels_current:
            return self.db_name
        return None

    def db_for_write(self, model, **hints):
        """
        Attempts to write models of app api_admin and django default app
        models
        """

        if (model._meta.app_label in DEFAULT_APPS):
            if hasattr(request_cfg, 'is_partner'):
                if(request_cfg.is_partner):
                    return None

        # Only default apps that manage sessions will be check by url
        if (model._meta.app_label in DEFAULT_APPS):
            if hasattr(request_cfg, 'url_path'):
                if(request_cfg.url_path.split("/")[1] in ADMIN_TOP_PATH_URLS):
                    return self.db_name

        if model._meta.app_label in self.app_labels_current:
            return self.db_name
        return None

    def allow_relation(self, obj1, obj2, **hints):
        """
        Allow relations if the models in the app api_admin and django
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
        Make tool determines migrations for app models (api_admin and django
        default models) it does not take effect on default DB, on django
        version 3.2.5
        """

        if app_label in self.app_labels_current:
            return db == self.db_name

        if app_label in DEFAULT_APPS and db == self.db_name:
            return True

        return None
