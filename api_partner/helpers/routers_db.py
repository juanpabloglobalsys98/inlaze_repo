from core.helpers.default_auth_session_apps import DEFAULT_APPS
from core.helpers.path_route_db import request_cfg

USER_TOP_PATH_URLS = ["api_partner"]

DB_USER_PARTNER = "default"


class DefaultRouter:
    """
    A router to control all database operations on default models
    """
    app_labels_current = ("api_partner",)
    app_labels_gen = app_labels_current + DEFAULT_APPS
    db_name = DB_USER_PARTNER

    def db_for_read(self, model, **hints):
        """
        Attempts to read models of app api_partner
        """
        if (model._meta.app_label in DEFAULT_APPS):
            if hasattr(request_cfg, 'is_partner'):
                if(request_cfg.is_partner):
                    return self.db_name

        # Only default apps that manage sessions will be check by url
        if (model._meta.app_label in DEFAULT_APPS):
            if hasattr(request_cfg, 'url_path'):
                if(request_cfg.url_path.split("/")[1] in USER_TOP_PATH_URLS):
                    return self.db_name

        # Only non-admin users are in Default DB
        if model._meta.app_label in self.app_labels_current:
            return self.db_name
        return None

    def db_for_write(self, model, **hints):
        """
        Attempts to write models of app api_partner
        """
        if (model._meta.app_label in DEFAULT_APPS):
            if hasattr(request_cfg, 'is_partner'):
                if(request_cfg.is_partner):
                    return self.db_name

        # Only default apps that manage sessions will be check by url
        if (model._meta.app_label in DEFAULT_APPS):
            if hasattr(request_cfg, 'url_path'):
                if(request_cfg.url_path.split("/")[1] in USER_TOP_PATH_URLS):
                    return self.db_name

        if model._meta.app_label in self.app_labels_current:
            return self.db_name
        return None

    def allow_relation(self, obj1, obj2, **hints):
        """
        Allow relations if the models in the app api_partner is involved.
        """
        if (
            obj1._meta.app_label in self.app_labels_gen or
            obj2._meta.app_label in self.app_labels_gen
        ):
            return True
        return None

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        """
        Make determine migrations for models of app api_partner
        """
        if app_label in self.app_labels_current:
            return db == self.db_name

        if app_label in DEFAULT_APPS and db == self.db_name:
            return True

        return None
