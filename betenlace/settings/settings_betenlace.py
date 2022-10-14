from .base import *

# urls temp file
ALLOWED_HOSTS = [os.getenv("ALLOWED_HOSTS_BETENLACE")]

ROOT_URLCONF = 'betenlace.urls.urls_betenlace'

LOGS_DIR = os.path.join("logs", "betenlace")

# API chat webhooks
CHAT_WEBHOOK_PARTNERS_REGISTRATION = os.getenv("CHAT_WEBHOOK_PARTNERS_REGISTRATION")
CHAT_WEBHOOK_REPORT_UPLOAD = os.getenv("CHAT_WEBHOOK_REPORT_UPLOAD")
CHAT_WEBHOOK_CELERY = os.getenv("CHAT_WEBHOOK_CELERY")

CHAT_WEBHOOK_DJANGO_GEN = os.getenv("CHAT_WEBHOOK_DJANGO_GEN")
CHAT_WEBHOOK_DJANGO_SERVER = os.getenv("CHAT_WEBHOOK_DJANGO_SERVER")
CHAT_WEBHOOK_DJANGO_REQUEST = os.getenv("CHAT_WEBHOOK_DJANGO_REQUEST")
CHAT_WEBHOOK_DJANGO_APPS = os.getenv("CHAT_WEBHOOK_DJANGO_APPS")
CHAT_WEBHOOK_DJANGO_IPS = os.getenv("CHAT_WEBHOOK_DJANGO_IPS")
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "simple": {
            "format": "%(name)-12s %(levelname)-8s %(message)s",
            "style": "%",
        },
        "simple.debug": {
            "format": "\033[95m%(name)-12s %(levelname)-8s %(message)s\033[0m",
            "style": "%",
        },
        "simple.info": {
            "format": "\033[96m%(name)-12s %(levelname)-8s %(message)s\033[0m",
            "style": "%",
        },
        "simple.warning": {
            "format": "\033[93m%(name)-12s %(levelname)-8s %(message)s\033[0m",
            "style": "%",
        },
        "simple.error": {
            "format": "\033[91m%(name)-12s %(levelname)-8s %(message)s\033[0m",
            "style": "%",
        },
        "simple.critical": {
            "format": "\33[1m\033[91m%(name)-12s %(levelname)-8s %(message)s\033[0m",
            "style": "%",
        },
        "simple.db": {
            "format": "\033[93m%(name)-12s %(levelname)-8s \33[35m%(message)s\033[0m",
            "style": "%",
        },
        "simple.installed_apps": {
            "format": "\033[1m\033[94m%(message)s\033[0m",
            "style": "%",
        },
        "time_simple": {
            "format": "%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
            "style": "%",
        },
        "ip_csv": {
            "format": "%(asctime)s,%(message)s",
            "style": "%",
        },
        "only_message": {
            "format": "{message}",
            "style": "{",
        },
        "verbose": {
            "format": "{levelname} {asctime} {module} {process:d} {thread:d} {message}",
            "style": "{",
        },
        "django.server": {
            "()": "django.utils.log.ServerFormatter",
            "format": "[{server_time}] {message}",
            "style": "{",
        },
        "django.server.debug": {
            "()": "django.utils.log.ServerFormatter",
            "format": "\033[95m[{server_time}] {message}\033[0m",
            "style": "{",
        },
        "django.server.info": {
            "()": "django.utils.log.ServerFormatter",
            "format": "\033[96m[{server_time}] {message}\033[0m",
            "style": "{",
        },
        "django.server.warning": {
            "()": "django.utils.log.ServerFormatter",
            "format": "\033[93m[{server_time}] {message}\033[0m",
            "style": "{",
        },
        "django.server.error": {
            "()": "django.utils.log.ServerFormatter",
            "format": "\033[91m[{server_time}] {message}\033[0m",
            "style": "{",
        },
        "django.server.critical": {
            "()": "django.utils.log.ServerFormatter",
            "format": "\33[1m\033[91m[{server_time}] {message}\033[0m",
            "style": "{",
        },
        "chat.gen": {
            "format": (
                "*LEVEL:* `{levelname}`\n*Time:* `{asctime}`\n*Name:* `{name}`\n*Module:*`{module}`\n\n"
                "_*extra data*_\n*Process:* `{process:d}`\n*Thread:* `{thread:d}`\n*Message:* ```{message}```"
            ),
            "style": "{",
        },
    },
    "filters": {
        "require_debug_true": {
            "()": "django.utils.log.RequireDebugTrue",
        },
        "require_debug_false": {
            "()": "django.utils.log.RequireDebugFalse",
        },
        # Only certain level
        "debug_level_only": {
            "()": "core.logger.filter_level.FilterLevels",
            "filter_levels": [
                "DEBUG"
            ]
        },
        "info_level_only": {
            "()": "core.logger.filter_level.FilterLevels",
            "filter_levels": [
                "INFO"
            ]
        },
        "warning_level_only": {
            "()": "core.logger.filter_level.FilterLevels",
            "filter_levels": [
                "WARNING"
            ]
        },
        "error_level_only": {
            "()": "core.logger.filter_level.FilterLevels",
            "filter_levels": [
                "ERROR"
            ]
        },
        "critical_level_only": {
            "()": "core.logger.filter_level.FilterLevels",
            "filter_levels": [
                "CRITICAL"
            ]
        },
        # Bool
        "console.apps.allow_debug": {
            "()": "core.logger.filter_bool.FilterBool",
            "value": [
                DJANGO_APPS_CONSOLE_LOG_LEVEL in ["DEBUG", "WARNING", "INFO", "ERROR", "CRITICAL"]
            ]
        },
        "console.apps.allow_warning": {
            "()": "core.logger.filter_bool.FilterBool",
            "value": [
                DJANGO_APPS_CONSOLE_LOG_LEVEL in ["WARNING", "INFO", "ERROR", "CRITICAL"]
            ]
        },
        "console.apps.allow_info": {
            "()": "core.logger.filter_bool.FilterBool",
            "value": [
                DJANGO_APPS_CONSOLE_LOG_LEVEL in ["INFO", "ERROR", "CRITICAL"]
            ]
        },
        "console.apps.allow_error": {
            "()": "core.logger.filter_bool.FilterBool",
            "value": [
                DJANGO_APPS_CONSOLE_LOG_LEVEL in ["ERROR", "CRITICAL"]
            ]
        },
        "console.apps.allow_critical": {
            "()": "core.logger.filter_bool.FilterBool",
            "value": [
                DJANGO_APPS_CONSOLE_LOG_LEVEL in ["CRITICAL"]
            ]
        },
        "console.django.server.allow_debug": {
            "()": "core.logger.filter_bool.FilterBool",
            "value": [
                DJANGO_SERVER_CONSOLE_LOG_LEVEL in ["DEBUG", "WARNING", "INFO", "ERROR", "CRITICAL"]
            ]
        },
        "console.django.server.allow_warning": {
            "()": "core.logger.filter_bool.FilterBool",
            "value": [
                DJANGO_SERVER_CONSOLE_LOG_LEVEL in ["WARNING", "INFO", "ERROR", "CRITICAL"]
            ]
        },
        "console.django.server.allow_info": {
            "()": "core.logger.filter_bool.FilterBool",
            "value": [
                DJANGO_SERVER_CONSOLE_LOG_LEVEL in ["INFO", "ERROR", "CRITICAL"]
            ]
        },
        "console.django.server.allow_error": {
            "()": "core.logger.filter_bool.FilterBool",
            "value": [
                DJANGO_SERVER_CONSOLE_LOG_LEVEL in ["ERROR", "CRITICAL"]
            ]
        },
        "console.django.server.allow_critical": {
            "()": "core.logger.filter_bool.FilterBool",
            "value": [
                DJANGO_SERVER_CONSOLE_LOG_LEVEL in ["CRITICAL"]
            ]
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "simple"
        },
        "console.debug_only": {
            "class": "logging.StreamHandler",
            "formatter": "simple.debug",
            "filters": ["debug_level_only"],
            "level": "DEBUG"
        },
        "console.info_only": {
            "class": "logging.StreamHandler",
            "formatter": "simple.info",
            "filters": ["info_level_only"],
            "level": "INFO"
        },
        "console.warning_only": {
            "class": "logging.StreamHandler",
            "formatter": "simple.warning",
            "filters": ["warning_level_only"],
            "level": "WARNING"
        },
        "console.error_only": {
            "class": "logging.StreamHandler",
            "formatter": "simple.error",
            "filters": ["error_level_only"],
            "level": "ERROR"
        },
        "console.critical_only": {
            "class": "logging.StreamHandler",
            "formatter": "simple.critical",
            "filters": ["critical_level_only"],
            "level": "CRITICAL"
        },
        "console.django": {
            "class": "logging.StreamHandler",
            "formatter": "django.server",
            "level": DJANGO_GEN_CONSOLE_LOG_LEVEL,
        },
        "console.django.request": {
            "class": "logging.StreamHandler",
            "formatter": "django.server",
            "level": DJANGO_REQUEST_CONSOLE_LOG_LEVEL,
        },
        "console.django.server": {
            "class": "logging.StreamHandler",
            "formatter": "django.server",
            "level": DJANGO_SERVER_CONSOLE_LOG_LEVEL,
        },
        "console.django.server.debug": {
            "class": "logging.StreamHandler",
            "formatter": "django.server.debug",
            "filters": ["debug_level_only", "console.django.server.allow_debug"],
            "level": "DEBUG"
        },
        "console.django.server.warning": {
            "class": "logging.StreamHandler",
            "formatter": "django.server.debug",
            "filters": ["warning_level_only", "console.django.server.allow_warning"],
            "level": "WARNING"
        },
        "console.django.server.info": {
            "class": "logging.StreamHandler",
            "formatter": "django.server.info",
            "filters": ["info_level_only", "console.django.server.allow_info"],
            "level": "INFO"
        },
        "console.django.server.error": {
            "class": "logging.StreamHandler",
            "formatter": "django.server.error",
            "filters": ["error_level_only", "console.django.server.allow_error"],
            "level": "ERROR"
        },
        "console.django.server.critical": {
            "class": "logging.StreamHandler",
            "formatter": "django.server.critical",
            "filters": ["critical_level_only", "console.django.server.allow_critical"],
            "level": "CRITICAL"
        },
        "console.db": {
            "class": "logging.StreamHandler",
            "formatter": "simple.db",
            "level": os.getenv("DJANGO_DB_CONSOLE_LOG_LEVEL", "INFO")
        },
        "console.installed_apps": {
            "class": "logging.StreamHandler",
            "formatter": "simple.installed_apps",
            "level": "INFO"
        },
        "console.apps.debug": {
            "class": "logging.StreamHandler",
            "formatter": "simple.debug",
            "filters": ["debug_level_only", "console.apps.allow_debug"],
            "level": "DEBUG"
        },
        "console.apps.warning": {
            "class": "logging.StreamHandler",
            "formatter": "simple.debug",
            "filters": ["warning_level_only", "console.apps.allow_warning"],
            "level": "WARNING"
        },
        "console.apps.info": {
            "class": "logging.StreamHandler",
            "formatter": "simple.info",
            "filters": ["info_level_only", "console.apps.allow_info"],
            "level": "INFO"
        },
        "console.apps.error": {
            "class": "logging.StreamHandler",
            "formatter": "simple.error",
            "filters": ["error_level_only", "console.apps.allow_error"],
            "level": "ERROR"
        },
        "console.apps.critical": {
            "class": "logging.StreamHandler",
            "formatter": "simple.critical",
            "filters": ["critical_level_only", "console.apps.allow_critical"],
            "level": "CRITICAL"
        },
        "file.django": {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": os.path.join(LOGS_DIR, "django_{}.log".format(DJANGO_GEN_FILE_LOG_LEVEL.lower())),
            "maxBytes": 5242880,  # 1024 * 1024 * 5B = 5MB
            "backupCount": 10,
            "filters": ["require_debug_false"],
            "formatter": "django.server",
            "level": DJANGO_GEN_FILE_LOG_LEVEL,
        },
        "file.django.server": {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": os.path.join(LOGS_DIR, "django_server_{}.log".format(DJANGO_SERVER_FILE_LOG_LEVEL.lower())),
            "maxBytes": 5242880,  # 1024 * 1024 * 5B = 5MB
            "backupCount": 10,
            "filters": ["require_debug_false"],
            "formatter": "django.server",
            "level": DJANGO_SERVER_FILE_LOG_LEVEL,
        },
        "file.django.db": {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": os.path.join(LOGS_DIR, "django_db_{}.log".format(DJANGO_DB_FILE_LOG_LEVEL.lower())),
            "maxBytes": 5242880,  # 1024 * 1024 * 5B = 5MB
            "backupCount": 10,
            "filters": ["require_debug_false"],
            "formatter": "django.server",
            "level": DJANGO_DB_FILE_LOG_LEVEL,
        },
        "file.django.request": {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": os.path.join(LOGS_DIR, "django_request_{}.log".format(DJANGO_REQUEST_FILE_LOG_LEVEL.lower())),
            "maxBytes": 5242880,  # 1024 * 1024 * 5B = 5MB
            "backupCount": 10,
            "filters": ["require_debug_false"],
            "formatter": "django.server",
            "level": DJANGO_REQUEST_FILE_LOG_LEVEL,
        },
        "file.apps": {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": os.path.join(LOGS_DIR, "django_apps_{}.log".format(DJANGO_APPS_FILE_LOG_LEVEL.lower())),
            "maxBytes": 5242880,  # 1024 * 1024 * 5B = 5MB
            "backupCount": 10,
            "formatter": "time_simple",
            "filters": ["require_debug_false"],
            "level": DJANGO_APPS_FILE_LOG_LEVEL,
        },
        "file.ips": {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": os.path.join(LOGS_DIR, "django_request_ips.log"),
            "maxBytes": 5242880,  # 1024 * 1024 * 5B = 5MB
            "backupCount": 10,
            "filters": ["require_debug_false"],
            "formatter": "ip_csv",
            "level": DJANGO_IPS_FILE_LOG_LEVEL,
        },
        "file.logger": {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": os.path.join(LOGS_DIR, "django_core_logger_critical.log"),
            "maxBytes": 5242880,  # 1024 * 1024 * 5B = 5MB
            "backupCount": 10,
            "filters": ["require_debug_false"],
            "formatter": "django.server",
            "level": "CRITICAL",
        },
        "mail_admins.error": {
            "class": "django.utils.log.AdminEmailHandler",
            "filters": ["require_debug_false"],
            "level": "ERROR",
        },
        "mail_admins.critical": {
            "class": "django.utils.log.AdminEmailHandler",
            "filters": ["require_debug_false"],
            "level": "CRITICAL",
        },
        "chat.general": {
            "class": "core.logger.chat.ChatLogHandler",
            "webhook_url": CHAT_WEBHOOK_DJANGO_GEN,
            "formatter": "chat.gen",
            "filters": ["require_debug_false"],
            "level": DJANGO_GEN_CHAT_LOG_LEVEL,
        },
        "chat.request": {
            "class": "core.logger.chat.ChatLogHandler",
            "webhook_url": CHAT_WEBHOOK_DJANGO_REQUEST,
            "formatter": "chat.gen",
            "filters": ["require_debug_false"],
            "level": DJANGO_REQUEST_CHAT_LOG_LEVEL,
        },
        "chat.server": {
            "class": "core.logger.chat.ChatLogHandler",
            "webhook_url": CHAT_WEBHOOK_DJANGO_SERVER,
            "formatter": "chat.gen",
            "filters": ["require_debug_false"],
            "level": DJANGO_SERVER_CHAT_LOG_LEVEL,
        },
        "chat.apps": {
            "class": "core.logger.chat.ChatLogHandler",
            "webhook_url": CHAT_WEBHOOK_DJANGO_APPS,
            "formatter": "chat.gen",
            "filters": ["require_debug_false"],
            "level": DJANGO_APPS_CHAT_LOG_LEVEL,
        },
        "chat.ips": {
            "class": "core.logger.chat.ChatLogHandler",
            "webhook_url": CHAT_WEBHOOK_DJANGO_IPS,
            "formatter": "chat.gen",
            "filters": ["require_debug_false"],
            "level": DJANGO_IPS_CHAT_LOG_LEVEL,
        },
        "notion.ips": {
            "class": "core.logger.notion.NotionIPLogHandler",
            "notion_secret_key": LOGGING_NOTION_SECRET_KEY_IPS,
            "notion_db_id": LOGGING_NOTION_DB_ID_IPS,
            "time_zone_settings": TIME_ZONE,
            "formatter": "only_message",
            "filters": ["require_debug_false"],
            "level": DJANGO_IPS_NOTION_LOG_LEVEL,
        },
    },
    "loggers": {
        "django": {
            "handlers": ["console.django", "file.django", "chat.general"],
            "propagate": True,
            "level": "DEBUG",
        },
        "django.request": {
            "handlers": ["mail_admins.error", "file.django.request", "console.django.request", "chat.request"],
            "propagate": False,
            "level": "DEBUG",
        },
        "django.server": {
            "handlers": ["console.django.server.debug", "console.django.server.warning", "console.django.server.info",
                         "console.django.server.error", "console.django.server.critical", "file.django.server",
                         "chat.server"],
            "propagate": False,
            "level": "DEBUG",
        },
        # Log for all DB operations
        "django.db": {
            "handlers": [
                "console.db",
                "file.django.db",
            ],
            "propagate": False,
            "level": "DEBUG",
        },
        # Log for all ip at request call
        "django.ips": {
            "handlers": ["chat.ips", "file.ips", "notion.ips"],
            "propagate": False,
            "level": "DEBUG",
        },
        # Log at project run
        "betenlace.urls": {
            "handlers": ["console.installed_apps"],
            "propagate": True,
            "level": "DEBUG",
        },
        # Console on apps
        "api_partner": {
            "handlers": ["console.apps.debug", "console.apps.warning", "console.apps.info", "console.apps.error",
                         "console.apps.critical", "file.apps", "mail_admins.critical", "chat.apps"
                         ],
            "level": "DEBUG",
            "propagate": False,
        },
        "api_admin": {
            "handlers": ["console.apps.debug", "console.apps.warning", "console.apps.info", "console.apps.error",
                         "console.apps.critical", "file.apps", "mail_admins.critical", "chat.apps"
                         ],
            "level": "DEBUG",
            "propagate": False,
        },
        "core.management": {
            "handlers": ["console.info_only", "console.warning_only",
                         "console.error_only", "console.critical_only"
                         ],
            "level": os.getenv("DJANGO_CUSTOM_COMMANDS_LOG_LEVEL", "INFO"),
            "propagate": False,
        },
        "core.logger": {
            "handlers": ["file.logger"],
            "level": "DEBUG",
            "propagate": False,
        },
        "core.helpers": {
            "handlers": ["console.apps.debug", "console.apps.warning", "console.apps.info", "console.apps.error",
                         "console.apps.critical", "file.apps", "mail_admins.critical", "chat.apps"
                         ],
            "level": "DEBUG",
            "propagate": False,
        },
    },
}

# Create required dir for logs
os.makedirs(os.path.join(LOGS_DIR, "supervisor_log"), exist_ok=True)
os.makedirs(os.path.join(LOGS_DIR, "supervisor_run"), exist_ok=True)
os.makedirs(os.path.join(LOGS_DIR, "celery"), exist_ok=True)
