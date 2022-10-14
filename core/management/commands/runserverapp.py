import os

from django.conf import settings
from django.core.management.base import CommandError
from django.core.management.commands.runserver import \
    Command as RunserverCommand


class Command(RunserverCommand):
    """
    Custom command Runserver, this is same to runserver command, and 
    have the custom argument --apps, this argument can indicates the apps
    that will route on runserver process.
    """
    # Add admin for admin.site distinction
    default_apps_to_run = settings.INSTALLED_CUSTOM_APPS.copy()+["admin"]
    
    def add_arguments(self, parser):
        """
        Arguments that have the custom command runserver
        """
        parser.add_argument(
            'addrport', nargs='?',
            help='Optional port number, or ipaddr:port'
        )
        parser.add_argument(
            '--ipv6', '-6', action='store_true', dest='use_ipv6',
            help='Tells Django to use an IPv6 address.',
        )
        parser.add_argument(
            '--nothreading', action='store_false', dest='use_threading',
            help='Tells Django to NOT use threading.',
        )
        parser.add_argument(
            '--noreload', action='store_false', dest='use_reloader',
            help='Tells Django to NOT use the auto-reloader.',
        )
        parser.add_argument(
            '--apps', nargs="+", default=self.default_apps_to_run,
            help='Determine the custom apps that will run',
        )
    
    def inner_run(self, *args, **options):
        """
        Execute process of command, this custom definition run previous
        to the main extend class from method inner_run
        """
        # Must be this file on /ProyectRoot/core/management/commants and try to 
        # search a file on /ProyectRoot
        urls_file=open(os.path.join(os.path.dirname(__file__), "../../../.urls"), 
            "w"
        )

        # Check if apps from param are valid and remove from INSTALLED_APPS
        for app_name in options['apps']:
            if not (app_name in self.default_apps_to_run):
                raise CommandError("the app '{}' is not defined".format(app_name))
            urls_file.write(app_name+"\n")
        urls_file.close()
        super().inner_run(*args, **options)
