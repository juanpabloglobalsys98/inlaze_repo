from rest_framework.utils.formatting import camelcase_to_spaces


def get_view_name(view_func):
    view_name = getattr(view_func, '__qualname__', view_func.__class__.__name__)
    return camelcase_to_spaces(view_name)


def get_codename(class_name, method_name="get"):
    return f"{get_view_name(class_name)}-{method_name}".lower()

