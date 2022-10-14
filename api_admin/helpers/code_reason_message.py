def get_message_from_code_reason(code_reason, user_language):
    """
    Gets the message for a certain CodeReason, filtered by the user's language.
    If not found will return the message in the default language.
    """
    message = code_reason.translate_messages.filter(
        language=user_language,
    ).first()
    return message or code_reason.get_default_message()
