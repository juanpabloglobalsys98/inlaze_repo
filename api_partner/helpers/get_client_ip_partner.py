def get_client_ip(request):
    if x_forwarded_for := request.META.get('HTTP_X_FORWARDED_FOR'):
        return x_forwarded_for
    elif x_real_ip := request.META.get('HTTP_X_REAL_IP'):
        return x_real_ip
    elif(remote_addr := request.META.get('REMOTE_ADDR')):
        return remote_addr
    return None