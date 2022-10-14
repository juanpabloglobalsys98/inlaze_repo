FROM nginxinc/nginx-unprivileged:1-alpine
LABEL mantainer="juanpabloperezdevelopment@gmail.com"

COPY ./default.conf.tpl /etc/nginx/default.conf.tpl
COPY ./uwsgi_params /etc/nginx/uwsgi_params
COPY ./run.sh /run.sh
COPY ./ssl/betlinks.bet.key /etc/nginx/certs/betlinks.bet.key
COPY ./ssl/betlinks.bet.pem /etc/nginx/certs/betlinks.bet.pem
COPY ./ssl/betenlace.com.key /etc/nginx/certs/betenlace.com.key
COPY ./ssl/betenlace.com.pem /etc/nginx/certs/betenlace.com.pem
COPY ./ssl/inlazz.com.key /etc/nginx/certs/inlazz.com.key
COPY ./ssl/inlazz.com.pem /etc/nginx/certs/inlazz.com.pem
COPY ./ssl/inlaze.com.key /etc/nginx/certs/inlaze.com.key
COPY ./ssl/inlaze.com.pem /etc/nginx/certs/inlaze.com.pem

ENV LISTEN_PORT=8000
ENV APP_HOST_BETENLACE=app_betenlace
ENV APP_HOST_REDIRECT=app_redirect
ENV APP_PORT_BETENLACE=9000
ENV APP_PORT_REDIRECT=9001

USER root

RUN mkdir -p /vol/media && \
    chmod 755 /vol/media && \
    touch /etc/nginx/conf.d/default.conf && \
    chown nginx:nginx /etc/nginx/conf.d/default.conf && \
    chmod +x /run.sh

VOLUME /vol/media

USER nginx

CMD ["/run.sh"]