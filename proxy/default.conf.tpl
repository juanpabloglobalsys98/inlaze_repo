# Main Backend
# bk.inlaze.com 80 - bk.inlaze.com 443
server {
    listen ${LISTEN_PORT};
    server_name bk.inlaze.com;

    rewrite ^/(.*) https://bk.inlaze.com/$1 redirect;
}
# bk.inlaze.com 443
server {
    listen 443 ssl;
    server_name bk.inlaze.com;

    location /media {
        alias /vol/media;
    }
    
    location / {
        uwsgi_pass              ${APP_HOST_BETENLACE}:${APP_PORT_BETENLACE};
        include                 /etc/nginx/uwsgi_params;
        client_max_body_size    10M;
    }
    # SSL
    ssl_certificate /etc/nginx/certs/inlaze.com.pem;
    ssl_certificate_key /etc/nginx/certs/inlaze.com.key;
}
# Redirects Main backend
# bk.inlazz.com 80 - bk.inlaze.com 443
server {
    listen ${LISTEN_PORT};
    server_name bk.inlazz.com;
    rewrite ^/(.*) https://bk.inlaze.com/$1 redirect;
}
# bk.inlazz.com 443 - bk.inlaze.com 443
server {
    listen 443 ssl;
    server_name bk.inlazz.com;

    rewrite ^/(.*) https://bk.inlaze.com/$1 redirect;
    
    # SSL
    ssl_certificate /etc/nginx/certs/inlazz.com.pem;
    ssl_certificate_key /etc/nginx/certs/inlazz.com.key;
}
# bk.betenlace.com 80 - bk.inlaze.com 443
server {
    listen ${LISTEN_PORT};
    server_name bk.betenlace.com;

    rewrite ^/(.*) https://bk.inlaze.com/$1 redirect;
}
# bk.betenlace.com 443 - bk.inlaze.com 443
server {
    listen 443 ssl;
    server_name bk.betenlace.com;

    rewrite ^/(.*) https://bk.inlaze.com/$1 redirect;

    # SSL
    ssl_certificate /etc/nginx/certs/betenlace.com.pem;
    ssl_certificate_key /etc/nginx/certs/betenlace.com.key;
}

# Campaign redirect
# go.inlaze.com 80 - go.inlaze.com 443
server {
    listen ${LISTEN_PORT};
    server_name go.inlaze.com;
    
    rewrite ^/(.*) https://go.inlaze.com/$1 redirect;
}
# go.inlaze.com 443
server {
    listen 443 ssl;
    server_name go.inlaze.com;
    
    location / {
        uwsgi_pass              ${APP_HOST_REDIRECT}:${APP_PORT_REDIRECT};
        include                 /etc/nginx/uwsgi_params;
        client_max_body_size    10M;
    }
    # SSL
    ssl_certificate /etc/nginx/certs/inlaze.com.pem;
    ssl_certificate_key /etc/nginx/certs/inlaze.com.key;
}
# Redirects Campaign redirect
# go.inlazz.com 80 - go.inlaze.com 443
server {
    listen ${LISTEN_PORT};
    server_name go.inlazz.com;

    rewrite ^/(.*) https://go.inlaze.com/$1 redirect;
}
# go.inlazz.com 443 - go.inlaze.com 443
server {
    listen 443 ssl;
    server_name go.inlazz.com;

    rewrite ^/(.*) https://go.inlaze.com/$1 redirect;
    
    # SSL
    ssl_certificate /etc/nginx/certs/inlazz.com.pem;
    ssl_certificate_key /etc/nginx/certs/inlazz.com.key;
}
# betlinks.bet 80 - go.inlaze.com 443
server {
    listen ${LISTEN_PORT};
    server_name betlinks.bet;

    rewrite ^/(.*) https://go.inlaze.com/$1 redirect;
}
# betlinks.bet 443 - go.inlaze.com 443
server {
    listen 443 ssl;
    server_name betlinks.bet;

    rewrite ^/(.*) https://go.inlaze.com/$1 redirect;
    
    # SSL
    ssl_certificate /etc/nginx/certs/betlinks.bet.pem;
    ssl_certificate_key /etc/nginx/certs/betlinks.bet.key;
}