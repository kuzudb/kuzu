FROM nginx:stable
COPY --chown=nginx:nginx ./releases /usr/share/nginx/html
COPY --chown=nginx:nginx ./dataset /usr/share/nginx/html/dataset
COPY nginx.conf /etc/nginx/nginx.conf
RUN rm /usr/share/nginx/html/index.html /usr/share/nginx/html/50x.html
