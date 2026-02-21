FROM debian:latest
RUN mkdir /app
WORKDIR /app
COPY fulminata /app/.
EXPOSE 4595
CMD ./fulminata
