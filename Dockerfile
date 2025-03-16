FROM eclipse-temurin:21-alpine

# Configure build options
ARG IMAGE_BUILD_TIMESTAMP
ENV IMAGE_BUILD_TIMESTAMP=${IMAGE_BUILD_TIMESTAMP}
RUN echo IMAGE_BUILD_TIMESTAMP=${IMAGE_BUILD_TIMESTAMP}

# Install packages
RUN apk add --update --no-cache tzdata \
    bash \
    && rm -rf /var/cache/apk/*

# Set timezone
ENV TZ="Australia/Sydney"
ENV MLLP_CERTS="/etc/mllp/secrets"

# Location for Certificates
RUN mkdir -p /etc/mllp/secrets

WORKDIR /app

COPY target/cap-pathology-mllp-1.0.0-SNAPSHOT.jar /app/cap-pathology-mllp-1.0.0-SNAPSHOT.jar
COPY src/main/resources/logback.xml /app/logback.xml
COPY start-mllp.sh /usr/local/bin/start-mllp.sh
RUN chmod +x /usr/local/bin/start-mllp.sh

ENTRYPOINT ["/usr/local/bin/start-mllp.sh"]