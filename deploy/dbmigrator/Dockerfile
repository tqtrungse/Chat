FROM ruby:3.2.9-slim

# Install dependencies
RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends \
    build-essential \
    default-libmysqlclient-dev \
    default-mysql-client \
    postgresql-client \
    libpq-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Pin gem versions via build args for reproducibility
ARG RIDGEPOLE_VERSION=3.0.4
ARG MYSQL2_VERSION=0.5.7

# Install Ridgepole and database adapters (pinned versions)
RUN gem install ridgepole -v ${RIDGEPOLE_VERSION} --no-document && \
    gem install mysql2 -v ${MYSQL2_VERSION} --no-document
