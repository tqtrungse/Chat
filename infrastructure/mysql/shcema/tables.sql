-- Flow:
--    Client sends phone number + token + device type + OS type
--      * token is not empty -> if expired, renew.
--      * token is empty:
--          + If phone number doesn't exist -> new account -> login by Auth0 phone OTP.
--          + If phone number exists -> new device -> send OTP to channel on root device.
CREATE TABLE IF NOT EXISTS `users`
(
    `id`          BIGINT UNSIGNED NOT NULL,           -- Snowflake
    `username`    NVARCHAR(50)    NOT NULL,
    `phone_hash`  BINARY(16)      NOT NULL,           -- HMAC-SHA256 full 256-bit truncate to 128 bit
    `phone_tail`  INT UNSIGNED    NOT NULL,
    `state`       SMALLINT        NOT NULL DEFAULT 1, -- 1: active, 3: locked
    `created_at`  TIMESTAMP       NOT NULL NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    `updated_at`  TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (`id`),
    UNIQUE (`username`),
    UNIQUE (phone_hash)
) ENGINE = INNODB;

-- Users have max 5 devices.
CREATE TABLE IF NOT EXISTS `devices`
(
    `id`           BIGINT UNSIGNED NOT NULL,           -- Snowflake
    `external_id`  NVARCHAR(64)    NOT NULL,           -- Auth0 ID
    `user_id`      BIGINT UNSIGNED NOT NULL,
    `name`         NVARCHAR(128)   NOT NULL,           -- Device type + OS name
    `state`        SMALLINT        NOT NULL DEFAULT 1, -- 1: active, 2: locked
    `identity_pub` TINYBLOB        NOT NULL,           -- ed25519.GenerateKey(rand.Reader)
    `peer_pub`     TINYBLOB        NOT NULL,           -- x25519
    `created_at`   TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    `updated_at`   TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    `deleted_at`   TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE (`external_id`),
    FOREIGN KEY (`user_id`) REFERENCES users (`id`)
) ENGINE = INNODB;

CREATE TABLE IF NOT EXISTS channels
(
    `id`         BIGINT UNSIGNED NOT NULL, -- Snowflake
    `name`       NVARCHAR(128),            -- Not null for group
    `created_at` TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    `updated_at` TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (`id`)
) ENGINE = INNODB;

CREATE TABLE IF NOT EXISTS channel_members
(
    `channel_id` BIGINT UNSIGNED NOT NULL, -- Snowflake
    `user_id`    BIGINT UNSIGNED NOT NULL,
    `role`       SMALLINT,                 -- Not null for group, 1: admin, 2: member
    `joined_at`  TIMESTAMP,                -- Not null for group
    `left_at`    TIMESTAMP,                -- Not null for group
    PRIMARY KEY (`channel_id`, `user_id`),
    FOREIGN KEY (`channel_id`) REFERENCES channels (`id`),
    FOREIGN KEY (`user_id`) REFERENCES users (`id`)
) ENGINE = INNODB;