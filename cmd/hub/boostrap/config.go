/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package boostrap

//import (
//	"time"
//
//	"xxx/internal/hub/connection"
//	"xxx/internal/hub/infrastructure/nats"
//	"xxx/internal/hub/infrastructure/redis"
//
//	"xxx/pkg"
//
//	"github.com/spf13/viper"
//)

//type InfraConfig struct {
//	HubID   uint64
//	NatsURL string
//}
//
//type Config interface {
//	Redis() *redis.Config
//	Nats() *nats.Config
//	NatsConn() *nats.ConnConfig
//	Infra() *InfraConfig
//	Router() *connection.Config
//}
//
//func NewViperConfig(folderPath, fileName string) (Config, error) {
//	v, err := pkg.DefaultConfigLoader(folderPath, fileName)
//	if err != nil {
//		return nil, err
//	}
//	return &viperConfig{
//		viper: v,
//	}, nil
//}
//
//type viperConfig struct {
//	viper *viper.Viper
//}
//
//func (v *viperConfig) Redis() *redis.Config {
//	return &redis.Config{
//		Addr:              v.viper.GetString("REDIS_ADDR"),
//		Password:          v.viper.GetString("REDIS_PASSWORD"),
//		PoolSize:          v.viper.GetUint16("REDIS_POOL_SIZE"),
//		MaxIdleConns:      v.viper.GetUint16("REDIS_MAX_IDLE_CONNS"),
//		MaxActiveConns:    v.viper.GetUint16("REDIS_MAX_ACTIVE_CONNS"),
//		DB:                v.viper.GetUint16("REDIS_DB"),
//		HeartbeatInterval: v.viper.GetDuration("REDIS_HEARTBEAT_INTERVAL"),
//		HeartbeatTTL:      v.viper.GetDuration("REDIS_HEARTBEAT_TTL"),
//		ReapBatchSize:     v.viper.GetUint16("REDIS_REAP_BATCH_SIZE"),
//		DeviceBucketCount: v.viper.GetUint16("REDIS_DEVICE_BUCKET_COUNT"),
//	}
//}
//
//func (v *viperConfig) Nats() *nats.Config {
//	var (
//		idx             = 0
//		strs            = v.viper.GetStringSlice("NATS_CONSUMER_BACKOFF")
//		consumerBackOff = make([]time.Duration, len(strs))
//	)
//	for idx = 0; idx < len(strs); idx++ {
//		consumerBackOff[idx], _ = time.ParseDuration(strs[idx])
//	}
//
//	return &nats.Config{
//		HubStreamName:            v.viper.GetString("NATS_HUB_STREAM_NAME"),
//		HubSubjectPrefix:         v.viper.GetString("NATS_HUB_SUBJECT_PREFIX"),
//		OfflineStreamName:        v.viper.GetString("NATS_OFFLINE_STREAM_NAME"),
//		OfflineSubjectPrefix:     v.viper.GetString("NATS_OFFLINE_SUBJECT_PREFIX"),
//		MaxHops:                  v.viper.GetUint8("NATS_MAX_HOPS"),
//		Replicas:                 v.viper.GetInt("NATS_REPLICAS"),
//		PublishTimeout:           v.viper.GetDuration("NATS_PUBLISH_TIMEOUT"),
//		OfflineMaxMsgsPerSubject: v.viper.GetInt64("NATS_MAX_MSG_PER_SUBJECT"),
//		ConsumerAckWait:          v.viper.GetDuration("NATS_CONSUMER_ACK_WAIT_TIME"),
//		DeliveryBackOff:          consumerBackOff,
//		ConsumerFetchBatch:       v.viper.GetInt("NATS_CONSUMER_FETCH_BATCH"),
//	}
//}
//
//// NatsConn returns connection-level config (not persisted stream/consumer state).
//// Only ReconnectWait is env-overridable — Name and MaxReconnects are fixed
//// operational defaults, not meant to vary per deployment.
//func (v *viperConfig) NatsConn() *nats.ConnConfig {
//	cc := nats.DefaultConnConfig()
//	// Zero from viper is indistinguishable from "unset" — only override when
//	// explicitly a positive duration, so an absent key keeps the code default.
//	if wait := v.viper.GetDuration("NATS_RECONNECT_WAIT"); wait > 0 {
//		cc.ReconnectWait = wait
//	}
//	return &cc
//}
//
//func (v *viperConfig) Infra() *InfraConfig {
//	return &InfraConfig{
//		HubID:   v.viper.GetUint64("HUB_ID"),
//		NatsURL: v.viper.GetString("NATS_URL"),
//	}
//}
//
//func (v *viperConfig) Router() *connection.Config {
//	return &connection.Config{
//		MaxConns:           v.viper.GetUint32("CONN_MANAGER_MAX_CONNS"),
//		TtlUnactiveSession: v.viper.GetDuration("CONN_MANAGER_TTL_UNACTIVE_CONN"),
//		TtlActiveConnIdle:  v.viper.GetDuration("CONN_MANAGER_TTL_ACTIVE_CONN_IDLE"),
//		TickerDuration:     v.viper.GetDuration("CONN_MANAGER_TICKER_DURATION"),
//	}
//}
