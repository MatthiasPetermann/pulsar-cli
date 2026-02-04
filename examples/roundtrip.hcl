global {
  broker_url        = "pulsar://localhost:6650"
  parallelism       = 4
  message_count     = 100
  timeout           = "45s"
  progress_interval = "2s"
}

topic "persistent://public/default/rt-persistent" {}
topic "non-persistent://public/default/rt-nonpersistent" {}

scenario "single-producer-single-consumer" {
  producers         = 1
  consumers         = 1
  subscription_type = "exclusive"
}

scenario "multi-producer-single-consumer" {
  producers         = 3
  consumers         = 1
  subscription_type = "shared"
}

scenario "single-producer-multi-consumer-shared" {
  producers         = 1
  consumers         = 3
  subscription_type = "shared"
}

scenario "single-producer-multi-consumer-failover" {
  producers         = 1
  consumers         = 3
  subscription_type = "failover"
}

scenario "single-producer-multi-consumer-exclusive" {
  producers         = 1
  consumers         = 3
  subscription_type = "exclusive"
}

scenario "multi-producer-multi-consumer-shared" {
  producers         = 3
  consumers         = 3
  subscription_type = "shared"
}

scenario "multi-producer-multi-consumer-failover" {
  producers         = 3
  consumers         = 3
  subscription_type = "failover"
}

scenario "multi-producer-multi-consumer-exclusive" {
  producers         = 3
  consumers         = 3
  subscription_type = "exclusive"
}
