data_dir = "/opt/nomad/data"
bind_addr = "0.0.0.0"

# Enable the client
client {
  enabled = true
  options {
    "driver.raw_exec.enable" = "1"
    "docker.privileged.enabled" = "true"
  }
  server_join {
    retry_join = ["RETRY_JOIN"]
    retry_max = 3
    retry_interval = "15s"
  }
}



vault {
  enabled = true
  address = "vault.service.consul"
}
