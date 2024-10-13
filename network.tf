resource "google_compute_subnetwork" "default" {
  name          = "default"
  ip_cidr_range = "10.142.0.0/20"
  region        = var.region
  network       = "default"

  private_ip_google_access = true
}