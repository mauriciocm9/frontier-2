variable "region" {
  type    = string
  default = "us-east-1"
}
variable "name" {
  type    = string
  default = "frontier"
}
variable "deploy_services" {
  description = "Enable after pushing the image to ECR."
  type        = bool
  default     = false
}
variable "image_tag" {
  type    = string
  default = "bootstrap"
}
variable "certificate_arn" {
  description = "Optional ACM certificate in the deployment region; enables HTTPS and redirects HTTP."
  type        = string
  default     = ""
}
variable "database_instance_class" {
  type    = string
  default = "db.t4g.micro"
}
variable "database_multi_az" {
  type    = bool
  default = false
}
