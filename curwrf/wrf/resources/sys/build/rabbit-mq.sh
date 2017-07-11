#!/usr/bin/env bash

rabbitmqctl add_user curw rabbitmq_password
rabbitmqctl add_vhost rabbitmq_virtual_host_name
rabbitmqctl set_user_tags rabbitmq_user_name rabbitmq_tag_name
rabbitmqctl set_permissions -p rabbitmq_virtual_host_name rabbitmq_user_name ".*" ".*" ".*"