#!/bin/bash
cat > /lib/systemd/system/externalmerchantapi.service <<- "EOF"
[Unit]
Description=ExternalMerchant.API
After=cloud-final.service

[Service]
EnvironmentFile=/var/www/env.conf
Type=simple
User=ubuntu
ExecStart=/usr/bin/node /var/www/externalmerchantapi/dist/main.js
Restart=on-failure

[Install]
WantedBy=cloud-init.target
EOF