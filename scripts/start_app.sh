#!/bin/bash
systemctl daemon-reload
systemctl enable externalmerchantapi
systemctl restart externalmerchantapi