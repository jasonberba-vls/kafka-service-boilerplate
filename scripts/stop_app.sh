#!/bin/bash
systemctl is-active externalmerchantapi && systemctl stop externalmerchantapi
echo externalmerchantapi stopped