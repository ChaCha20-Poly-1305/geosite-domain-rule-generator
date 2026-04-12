# What the hell is this?

A set of scripts to generate domain-based routing rulesets sourced from [@v2fly/domain-list-community](https://github.com/v2fly/domain-list-community). As of now, it generates rulesets for `category-ru` and `category-ip-geo-detect`, but it's possible to add other categories too (open an issue or PR if you have a request).

`*-domain.list` - DOMAIN-SUFFIX rulesets compatible with Mihomo and other tools that follow the same syntax (Shadowrocket, QuantumultX and others)

`*-unbound-domain-blocklist.conf` or `*-unbound-domain-whitelist.conf` - Unbound-compatible rulesets for DNS-based routing

The scripts automatically run and generate the rulesets every day at UTC 16:00.

## Disclaimer

This project is intended for research and educational purposes only.

Users must comply with all applicable laws and regulations in their respective jurisdictions when using this project. The author is not responsible for any misuse or illegal activities.

Do not use this project for any unlawful purposes.

This project does not provide, promote, or imply any specific usage scenarios. Users are solely responsible for how they use this project.

This repository provides configuration templates only. All values and endpoints must be provided by the user.

The included rules are generic network routing examples and do not target or guarantee access to any specific services.
