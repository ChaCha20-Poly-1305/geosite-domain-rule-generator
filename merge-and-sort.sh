#!/usr/bin/env bash
# Removes leftovers from extraction, merges two domain lists (one extracted from GeoSite and one I compiled myself) because I am too lazy to contribute my findings to GeoSite list yet.
rm dlc.dat dlc-includes.json
sort -u raw/domain_list_init.txt raw/domain_list_extracted.txt > raw/domain_list.txt
rm raw/domain_list_extracted.txt
cp raw/domain_list.txt raw/domain_list_init.txt
