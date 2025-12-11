#!/bin/sh

nohup python conv_android_adjust.py -i af_passback_android.csv -a 2m1meym0k6tc_android_android_id.csv -g 2m1meym0k6tc_android_gps_adid.csv &
nohup python conv_ios_adjust.py -i af_passback_ios.csv -a 2m1meym0k6tc_ios_idfa.csv -v 2m1meym0k6tc_ios_idfa.csv &