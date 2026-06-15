ydb
--endpoint grpcs://ydb.serverless.yandexcloud.net:2135
--database /ru-central1/b1ge5e6pdk5pbhmv8o0g/etnb1gf4utd6n4ljvrqo
--sa-key-file authorized_key.json
import file csv
--path mental_health
--delimiter ","
--skip-rows 1
mh_transformed.csv