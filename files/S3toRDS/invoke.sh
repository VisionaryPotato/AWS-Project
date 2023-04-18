echo "**** Invoking Lambda Function ****"
#aws lambda invoke --function-name s3ToRDS --cli-binary-format raw-in-base64-out --payload '{"key1": "value1", "key2": "value2", "key3": "value3"}' output.json --no-cli-pager
aws lambda invoke --function-name s3ToRDS --cli-binary-format raw-in-base64-out --payload file://payload.json  output.json --no-cli-pager
cat output.json | jq