zip -r my-deployment-package.zip *
echo "**** Uploading Function To AWS ****"
aws lambda update-function-code --function-name GoogleSheetsToS3 --zip-file fileb://my-deployment-package.zip --no-cli-pager

sleep 3
./invoke.sh

