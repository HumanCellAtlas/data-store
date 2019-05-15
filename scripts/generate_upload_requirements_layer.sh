#!/usr/bin/env bash
# This script is used to generate a dependency layer for the lambdas to utilize
# It's not really for direct usage, `make generate-dependencies` can be used
# but its part of the `make deploy` 

set -euo pipefail

account_id=$(aws sts get-caller-identity | jq -r ".Account" )
populated_dss_tf_backend=$(echo ${DSS_TERRAFORM_BACKEND_BUCKET_TEMPLATE} | sed 's/{account_id}/'"${account_id}"'/'  )

if [[ $(aws s3api get-bucket-location --bucket ${populated_dss_tf_backend} &>/dev/null ;echo $?) -ne 0 ]]
then
    echo "verify if bucket: ${populated_dss_tf_backend} exist"
    exit 1
fi



build_path="$DSS_HOME/dependencies/python/lib/python3.6/site-packages"
docker_download_path="/mnt/dependencies/python/lib/python3.6/site-packages"
dependency_dir="$DSS_HOME/dependencies"
aws_req_key=$DSS_DEPLOYMENT_STAGE/requirements.txt
aws_zip_key=$DSS_DEPLOYMENT_STAGE/dss-dependencies-$DSS_DEPLOYMENT_STAGE.zip
local_req=$DSS_HOME/requirements.txt
local_zip=$DSS_HOME/dss-dependencies-$DSS_DEPLOYMENT_STAGE.zip
layer_name=dss-dependencies-${DSS_DEPLOYMENT_STAGE}

function create_layer(){
    layer_version_arn=$(aws lambda publish-layer-version --layer-name $layer_name --content S3Bucket=${populated_dss_tf_backend},S3Key=${aws_zip_key}| jq -r .LayerVersionArn )
    echo "created layer/-version_arn: ${layer_version_arn}"
}

function check() {
     aws_req_hash=$(aws s3api head-object --bucket ${populated_dss_tf_backend} --key ${aws_req_key} | jq -r .ETag | xxd -r -p | base64)
     local_req_hash=$(openssl md5 -binary ${local_req} | base64)
if [[ $aws_req_hash != $local_req_hash ]]; then
    echo "checksum missmatch, uploading new ${aws_zip_key}"
    upload
else
    echo "requirements have not changed, no need to update"
    exit 0
fi
}

function upload() {

	# if this docker fails with `pull access denied` your credentials might be expired, perform a `docker logout`
	docker pull humancellatlas/dss-lambda-layer
	echo "downloading requirements to ${dependency_dir}"
	docker run -v "${DSS_HOME}":/mnt humancellatlas/dss-lambda-layer /bin/bash -c \
	"cd /mnt/ ; python3 -m pip -q --no-cache-dir install --target=${docker_download_path} -r requirements.txt"
	if [[ ! -d $dependency_dir ]]
	then
		echo "was not able to download directories from docker image"
		exit 1
	fi
     # target looks like site-package
    echo "compressing......"
    cd ${dependency_dir}
    zip -qq -r -o $local_zip *
    cd ..
    echo "uploading zip to ${populated_dss_tf_backend} for stage: ${DSS_DEPLOYMENT_STAGE}"
    aws s3 cp $local_zip s3://${populated_dss_tf_backend}/$aws_zip_key
    aws s3 cp $local_req s3://${populated_dss_tf_backend}/$aws_req_key
    echo "deleting ${dependency_dir} and ${local_zip}"
    rm -rf ${dependency_dir}
    rm -f ${local_zip}
    create_layer
}

if [[ $(aws s3api head-object --bucket ${populated_dss_tf_backend} --key $aws_zip_key &>/dev/null; echo $?) -eq 255 ]]
then
    echo "Could not locate $aws_zip_key in aws, starting upload"
    upload
    exit 0
else
    echo "$aws_zip_key found in aws"
    check
fi


# Onions have layers, Lambdas have layers
#                            ~
#                           /~
#                     \  \ /**
#                      \ ////
#                      // //
#                     // //
#                   ///&//
#                  / & /\ \
#                /  & .,,  \
#              /& %  :       \
#            /&  %   :  ;     `\
#           /&' &..%   !..    `.\
#          /&' : &''" !  ``. : `.\
#         /#' % :  "" * .   : : `.\
#        I# :& :  !"  *  `.  : ::  I
#        I &% : : !%.` '. . : : :  I
#        I && :%: .&.   . . : :  : I
#        I %&&&%%: WW. .%. : :     I
#         \&&&##%%%`W! & '  :   ,'/
#          \####ITO%% W &..'  #,'/
#            \W&&##%%&&&&### %./
#              \###j[\##//##}/
#                 ++///~~\//_
#                  \\ \ \ \  \_
#                  /  /    \
