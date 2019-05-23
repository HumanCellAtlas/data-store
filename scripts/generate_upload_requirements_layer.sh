#!/usr/bin/env bash
# This script is used to generate a dependency layer for the lambdas to utilize
# It's not really for direct usage, `make generate-dependencies` can be used
# but its part of the `make deploy` 

set -euo pipefail
shopt -s nullglob;

account_id=$(aws sts get-caller-identity | jq -r ".Account" )
populated_dss_tf_backend=$(echo ${DSS_TERRAFORM_BACKEND_BUCKET_TEMPLATE} | sed 's/{account_id}/'"${account_id}"'/'  )

if [[ $(aws s3api get-bucket-location --bucket ${populated_dss_tf_backend} &>/dev/null ;echo $?) -ne 0 ]]
then
    echo "verify if bucket: ${populated_dss_tf_backend} exist"
    exit 1
fi

build_path="$DSS_HOME/dependencies/python/lib/python3.6/site-packages"
dependency_dir="$DSS_HOME/dependencies"
aws_req_key=$DSS_DEPLOYMENT_STAGE/requirements.txt
aws_zip_key=$DSS_DEPLOYMENT_STAGE/dss-dependencies-$DSS_DEPLOYMENT_STAGE.zip
local_temp_chalice=${DSS_HOME}/temp_chalice
local_req=$DSS_HOME/requirements.txt
local_zip=$DSS_HOME/dss-dependencies-$DSS_DEPLOYMENT_STAGE.zip
layer_name=dss-dependencies-${DSS_DEPLOYMENT_STAGE}

function create_layer(){
    layer_version_arn=$(aws lambda publish-layer-version --layer-name $layer_name --content S3Bucket=${populated_dss_tf_backend},S3Key=${aws_zip_key}| jq -r .LayerVersionArn )
    echo "created layer/-version_arn: ${layer_version_arn}"
}


function build_clean_zip(){
	mkdir -p $build_path
	unzip -q -o -d $build_path deployment.zip
	cd $dependency_dir
	zip -qq -r -o $local_zip ./
    cd ..
}

function build_chalice(){
	# utilize challice to build out dependencies
	# chalice requires a a configured chalice application to package.
	# copy chalice folder TODO verify that in process this folder will be clean....
	cp -R ${DSS_HOME}/chalice ${local_temp_chalice}
	mkdir ${local_temp_chalice}/vendor
	cp -R ${DSS_HOME}/vendor.in ${local_temp_chalice}; ln -s ${DSS_HOME}/requirements.txt ${local_temp_chalice}/requirements.txt;
	cd ${local_temp_chalice}
	shopt -s nullglob;for wheel in vendor.in/*/*.whl; do unzip -q -o -d vendor $wheel; done
	chalice package .
	echo "removing non-dependencies from zip"
	zip -d deployment.zip chalicelib/\*
	zip -d deployment.zip app.py
}

function upload() {
	if [[ ! -f $local_zip ]]
	then
		echo "unable to locate zip file at: $local_zip"
		exit 1
	fi
    echo "uploading zip to ${populated_dss_tf_backend} for stage: ${DSS_DEPLOYMENT_STAGE}"
    aws s3 cp $local_zip s3://${populated_dss_tf_backend}/$aws_zip_key
    aws s3 cp $local_req s3://${populated_dss_tf_backend}/$aws_req_key
    create_layer
}

function clean (){
	echo "deleting $1"
    rm -rf $1
}

function clean_all(){
	clean $dependency_dir
	clean $local_temp_chalice
	clean $local_zip
}

function build_upload_clean(){
	build_chalice
    build_clean_zip
    upload
	clean_all
}

function check() {
    aws_req_hash=$(aws s3api head-object --bucket ${populated_dss_tf_backend} --key ${aws_req_key} | jq -r .ETag | xxd -r -p | base64)
    local_req_hash=$(openssl md5 -binary ${local_req} | base64)
	if [[ $aws_req_hash != $local_req_hash ]]; then
    	echo "checksum missmatch, uploading new ${aws_zip_key}"
    	build_upload_clean
	else
    	echo "requirements have not changed, no need to update"
    	exit 0
	fi
}

optspec=":hbcd"
while getopts "$optspec" optchar; do
    case "${optchar}" in
        b)
        	clean_all
            build_chalice
            build_clean_zip
            echo "created: $local_zip"
            clean $dependency_dir
            clean $local_temp_chalice
            exit 0
            ;;
       	c)
			clean_all
       		exit 0
       		;;
       	d)
       		upload
       		exit 0
       		;;
		h)
			echo "usage: $0 [-bd]" >&2
			echo "Description:"
			echo " -b | builds out the zip package"
			echo " -d | upload from $local_zip and requirements.txt to s3://${populated_dss_tf_backend}, registers new layer"
			echo " No Args | checks if requirements file has change from whats in s3, updates if needed"
			exit 2
			;;
esac
done

# if no arguments then check and run
if [[ $(aws s3api head-object --bucket ${populated_dss_tf_backend} --key $aws_zip_key &>/dev/null; echo $?) -eq 255 ]]
then
    echo "Could not locate $aws_zip_key in aws, starting upload"
	build_upload_clean
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
