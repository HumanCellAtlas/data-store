# HCA DSS: The Human Cell Atlas Data Storage System

This repository contains design specs and prototypes for the replicated data storage system (aka the "blue box") of
the [Human Cell Atlas](https://www.humancellatlas.org/). We
use [this Google Drive folder](https://drive.google.com/open?id=0B-_4IWxXwazQbWE5YmtqUWx3RVE) for design docs and
meeting notes, and [this Zenhub board](https://app.zenhub.com/workspace/o/humancellatlas/data-store) to track our GitHub work.

#### About this prototype

The prototype in this repository uses [Swagger](http://swagger.io/) to specify the API in [dss-api.yml](dss-api.yml),
and [Connexion](https://github.com/zalando/connexion) to map the API specification to its implementation in Python.

You can use the
[Swagger Editor](http://editor.swagger.io/#/?import=https://raw.githubusercontent.com/HumanCellAtlas/data-store/master/dss-api.yml) to
review and edit the prototype API specification. When the prototype app is running, the Swagger spec is also available
at `/v1/swagger.json`.

The prototype is deployed continuously from the `master` branch, with the resulting producer and consumer API available
at https://dss.integration.data.humancellatlas.org/.

#### Installing dependencies for development on the prototype

The HCA DSS prototype development environment requires Python 3.6+ to run. Run `pip install -r requirements-dev.txt` in
this directory.

#### Installing dependencies for the prototype

The HCA DSS prototype requires Python 3.6+ to run. Run `pip install -r requirements.txt` in this directory.

The tests require certain node.js packages. They must be installed using `npm`, the node.js package manager. Install 
[npm](https://www.npmjs.com/get-npm) (on macOS `brew install npm`) and run `npm install` from the project root.

#### Pull sample data bundles

Tests also use data from the data-bundle-examples subrepository. Run: `git submodule update --init`


#### Configuring cloud-specific access credentials

##### AWS

1. Follow the instructions in http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html to get the
   `aws` command line utility.

2. To configure your account credentials and named profiles for the `aws` cli, see
   https://docs.aws.amazon.com/cli/latest/userguide/cli-config-files.html and
   https://docs.aws.amazon.com/cli/latest/userguide/cli-multiple-profiles.html

##### GCP

1.  Follow the instructions in https://cloud.google.com/sdk/downloads to get the `gcloud` command line utility.

2.  Run `gcloud auth login` to authorize the gcloud cli.

#### Terraform

Some cloud assets are managed by Terraform, inlcuding the storage buckets and Elasticsearch domain.

1.  Follow the instructions in https://www.terraform.io/intro/getting-started/install.html to get the
    `terraform` command line utility.

2.  Run `configure.py` to prepare the deployment.

3.  Infrastructure deployment definiations may be further customized by editing the terraform scripts in
    'deployment/active' subdirectories.

Now you may deploy the cloud assets with
    make deploy-infra

##### GCP Application Secrets

3.  Generate OAuth application secrets to be used for your instance: 

    1) Go to https://console.developers.google.com/apis/credentials (you may have to select Organization and Project
    again)
    
	2) Click *Create Credentials* and select *OAuth client*
    
	3) For *Application type* choose *Other*  
    
	4) Under application name, use `hca-dss-` followed by the stage name i.e., the value of DSS_DEPLOYMENT_STAGE. This
    is a convention only and carries no technical significance.
    
	5) Click *Create*, don't worry about noting the client ID and secret, click *OK*
    
	6) Click the edit icon for the new credentials and click *Download JSON*
    
	7) Place the downloaded JSON file into active stage root as `deployment/active/application_secrets.json`

#### Environment Variables

Environment variables are required for test and deployment. The required environment variables and their default values
are in the file `environment`. To customize the values of these environment variables, run `configure.py`.

Run `source environment`  now and whenever `configure.py` is executed.

##### Azure

1. Set the environment variables `AZURE_STORAGE_ACCOUNT_NAME` and `AZURE_STORAGE_ACCOUNT_KEY`.

#### Running the DSS API locally

Run `./dss-api` in the top-level `data-store` directory.

#### Check and install software required to test and deploy

Check that software packages required to test and deploy are available, and install them if necessary.

Run: `make --dry-run`

#### Populate test data

To run the tests, test fixture data must be set up using the following command. **This command will completely empty the
given buckets** before populating them with test fixture data, please ensure the correct bucket names are provided.

    tests/fixtures/populate.py --s3-bucket $DSS_S3_BUCKET_TEST_FIXTURES --gs-bucket $DSS_GS_BUCKET_TEST_FIXTURES


#### Running tests

Set the environment variable `DSS_TEST_ES_PATH` to the path of the `elasticsearch` binary on your machine. Then to
perform the data store tests:

Run `make test` in the top-level `data-store` directory.

#### Deployment

Assuming the tests have passed above, the next step is to manually deploy. See the section below for information on
CI/CD with Travis if continuous deployment is your goal.

Now deploy using make:

    make deploy

If successful, you should be able to see the Swagger API documentation at:

    https://<domain_name>

And you should be able to list bundles like this:

    curl -X GET "https://<domain_name>/v1/bundles" -H  "accept: application/json"

#### Configure email notifications

Some daemons (dss-checkout-sfn for example) use Amazon SES to send emails. You must set `DSS_NOTIFICATION_SENDER` to 
your email address and then verify that address using the SES Console enabling SES to send notification emails from it. 

#### Using the HCA Data Store CLI Client

Now that you have deployed the data store, the next step is to use the HCA Data Store CLI to upload and download data to
the system. See [data-store-cli](https://github.com/HumanCellAtlas/data-store-cli) for installation instructions. The
client requires you change `hca/api_spec.json` to point to the correct host, schemes, and, possibly, basePath. Examples
of CLI use:

    # list bundles
    hca get-bundles
    # upload full bundle
    hca upload --replica aws --staging-bucket staging_bucket_name data-bundle-examples/smartseq2/paired_ends

#### Checking Indexing

Now that you've uploaded data, the next step is to confirm the indexing is working properly and you can query the
indexed metadata.

    hca post-search --query '
    {
        "query": {
            "bool": {
                "must": [{
                    "match": {
                        "files.sample_json.donor.species": "Homo sapiens"
                    }
                }, {
                    "match": {
                        "files.assay_json.single_cell.method": "Fluidigm C1"
                    }
                }, {
                    "match": {
                        "files.sample_json.ncbi_biosample": "SAMN04303778"
                    }
                }]
            }
        }
    }'

#### CI/CD with Travis CI

We use [Travis CI](https://travis-ci.org/HumanCellAtlas/data-store) for continuous integration testing and
deployment. When `make test` succeeds, Travis CI deploys the application into the `dev` stage on AWS for every commit
that goes on the master branch. This behavior is defined in the `deploy` section of `.travis.yml`.

#### Authorizing Travis CI to deploy

Encrypted environment variables give Travis CI the AWS credentials needed to run the tests and deploy the app. Run
`scripts/authorize_aws_deploy.sh IAM-PRINCIPAL-TYPE IAM-PRINCIPAL-NAME` (e.g. `authorize_aws_deploy.sh group
travis-ci`) to give that principal the permissions needed to deploy the app. Because a group policy has a higher size
limit (5,120 characters) than a user policy (2,048 characters), it is advisable to apply this to a group and add the
principal to that group. Because this is a limited set of permissions, it does not have write access to IAM. To set up
the IAM policies for resources in your account that the app will use, run `make deploy` using privileged account
credentials once from your workstation. After this is done, Travis CI will be able to deploy on its own. You must
repeat the `make deploy` step from a privileged account any time you change the IAM policies templates in
`iam/policy-templates/`.

#### Authorizing the event relay

Environment variables provide the AWS credentials needed to relay events originating from supported cloud platforms
outside of AWS. Run `scripts/create_aws_event_relay_user.py` to create an AWS IAM user with the appropriate
restricted access policy. The access key id and secret access key are created and populated into
AWS SSM parameters by running the script `scripts/set_event_relay_parameters.py`.

#### Managing dependencies

The direct runtime dependencies of this project are defined in `requirements.txt.in`. Direct development dependencies
are defined in `requirements-dev.txt.in`. All dependencies, direct and transitive, are defined in the corresponding
`requirements.txt` and `requirements-dev.txt` files. The latter two can be generated using `make requirements.txt` or
`make requirements-dev.txt` respectively. Modifications to any of these four files need to be committed. This process is
aimed at making dependency handling more deterministic without accumulating the upgrade debt that would be incurred by
simply pinning all direct and transitive dependencies.  Avoid being overly restrictive when constraining the allowed
version range of direct dependencies in -`requirements.txt.in` and `requirements-dev.txt.in`

If you need to modify or add a direct runtime dependency declaration, follow the steps below:

1) Make sure there are no pending changes to `requirements.txt` or `requirements-dev.txt`.
2) Make the desired change to `requirements.txt.in` or `requirements-dev.txt.in`
3) Run `make requirements.txt`.  Run `make requirements-dev.txt` if you have modified `requirements-dev.txt.in`.
4) Visually check the changes to `requirements.txt` and `requirements-dev.txt`.
5) Commit them with a message like `Bumping dependencies`.

You now have two commits, one that catches up with updates to transitive dependencies, and one that tracks your explict
change to a direct dependency. This process applies to development dependencies as well, except for
`requirements-dev.txt` and `requirements-dev.txt.in` respectively.

If you wish to re-pin all the dependencies, run `make refresh_all_requirements`.  It is advisable to do a full
test-deploy-test cycle after this (the test after the deploy is required to test the lambdas).

#### Logging conventions

1.  Always use a module-level logger, call it `logger` and initialize it as follows:

    ```python
    import logging
    logger = logging.getLogger(__name__)
    ```

2.  Do not configure logging at module scope. It should be possible to import any module without side-effects on 
    logging. The `dss.logging` module contains functions that configure logging for this application, its Lambda 
    functions and unit tests.
    
3.  When logging a message, pass either
 
    * an f-string as the first and only positional argument or
    
    * a %-string as the first argument and substitution values as subsequent arguments. Do not mix the two string 
      interpolation methods. If you mix them, any percent sign in a substituted value will raise an exception.
    
    ```python
    # In other words, use
    logger.info(f"Foo is {foo} and bar is {bar}")
    # or
    logger.info("Foo is %s and bar is %s", foo, bar)
    # but not
    logger.info(f"Foo is {foo} and bar is %s", bar)
    # Keyword arguments can be used safely in conjunction with f-strings: 
    logger.info(f"Foo is {foo}", exc_info=True)
    ```
    
4.  To enable verbose logging by application code, set the environment variable `DSS_DEBUG` to `1`. To enable verbose 
    logging by dependencies set `DSS_DEBUG` to `2`. To disable verbose logging unset `DSS_DEBUG` or set it to `0`.

5.  To assert in tests that certain messages were logged, use the `dss` logger or one of its children 
    
    ```python
    dss_logger = logging.getLogger('dss')
    with self.assertLogs(dss_logger) as log_monitor:
        # do stuff
    # or
    import dss
    with self.assertLogs(dss.logger) as log_monitor:
        # do stuff     
    ```


[![](https://img.shields.io/badge/slack-%23data--store-557EBF.svg)](https://humancellatlas.slack.com/messages/data-store/)
[![Build Status](https://travis-ci.org/HumanCellAtlas/data-store.svg?branch=master)](https://travis-ci.org/HumanCellAtlas/data-store)
[![codecov](https://codecov.io/gh/HumanCellAtlas/data-store/branch/master/graph/badge.svg)](https://codecov.io/gh/HumanCellAtlas/data-store)
