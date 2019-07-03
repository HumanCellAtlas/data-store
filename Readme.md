# Fusillade

Fusillade (Federated User Identity Login & Access Decision Engine) is a service and library for managing user
authentication and authorization in federated services. Fusillade is built to be simple and to leverage well-known auth
protocols and standards together with existing global, scalable and supported IaaS APIs.

- The AuthN functionality in Fusillade consists of a login HTTPS endpoint that delegates user authentication to any
  configured [OpenID Connect](http://openid.net/connect/) compatible identity providers.
- The AuthZ part of Fusillade is
  an [ABAC](https://en.wikipedia.org/wiki/Attribute-based_access_control) [PDP](https://tools.ietf.org/html/rfc2904)
  (Policy Decision Point) API leveraging
  the [familiar syntax](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies.html) and reliable
  infrastructure of [AWS IAM](https://aws.amazon.com/iam/).

Together, these two subsystems provide an easy API for your application to answer the following questions:

- How do I instruct the user to log in?
- Who is the user performing this API request?
- Is this user authorized to perform action A on resource R?
- How do I delegate to the user an appropriately restricted ability to access cloud (IaaS) resources directly through
  IaaS (GCP, AWS) APIs?

To do this, your application should define an access control model consisting of the following:

- A list of trusted OIDC-compatible identity providers
- A naming schema for service actions (for example, `GetWidget`, `CreateFolder`, `DeleteAppointment`, `UpdateDocument`)
- A naming schema for resources in the following format: `arn:org-name:service-name:*:*:path/to/resource`
- A default policy assigned to new users, for example:
  ```json
  {
    "Statement": [
      {
        "Effect": "Allow",
        "Action": [
          "dss:CreateSubscription",
          "dss:UpdateSubscription",
          "dss:DeleteSubscription"
        ],
        "Resource": "arn:hca:dss:*:*:subscriptions/${user_id}/*"
      }
    ]
  }
  ```

## AWS Cloud Architecture
![AWS Cloud Architecture](https://www.lucidchart.com/publicSegments/view/b3470977-3924-4fb3-a07f-ce97be59dac1/image.png)
## Cloud Directory Structure
![Cloud Directory Structure](https://www.lucidchart.com/publicSegments/view/3f6f3cdc-7429-460c-b45f-33ae35d9e07c/image.png)


# Installing and configuring Fusillade

Create `oauth2_config.json` with the OIDC providers you'd like to use to 
authenticate users. This file is uploaded to AWS secrets manager using `make set_oauth2_config`. Use 
[`oauth2_config.example.json`](../master/oauth2_config.example.json) for help.
    

- pip install -r ./requirements-dev
- brew install jq
- brew install pandoc
- brew install moreutils
- brew install gettext
- brew link --force gettext 
- brew install terraform

- Setup [AWS CLI](https://github.com/aws/aws-cli) with the correct profile, default region, and output format.
- Local environment variables can be set in *environment.local* for convenience. If you use "source environment" and it 
  will set environment variables from *environment.local* after *environment* varaibles have been set, if you choose to 
  set them.
- Populate `FUS_ADMIN_EMAILS` with a list of admins to assigned upon creating the fusillade deployment. This
  is only used when fusillade is first deployed to create the first users. Afterwards this variable has no effect. If
  more admins are required assign a user the admin role.
- Environment variables can be set in `environment.local` for convenience.
- **Optionally** Before deploying fusillade you can modify the [default policies and roles](../blob/master/policies) 
 to suite your needs. The `default_admin_role.json` is policy attached to the fusillade_admin role created during 
 deployment. The `default_group_policy.json` is assigned to all new group when they are created. The 
 `default_user_role.json` is the role assigned to the group `default_user` which is created during deployment. All of 
 these policies and role can be modified after deployment using the fusillade API.

## Set secrets
Fusillade uses AWS Secret Store for its secrets. Use ./scripts/set_secrets to set the following secrets:

* **test_service_accounts** - contains google service accounts to test users access and admin access. See 
*./test_accounts_example.json* for the expected format.
* **oauth2_config** - contains the fields needed to proxy an OIDC provider. See *./oauth2_config.example.json* for 
expected format

# Using Fusillade as a Service

The following are created on deployment:
* `/role/fusillade_admin` - contains a policy based on `default_admin_role.json`
* `/role/default_user` - contains a policy based on `default_user_role.json`
* `/user/{FUS_ADMIN_EMAILS}` - a user for each admin in `FUS_ADMIN_EMAILS` with `/role/fusillade_admin` attached.
* `/user/public` -  a user for evaluating policies without an authenticated principle. Attach policies to this role 
 to limit what unauthenticated user can do.
* `/group/user_default` - a group assigned to all users upon creation. It has the `/role/default_user` attached. Add 
 new roles to this group to apply that role to all users.

All of these resources can be modified using the fusillade API after deployment.

## Adding Users to Roles
New admins can be assigned using the fusillade API and assigning the role of admin to a user.

# Using Fusillade as a library

# Using Fusillade as a proxy

# Bundling native cloud credentials

# Creating Custom Policy

Uses [AWS IAM Policy grammar](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_grammar.html)

### AWS

### GCP

# Service access control
To use Fusillade, your service must itself be authenticated and authorized. The access control model for this depends on
how you're using Fusillade.

### Library - Cooperative model

When using Fusillade as a library, your application's AWS IAM role is also your Fusillade access role. The library uses
AWS Cloud Directory and AWS IAM using your application's IAM credentials. (TODO: add links for ACD/IAM IAM and show
sample policy)

### Service - Enforced model

When using Fusillade as a service, your application is itself subject to an IAM policy governing its ability to read and
write permissions data. The Fusillade service administrator configures the Fusillade policy governing this in the
service configuration.

# How To
## Upgrade Clouddirectory Schema
1. Run `./scripts/upgrade` to check if your local schema matches the the published schema.
1. If the published schema does not match your local run `./scripts/upgrade --upgrade True`
## Links

* [Project home page (GitHub)](https://github.com/HumanCellAtlas/fusillade)
* [Documentation (Read the Docs)](https://fusillade.readthedocs.io/)
* [Package distribution (PyPI)](https://pypi.python.org/pypi/fusillade)

# Policies


## Special Fusillade Context Keys

In the same way AWS iam provides [context keys available to all services](https://docs.aws.amazon
.com/IAM/latest/UserGuide/reference_policies_condition-keys.html#condition-keys-globally-available), fusillade provides 
context keys that can be used in your policies.

- `fus:groups` - is a list of groups the current user belongs. This can be used to restrict permission based on 
  the group association
- `fus:roles` - is a list of roles the current user belongs. This can be used to restrict permission based on 
  the role association
- `fus:user_email` - is the email of the user. This can be used to restrict permission based on the users email.

### Bugs
Please report bugs, issues, feature requests, etc. on [GitHub](https://github.com/HumanCellAtlas/fusillade/issues).

### License
Licensed under the terms of the [MIT License](https://opensource.org/licenses/MIT).

[![Travis CI](https://travis-ci.org/HumanCellAtlas/fusillade.svg)](https://travis-ci.org/HumanCellAtlas/fusillade)
[![PyPI version](https://img.shields.io/pypi/v/fusillade.svg)](https://pypi.python.org/pypi/fusillade)
[![PyPI license](https://img.shields.io/pypi/l/fusillade.svg)](https://pypi.python.org/pypi/fusillade)
[![Read The Docs](https://readthedocs.org/projects/fusillade/badge/?version=latest)](https://pypi.python.org/pypi/fusillade)
[![Known Vulnerabilities](https://snyk.io/test/github/HumanCellAtlas/fusillade/badge.svg)](https://snyk.io/test/github/HumanCellAtlas/fusillade)
[![Build Status](https://travis-ci.com/HumanCellAtlas/fusillade.svg?branch=master)](https://travis-ci.com/HumanCellAtlas/fusillade)
[![codecov](https://codecov.io/gh/HumanCellAtlas/fusillade/branch/master/graph/badge.svg)](https://codecov.io/gh/HumanCellAtlas/fusillade)