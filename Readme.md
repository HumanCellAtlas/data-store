Fusillade
=========

Fusillade (Federated User Identity Login & Access Decision Engine) is a service and library for managing user
authentication and authorization in federated services. Fusillade is built to be simple and to leverage well-known auth
protocols and standards toegther with existing global, scalable and supported IaaS APIs.

- The AuthN functionality in Fusillade consists of a login endpoint that delegates user authentication to any configured
  [OpenID Connect](http://openid.net/connect/) compatible identity providers.
- The AuthZ part of Fusillade is an [ABAC](https://en.wikipedia.org/wiki/Attribute-based_access_control) system
  leveraging the familiar syntax and reliable infrastructure of [AWS IAM](https://aws.amazon.com/iam/).

Together, these two subsystems provide an easy API for your application to answer the following questions:

- How do I instruct the user to log in?
- Who is the user performing this API request?
- Is this user authorized to perform action A on resource R?
- How do I delegate to the user an appropriately restricted ability to access cloud (IaaS) resources directly through
  IaaS (GCP, AWS) APIs?

To do this, your application should define an access control model consisting of the following:

- A list of trusted OIDC-compatible identity providers
- A naming schema for actions (for example, `GetWidget`, `CreateFolder`, `DeleteAppointment`, `UpdateDocument`)
- A naming schema for resources in the following format: `arn:org-name:service-name:*:*:path/to/resource`
- A default policy assigned to new users, for example:
  ```json
  {
    "Statement": [
      {
        "Effect": "Allow",
        "Action": [
          "dss:*",
        ],
        "Resource": "arn:hca:dss:*:*:subscriptions/FIXME/*"
      }
    ]
  }
  ```

Installing and configuring Fusillade
------------------------------------

Using Fusillade as a service
----------------------------

Using Fusillade as a library
----------------------------

Using Fusillade as a proxy
--------------------------

Bundling native cloud credentials
---------------------------------

### AWS

### GCP

Service access control
----------------------

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

## Links

* [Project home page (GitHub)](https://github.com/HumanCellAtlas/fusilllade)
* [Documentation (Read the Docs)](https://fusillade.readthedocs.io/)
* [Package distribution (PyPI)](https://pypi.python.org/pypi/fusillade)

### Bugs
Please report bugs, issues, feature requests, etc. on [GitHub](https://github.com/github.com/HumanCellAtlas/issues).

### License
Licensed under the terms of the [MIT License](https://opensource.org/licenses/MIT).

[![Travis CI](https://travis-ci.org/HumanCellAtlas/fusilllade.svg)](https://travis-ci.org/HumanCellAtlas/fusilllade)
[![PyPI version](https://img.shields.io/pypi/v/fusillade.svg)](https://pypi.python.org/pypi/fusillade)
[![PyPI license](https://img.shields.io/pypi/l/fusillade.svg)](https://pypi.python.org/pypi/fusillade)
[![Read The Docs](https://readthedocs.org/projects/fusillade/badge/?version=latest)](https://pypi.python.org/pypi/fusillade)
