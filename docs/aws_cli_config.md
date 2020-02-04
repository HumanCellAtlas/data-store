# AWS CLI Configuration Guide

To configure the AWS CLI, we recommend you start with this AWS tutorial on getting started
with the CLI: [AWS CLI Tutorial](http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html).

## Configuring AWS Credentials

To use API keys for automated access to AWS, you must properly configure the `aws` utility.

Start by running `aws configure`, which will configure your AWS access credentials. These
keys should correspond to your main (top-level) account, or your principal service account
within the main account.

```
$ aws configure
AWS Access Key ID [None]: EXAMPLE-AWS-KEY
AWS Secret Access Key [None]: EXAMPLE-AWS-SECRET-KEY
Default region name [None]: us-east-1
Default output format [None]:
```

This will create a config file `~/.aws/config` with configuration variable values for your AWS account,
and a credentials file `~/.aws/credentials` with API keys for your account (this is sensitive information).

An example `~/.aws/config` file:

```
[profile my-red-profile]
role_arn = arn:aws:iam::123456789:role/red-developer
role_session_name = whoami@ucsc.edu
source_profile = red-creds
```

An example `~/.aws/credentials` file:

```
[red-creds]
aws_access_key_id = EXAMPLE-AWS-KEY
aws_secret_access_key = EXAMPLE-AWS-SECRET-KEY
```

Note that `source_profile` in the config file refers to the label given to the set of credentials
in the credentials file.

To test out the profile you just created, call the `aws` cli utility and specify the profile:

```
aws s3 ls --profile my-red-profile
```

## Enabling Multi-Factor Authentication

If you have multi-factor authentication enabled, you should specify your MFA device
in your AWS config file using the `mfa_serial` variable. Start by logging into the
AWS console, and finding the name of your MFA device. It will use the ARN number for
your top-level organization, and is typically something like

```
mfa_serial = arn:aws:iam::123456789:mfa/whoami@ucsc.edu
```

Once you have added this MFA device and enabled MFA, test it by running a simple
list-buckets action. You should be propmted for your 2FA code:

```
$ aws s3 ls
Enter MFA code for arn:aws:iam::123456789:mfa/whoami@ucsc.edu:

2020-01-17 15:03:22 mah-bukkit-1
2020-01-17 15:03:22 mah-bukkit-2
2020-01-17 15:03:22 mah-bukkit-3
...
```

## Using Assume Role with Multi-Factor Authentication

Once multi-factor authentication is enabled, every command run using the AWS CLI will
require a 2FA token. To have `aws` remember your identity across multiple commands,
use the [assume-role](https://github.com/remind101/assume-role) command line utility.
(See the Readme for installation instructions.)

Once you have installed `assume-role`, you can pass it the name of a profile, and it
will output temporary credentials:

```
$ assume-role my-red-profile
export AWS_ACCESS_KEY_ID="EXAMPLE-AWS-KEY"
export AWS_SECRET_ACCESS_KEY="EXAMPLE-AWS-SECRET-KEY"
export AWS_SESSION_TOKEN="AQ...1BQ=="
export AWS_SECURITY_TOKEN="AQ...1BQ=="
export ASSUMED_ROLE="my-red-profile"
```

To export these variables, pass the output of the `assume-role` command to `eval`:

```
$ eval $(assume-role my-red-profile)
```

This can be assigned to an alias, for example, with bash:

```
alias pgi='eval $(assume-role gi)'
```

Each time you call `assume-role` you will be asked to enter your 2FA token, and your
session will last approximately 30 minutes:

```
$ eval $(assume-role my-red-profile)
MFA: ******
```

Now you can run AWS commands without being prompted for a 2FA token each time.

## Using Multiple Profiles

If you are using the AWS CLI with multiple AWS accounts (profiles), extra care is required
to be able to switch between profiles. Running `aws configure` again will result in new entries
in the `config` and `credentials` files.

An example multi-profile `~/.aws/configure` file:

```
[profile my-red-profile]
role_arn = arn:aws:iam::123456789:role/red-developer
role_session_name = whoami@ucsc.edu
source_profile = red-creds

[profile my-green-profile]
role_arn = arn:aws:iam::123456789:role/green-developer
role_session_name = whoami@ucsc.edu
source_profile = green-creds
```

Now the credentials file will also have two sets of credentials.
An example multi-profile `~/.aws/credentials` file:

```
[red-creds]
aws_access_key_id = EXAMPLE-AWS-KEY-1
aws_secret_access_key = EXAMPLE-AWS-SECRET-KEY-1
[green-creds]
aws_access_key_id = EXAMPLE-AWS-KEY-2
aws_secret_access_key = EXAMPLE-AWS-SECRET-KEY-2
```

To verify that each profile is working, try running AWS commands
and specifying the profile.

```
aws s3 ls --profile my-red-profile
aws s3 ls --profile my-green-profile
```

**IMPORTANT:** Firefox has an internal bug that causes problems when accessing the
AWS web console using multiple AWS accounts. If you are having problems accessing the
AWS web console with multilpe accounts, try clearing the browser cache or restarting
the browser. Alternatively, try using the
[multi-account-containers](https://addons.mozilla.org/en-US/firefox/addon/multi-account-containers/)
Firefox addon.
