# Data-Store Security

This document provides guidelines on vulnerabilities in the data-store
and how to report them.

## Vulnerabilities in Data-Store

The data-store is a large and complex system built on cloud architecture in
AWS and GCP, whose vulnerabilities we are also subject to.  It also depends 
on a large set of third party libraries (e.g., `google-auth`, `Jinja2`, and
[more](https://github.com/HumanCellAtlas/data-store/blob/master/requirements.txt) ),
though we attempt to mitigate this with Snyk.

It is possible that the data-store or its dependent libraries contain
vulnerabilities that would allow triggering unexpected or dangerous behavior
with specially crafted inputs.

### What is a vulnerability?

Given the data-store's many components, a vulnerability could occur at 
[ingest](https://github.com/HumanCellAtlas/upload-service), 
the [data-store](https://github.com/humancellatlas/data-store) itself, 
the [data-browser](https://github.com/HumanCellAtlas/data-browser), 
the [data-portal](https://github.com/HumanCellAtlas/data-portal).
or the [dcp-cli](https://github.com/humancellatlas/dcp-cli).

Public read access to files in the data-store is intended and does not require 
authentication, however, any method that circumvents the normal auth process
for other endpoints **is** a vulnerability (check the swagger for endpoints 
that do or do not require auth: https://dss.data.humancellatlas.org/ ) and
should be reported.

One of the most critical parts of any system is input handling. If malicious
input can trigger side effects or incorrect behavior, this is a bug, and likely
a vulnerability.

### Reporting vulnerabilities

Please email reports about any security related issues you find to
`security-leads@data.humancellatlas.org`.  This mail is delivered to a small 
security team.  Your email will be acknowledged within one business day, 
and you'll receive a more detailed response to your email within 7 days 
indicating the next steps in handling your report.

Please use a descriptive subject line for your report email.  After the initial
reply to your report, the security team will endeavor to keep you informed of
the progress being made towards a fix and announcement.

In addition, please include the following information along with your report:

* Your name and affiliation (if any).
* A description of the technical details of the vulnerabilities. It is very
  important to let us know how we can reproduce your findings.
* An explanation who can exploit this vulnerability, and what they gain when
  doing so -- write an attack scenario.  This will help us evaluate your report
  quickly, especially if the issue is complex.
* Whether this vulnerability public or known to third parties. If it is, please
  provide details.

If you believe that an existing (public) issue is security-related, please send
an email to `security-leads@data.humancellatlas.org`.  The email should include 
the issue ID and a short description of why it should be handled according to 
this security policy.

Past security advisories will be listed below.  We credit reporters for identifying
security issues, although we keep your name confidential if you request it.
