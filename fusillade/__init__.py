from . import clouddirectory
'''
system config:
- directory schema

service config:
- provisioning policy

Q: how to model user-org relationships? e.g. member vs. admin
A: each org should be represented by the org node and sub-nodes (members, admins, ...)

Q: how do i enumerate all resources that a user has access to?

    org  project->policy[resource_id]
      |  |
members  members->policy[resource_id(parent?)]
      |  |
      user



'''
