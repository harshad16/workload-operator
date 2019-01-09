workload-operator
-----------------

Operator for managing jobs in project Thoth.

As jobs can lead to back-offs due to resource quotas in assigned namespace,
this operator ensures jobs do not starve, are kept alive and are executed in
first-come-first-served fashion.

The operator uses config maps (see Thoth's `common repository
<https://github.com/thoth-station/common>`_) for keeping track of work that
needs to be done. There is maintained a queue of jobs pending for execution -
once there are available resources in the desired namespace, workload-operator
schedules job based on its definition in the config map.

Deployment
==========

This operator is part of `Thoth core deployment
<https://github.com/thoth-station/core>`_. It accepts a parameter that
describes namespace in which it should watch for resources, config maps and
subsequently schedule jobs.

