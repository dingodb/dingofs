Deployment Precheck
===

Precheck Overview
---

The precheck module is used to detect factors that may cause deployment failure in advance, thereby improving the success rate of user deployments.


> 💡 **Note:**
>
> We run the precheck function by default when executing deployment. Users can also skip the precheck during deployment by using the `-k` option,
> but we strongly advise against doing so.

Executing Precheck
---

```shell
$ dingoadm precheck
```

Currently, the following 6 pre-check items are executed by default: `topology`, `SSH`, `permissions`, `kernel`, `network`, `time`, and `services`. Users can skip specific pre-check items using the `skip` option after ensuring there are no issues:

```shell
$ dingoadm precheck --skip <item>
```

### Pre-check item description

| Check item | Skip option   | Description                                          |
| :---   | :---       | :---                                          |
| Topology   | topology   | Check the validity of the cluster topology                          |
| SSH    | ssh        | Check SSH connectivity                             |
| Permissions   | permission | Check the current user's permissions to execute docker, create directories, etc.       |
| Kernel   | kernel     | Check whether the kernel version and kernel modules meet the requirements            |
| Network   | network    | Check network connectivity, firewall, etc.                      |
| Time   | date       | Check whether the time difference between hosts is too large                  |
| Service | service | Check the number of services, chunkfile pool, S3 configuration validity, etc. |

> 💡 **Reminder:**
>
> When pre-checks fail, we strongly recommend that users troubleshoot the issue step by step based on the reported [error code][errno] and the provided solutions,
> and ultimately pass all pre-checks. We strongly advise against skipping the check item when a pre-check fails,
> as this may leave potential issues for subsequent actual deployments, leading to deployment failures. We ensure that every pre-check item is essential for deployment. Please ensure all pre-check items are passed before deployment.

[errno]: ../errno.md
