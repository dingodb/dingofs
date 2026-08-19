# DingoFS quota 回归测试

测试脚本与 `test/regression-test/file` 使用相同的独立 case 目录和 `Checker` 风格。文件系统操作完成后，通过 MDS HTTP JSON 接口查询并验证 quota：

- `GetFsQuota`: `http://10.220.69.5:7801/MDSService/GetFsQuota`
- `GetDirQuota`: `http://10.220.69.5:7801/MDSService/GetDirQuota`

等价的手工查询命令：

```bash
curl http://10.220.69.5:7801/MDSService/GetFsQuota -d '{"fs_id":10000}'
curl http://10.220.69.5:7801/MDSService/GetDirQuota -d '{"fs_id":10000,"ino":10000000}'
```

## 运行

```bash
python3 run_all.py /mnt/dingofs/testdir
python3 run_all.py /mnt/dingofs/testdir --skip-slow
python3 run_all.py /mnt/dingofs/testdir --only 01,04
```

默认配置来自题目中的部署：

| 环境变量 | 默认值 | 说明 |
|---|---:|---|
| `DINGOFS_MDS_ADDR` | `10.220.69.5:7801` | MDS brpc HTTP 地址 |
| `DINGOFS_FS_ID` | `10000` | 文件系统 ID |
| `DINGOFS_ROOT_INO` | `10000000` | `GetDirQuota` 根目录 inode |
| `DINGOFS_TEST_DIR` | `./dingofs_test` | 挂载点/测试根目录 |
| `DINGOFS_QUOTA_WAIT` | `20` | 异步 quota 收敛等待秒数 |

`DINGOFS_ROOT_INO` 需要与 MDS 中的根 inode 一致；如果挂载部署使用了其他 inode，请显式设置。用例 04、05 会在测试目录下创建并删除独立的目录 quota，使用 `SetDirQuota`/`DeleteDirQuota` 做测试准备和清理，所有结果仍通过 `GetFsQuota`/`GetDirQuota` 验证。

## 用例覆盖

| 用例 | 覆盖内容 |
|---|---|
| 01 | 文件/目录创建删除、FS bytes/inodes、根目录 quota 的 FS fallback |
| 02 | 写入、同长度覆盖、truncate 缩小/扩大、稀疏文件逻辑长度 |
| 03 | hard link、symlink、rename、最终/非最终 unlink、目录 inode |
| 04 | 目录 quota 的 bytes/inodes 限制、EDQUOT、最近目录 quota 查询 |
| 05 | 嵌套目录 quota、祖先聚合、最近 quota、跨目录 rename |
| 06 | 重复读取、version、未知 inode fallback、未知 FS 错误 |

quota usage 更新是异步的，脚本会轮询接口直到预期值出现；测试失败时保留 case 目录，便于排查。配额超限在 POSIX/FUSE 层可能表现为 `EDQUOT` 或被客户端转换为 `ENOSPC`，两者都会被用例接受。
