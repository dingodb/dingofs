# MDS FsStat SliceRef 页面需求

日期：2026-05-11
范围：Lightweight

## 背景

`src/mds/service/fsstat_service.cc` 中的 `FsStatServiceImpl` 已实现 details / file session / quota / delfiles / delslices / oplog 等子页面。
当前缺少展示 `SliceRef`（slice 引用计数）信息的页面，运维定位 reflink/CopyFileRange 共享 slice 的引用关系不够直观。

## 目标

在 MDS 内置的 FsStat 页面中新增 SliceRef 列表页，并在 FS 表格的 Navigation 列添加 `slicerefs` 跳转入口，方便人工排查 slice 引用情况。

## 非目标

- 不新增 RPC 接口，不改 proto。
- 不实现写操作（不支持页面修改/删除 SliceRef）。
- 不做单 SliceId 详情子页（一行已能完整呈现 proto 字段）。
- 不做翻页 / 排序后端逻辑（沿用现有 `class="gridtable sortable"` 前端排序即可）。

## 数据模型

`pb::mds::SliceRef`（`proto/dingofs/mds.proto:227`）：

| 字段 | 含义 |
| --- | --- |
| `id` | slice id（全局唯一） |
| `size` | slice 大小（byte） |
| `ref_count` | 引用计数 |
| `inos` | 引用该 slice 的 inode 列表 |

底层存储为全局 key（`MetaCodec::EncodeSliceRefKey(slice_id)`，无 fs_id）。已有 `ScanSliceRefOperation`（`src/mds/filesystem/store_operation.h:1024`）可全量扫描。

## 用户故事

作为 DingoFS 运维：

- 我打开 MDS 的 `/FsStatService` 主页。
- 在 FS 表格的 Navigation 列任一行点击 **slicerefs**，新标签页打开 `/FsStatService/slicerefs`。
- 页面以表格形式列出全集群所有 SliceRef 记录。

## 页面规范

### 入口

- 在 `RenderMainPage` 的 `render_navigation_func` 中，`oplog` 之后追加：
  - 链接文本：`slicerefs`
  - 跳转路径：`FsStatService/slicerefs`（不带 fs_id，全局）
  - 与其它链接保持 `<br>` 分隔与 `target="_blank"` 风格

### `/FsStatService/slicerefs` 页面

- 标题：`Slice Reference`
- 子标题：`<h3>SliceRef [{count}]</h3>`，`count` 为本次扫描到的 SliceRef 总数
- 表格列：
  1. `SliceId`
  2. `Size(byte)`
  3. `RefCount`
  4. `Inos`：以逗号拼接 `slice_ref.inos()`，空时显示 `-`
- 沿用 `RenderHead("dingofs sliceref")`、`gridtable sortable` 样式与既有 delfiles/delslices 页面 DOM 结构。

### 路由处理

在 `default_method` 已有 `params` 分发分支处增加：

```
} else if (params.size() == 1 && params[0] == "slicerefs") {
  // 全局：扫描所有 SliceRef 并渲染
}
```

错误路径与既有页面保持一致：扫描失败时输出 `Get sliceref fail, {status}.`。

## 实现思路（供 plan 阶段细化）

- 在 `FileSystemSet` 上新增 `Status GetSliceRefs(std::vector<SliceRefEntry>& slice_refs)`，内部走 `ScanSliceRefOperation`，`ReadCommitted` 隔离级别，与 `GetDelSlices` 风格对齐。
- 在 `fsstat_service.cc` 新增 `RenderSliceRefPage`，与 `RenderDelslicePage` 同节内排版。
- 路由在现有 `delslices` / `oplog` 分支后追加。

## 验收标准

- 启动 MDS，访问 `/FsStatService`：
  - FS 表格 Navigation 列每行均包含 `slicerefs` 链接。
- 点击 `slicerefs`：
  - 在新标签页打开 `/FsStatService/slicerefs`。
  - 当存储中无 SliceRef 时，显示 `SliceRef [0]` 与空表格，不报错。
  - 当存在 SliceRef 时，每行展示 `id / size / ref_count / inos` 全部四列，`inos` 多元素以逗号拼接。
- 现有 delfiles / delslices / quota / oplog 等页面行为不变。
- 编译通过：`cmake --build build --target dingo-mds -j 12`。

## 风险与备注

- SliceRef 数据量上限未做评估；如果集群 slice 数量极大，单页全量扫描可能较慢。当前阶段接受该限制，与 delslices 一致。
- 路由仅匹配 `params.size() == 1 && params[0] == "slicerefs"`；若未来需要支持 `/slicerefs/{slice_id}` 详情页，可在该分支后追加新分支，不影响现有逻辑。
