`vendor/sysbench` 用于存放项目内置的 `sysbench` 二进制和 Lua 脚本。

- Docker 构建时会自动把 `sysbench` 复制到这里
- 非 Docker 场景可以执行 `scripts/vendor_sysbench.sh` 同步本机已安装的 `sysbench`
- 应用运行时会优先使用这里的 `bin/sysbench`
