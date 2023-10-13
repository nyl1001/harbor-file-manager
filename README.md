# harbor-file-manager
本package基于 github.com/containers/image 实现了一个通过harbor进行文件上传、下载的功能，利用其服务于docker image push/pull的优秀的缓存能力，使得本package在服务端和客户端均具备文件上传、下载的缓存能力，通过校验digest，判断是否命中缓存，若命中则从缓存获取数据，从而提高文件的上传、下载速度，另外本package也适用于需要充分利用harbor系统文件管理能力的场景。
# install deps

```shell
# for mac os
brew install gpgme

# for ubuntu
sudo apt-get install libgpgme11-dev libdevmapper-dev btrfs-progs libbtrfs-dev

```
