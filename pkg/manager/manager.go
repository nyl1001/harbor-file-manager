package manager

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"

	"github.com/containers/image/v5/pkg/blobinfocache"
	"github.com/containers/image/v5/transports/alltransports"
	"github.com/containers/image/v5/types"
	"github.com/opencontainers/go-digest"
)

type HarborFileManager interface {
	CreateRepositoryIfNotExist(ctx context.Context, harborRepo string, tag string) error
	UploadFile(ctx context.Context, localFilePath, harborRepo, tag string) (*types.BlobInfo, error)
	DownloadFile(ctx context.Context, harborRepo, tag, digest string, targetFilePath string) error
	GetDownloadReader(ctx context.Context, harborRepo, tag, digest string) (io.ReadCloser, int64, error)
	DownloadFileWithBlob(ctx context.Context, harborRepo, tag, targetFilePath string, blobInfo *types.BlobInfo) error
	GetDownloadReaderWithBlob(ctx context.Context, harborRepo, tag string, blobInfo *types.BlobInfo) (io.ReadCloser, int64, error)
	DeleteImage(ctx context.Context, harborRepo, tag string) error
	DeleteRepo(harborAPI, projectName, repoName string) error
}

type harborFileManager struct {
	hifConf *HfMConfig
}

type HfMConfig struct {
	HarborUserName     string
	HarborUserPassword string
	RootCacheDir       string
}

var hfManager *harborFileManager

var hfMOnce sync.Once

func SimpleNewOnce(harborUserName, harborUserPassword, rootCacheDir string) HarborFileManager {
	hfMOnce.Do(func() {
		hfManager = &harborFileManager{
			&HfMConfig{
				HarborUserName:     harborUserName,
				HarborUserPassword: harborUserPassword,
				RootCacheDir:       rootCacheDir,
			},
		}
	})
	return hfManager
}

func NewOnce(config *HfMConfig) HarborFileManager {
	hfMOnce.Do(func() {
		hfManager = &harborFileManager{
			config,
		}
	})
	return hfManager
}

func (hfM *harborFileManager) CreateRepositoryIfNotExist(ctx context.Context, harborRepo, tag string) error {
	// 检查远程仓库是否已存在
	exists, err := checkRemoteRepoExists(ctx, hfM.hifConf.HarborUserName, hfM.hifConf.HarborUserPassword, harborRepo)
	if err != nil {
		return err
	}

	if !exists {
		ociImageName := extractImageName(harborRepo)
		ociImageDir := "/tmp/" + ociImageName
		err = createOCIImageLayout(ociImageDir)
		if err != nil {
			return err
		}
		// 上传第一个image，必要操作
		err = uploadLocalImageToHarbor(ctx, ociImageDir, hfM.hifConf.HarborUserName, hfM.hifConf.HarborUserPassword, harborRepo, tag)
		if err != nil {
			return err
		}
	}

	return nil
}

func (hfM *harborFileManager) UploadFile(ctx context.Context, localFilePath, harborRepo, tag string) (*types.BlobInfo, error) {
	// 打开本地文件
	localFile, err := os.Open(localFilePath)
	if err != nil {
		return nil, err
	}
	defer localFile.Close()

	// 准备上传的目标路径
	destRef := fmt.Sprintf("%s:%s", harborRepo, tag)

	// 使用 containers/image 库上传文件到Harbor
	destCtx, err := alltransports.ParseImageName(fmt.Sprintf("docker://%s", destRef))
	if err != nil {
		return nil, err
	}

	// 创建 SystemContext，设置 Harbor 账号密码
	sys := &types.SystemContext{
		DockerAuthConfig: &types.DockerAuthConfig{
			Username: hfM.hifConf.HarborUserName,
			Password: hfM.hifConf.HarborUserPassword,
		},
		BlobInfoCacheDir:                    hfM.hifConf.RootCacheDir,
		DockerRegistryPushPrecomputeDigests: true,
	}

	destImg, err := destCtx.NewImageDestination(ctx, sys)
	if err != nil {
		return nil, err
	}

	// 获取文件信息
	fileInfo, err := localFile.Stat()
	if err != nil {
		return nil, err
	}
	// 获取文件大小
	fileSize := fileInfo.Size()

	// 使用 PutBlob 上传文件，并命中本地缓存， none.NoCache
	blobInfo, err := destImg.PutBlob(ctx, localFile, types.BlobInfo{Size: fileSize}, blobinfocache.DefaultCache(sys), false)
	if err != nil {
		return nil, err
	}

	err = destImg.Commit(ctx, nil)
	if err != nil {
		return nil, err
	}

	return &blobInfo, nil
}

func (hfM *harborFileManager) GetDownloadReader(ctx context.Context, harborRepo, tag, digestStr string) (io.ReadCloser, int64, error) {
	err := initRootCacheDir(hfM.hifConf.RootCacheDir)
	if err != nil {
		return nil, 0, err
	}
	// 准备下载的源路径
	srcRef, err := alltransports.ParseImageName(fmt.Sprintf("docker://%s:%s", harborRepo, tag))
	if err != nil {
		return nil, 0, err
	}
	// 创建 SystemContext，设置 Harbor 账号密码
	sys := &types.SystemContext{
		DockerAuthConfig: &types.DockerAuthConfig{
			Username: hfM.hifConf.HarborUserName,
			Password: hfM.hifConf.HarborUserPassword,
		},
		BlobInfoCacheDir: hfM.hifConf.RootCacheDir,
	}

	// 使用 image.NewImage 创建一个镜像对象
	srcImg, err := srcRef.NewImageSource(ctx, sys)
	if err != nil {
		return nil, 0, err
	}

	// 获取文件内容，并检查并命中本地缓存
	reader, size, err := srcImg.GetBlob(ctx, types.BlobInfo{
		Digest:               digest.Digest(digestStr),
		Size:                 0,
		URLs:                 nil,
		Annotations:          nil,
		MediaType:            "",
		CompressionOperation: 0,
		CompressionAlgorithm: nil,
		CryptoOperation:      0,
	}, blobinfocache.DefaultCache(sys))
	if err != nil {
		return nil, 0, err
	}
	return reader, size, nil
}

func (hfM *harborFileManager) DownloadFile(ctx context.Context, harborRepo, tag, targetFilePath, digestStr string) error {
	// 从Harbor下载文件
	reader, _, err := hfM.GetDownloadReader(ctx, harborRepo, tag, digestStr)
	if err != nil {
		return err
	}

	defer func(reader io.ReadCloser) {
		err = reader.Close()
		if err != nil {

		}
	}(reader)

	// 创建本地文件
	localFile, err := os.Create(targetFilePath)
	if err != nil {
		return err
	}
	defer func(localFile *os.File) {
		err = localFile.Close()
		if err != nil {

		}
	}(localFile)

	// 将文件内容复制到本地文件
	_, err = io.Copy(localFile, reader)
	if err != nil {
		return err
	}
	return nil
}

func (hfM *harborFileManager) GetDownloadReaderWithBlob(ctx context.Context, harborRepo, tag string, blobInfo *types.BlobInfo) (io.ReadCloser, int64, error) {
	err := initRootCacheDir(hfM.hifConf.RootCacheDir)
	if err != nil {
		return nil, 0, err
	}
	// 准备下载的源路径
	srcRef, err := alltransports.ParseImageName(fmt.Sprintf("docker://%s:%s", harborRepo, tag))
	if err != nil {
		return nil, 0, err
	}
	// 创建 SystemContext，设置 Harbor 账号密码
	sys := &types.SystemContext{
		DockerAuthConfig: &types.DockerAuthConfig{
			Username: hfM.hifConf.HarborUserName,
			Password: hfM.hifConf.HarborUserPassword,
		},
		BlobInfoCacheDir: hfM.hifConf.RootCacheDir,
	}

	// 使用 image.NewImage 创建一个镜像对象
	srcImg, err := srcRef.NewImageSource(ctx, sys)
	if err != nil {
		return nil, 0, err
	}

	// 获取文件内容，并检查并命中本地缓存
	reader, size, err := srcImg.GetBlob(ctx, *blobInfo, blobinfocache.DefaultCache(sys))
	if err != nil {
		return nil, 0, err
	}
	return reader, size, nil
}

func (hfM *harborFileManager) DownloadFileWithBlob(ctx context.Context, harborRepo, tag, targetFilePath string, blobInfo *types.BlobInfo) error {
	// 从Harbor下载文件
	reader, _, err := hfM.GetDownloadReaderWithBlob(ctx, harborRepo, tag, blobInfo)
	if err != nil {
		return err
	}

	defer func(reader io.ReadCloser) {
		err = reader.Close()
		if err != nil {

		}
	}(reader)

	// 创建本地文件
	localFile, err := os.Create(targetFilePath)
	if err != nil {
		return err
	}
	defer func(localFile *os.File) {
		err = localFile.Close()
		if err != nil {

		}
	}(localFile)

	// 将文件内容复制到本地文件
	_, err = io.Copy(localFile, reader)
	if err != nil {
		return err
	}
	return nil
}

func (hfM *harborFileManager) DeleteImage(ctx context.Context, harborRepo, tag string) error {

	// 准备上传的目标路径
	destRef := fmt.Sprintf("%s:%s", harborRepo, tag)

	// 使用 containers/image 库上传文件到Harbor
	destCtx, err := alltransports.ParseImageName(fmt.Sprintf("docker://%s", destRef))
	if err != nil {
		return err
	}

	// 创建 SystemContext，设置 Harbor 账号密码
	sys := &types.SystemContext{
		DockerAuthConfig: &types.DockerAuthConfig{
			Username: hfM.hifConf.HarborUserName,
			Password: hfM.hifConf.HarborUserPassword,
		},
		BlobInfoCacheDir: hfM.hifConf.RootCacheDir,
	}

	err = destCtx.DeleteImage(ctx, sys)
	if err != nil {
		return err
	}
	return nil
}

func (hfM *harborFileManager) DeleteRepo(baseHarborServerUrl, projectName, repoName string) error {
	// 构建项目 API URL
	repoAPI := baseHarborServerUrl + "/api/v2.0/projects/" + projectName + "/repositories/" + repoName

	client := &http.Client{}
	req, err := http.NewRequest("DELETE", repoAPI, nil)
	if err != nil {
		return err
	}
	req.SetBasicAuth(hfM.hifConf.HarborUserName, hfM.hifConf.HarborUserPassword)

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func(Body io.ReadCloser) {
		err = Body.Close()
		if err != nil {

		}
	}(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to delete repo. Status code: %d, project name: %s, repo name: %s", resp.StatusCode, projectName, repoName)
	}

	return nil
}
